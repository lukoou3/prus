//! Write Arrow data to Kafka.
//! - json: serialize each row as JSON object (one message per row)
//! - raw: Arrow must have exactly one column of type Utf8/LargeUtf8 or Binary/LargeBinary

use std::sync::Arc;
use std::time::Duration;
use arrow::datatypes::DataType;
use arrow_json::writer::{make_encoder, EncoderOptions};
use arrow_array::{Array, LargeStringArray, RecordBatch, StringArray, StructArray};
use arrow::datatypes::Field;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::ClientConfig;

/// Write Arrow data to Kafka topic.
///
/// Supports two modes:
/// - "json": Serialize each row as a JSON object (one message per row)
/// - "raw": Send raw bytes directly (Arrow must have exactly one column of type Utf8/LargeUtf8/Binary/LargeBinary)
///
/// Args:
///     data: PyArrow RecordBatch or Table to write
///     brokers: Kafka bootstrap servers (e.g., "localhost:9092")
///     topic: Target Kafka topic name
///     mode: Serialization mode, either "json" or "raw" (default: "json")
///     properties: Optional dict of Kafka producer config (e.g., {"acks": "all", "compression.type": "snappy"})
///     datetime_format: Optional datetime format string for JSON mode (default: "%Y-%m-%d %H:%M:%S")
///
/// Returns:
///     Number of rows written to Kafka
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> # JSON mode with multiple columns
///     >>> table = pa.table({"id": [1, 2], "name": ["a", "b"]})
///     >>> prus.write_arrow_to_kafka(table, "localhost:9092", "my-topic", mode="json")
///     2
///     >>> # Raw mode with single string column
///     >>> raw_table = pa.table({"value": ["msg1", "msg2"]})
///     >>> prus.write_arrow_to_kafka(raw_table, "localhost:9092", "my-topic", mode="raw")
///     2
#[pyfunction]
#[pyo3(signature = (data, brokers, topic, mode="json", properties=None, datetime_format=None))]
pub fn write_arrow_to_kafka(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    brokers: &str,
    topic: &str,
    mode: &str,
    properties: Option<&Bound<'_, PyAny>>,
    datetime_format: Option<&str>,
) -> PyResult<usize> {
    let batches = extract_record_batches(data)?;
    let brokers = brokers.to_string();
    let topic = topic.to_string();
    let mode = mode.to_string();
    let properties_vec = properties.map(py_dict_to_properties).transpose()?;
    let datetime_format = datetime_format.map(str::to_string);

    py.detach(|| {
        write_arrow_to_kafka_impl(
            &batches,
            &brokers,
            &topic,
            &mode,
            properties_vec.as_deref(),
            datetime_format.as_deref(),
        )
    })
    .map_err(PyRuntimeError::new_err)
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(write_arrow_to_kafka, module)?)?;
    Ok(())
}

fn py_dict_to_properties(d: &Bound<'_, PyAny>) -> PyResult<Vec<(String, String)>> {
    let dict = d.cast::<pyo3::types::PyDict>()?;
    let mut out = Vec::with_capacity(dict.len());
    for (k, v) in dict.iter() {
        let key = k.extract::<String>()?;
        let val = v.extract::<String>()?;
        out.push((key, val));
    }
    Ok(out)
}

fn extract_record_batches(data: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    use pyo3_arrow::{PyRecordBatch, PyTable};

    if let Ok(batch) = data.extract::<PyRecordBatch>() {
        return Ok(vec![batch.into_inner()]);
    }

    if let Ok(table) = data.extract::<PyTable>() {
        let (batches, _) = table.into_inner();
        return Ok(batches);
    }

    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected a pyarrow/Arrow RecordBatch or Table object",
    ))
}

fn write_arrow_to_kafka_impl(
    batches: &[RecordBatch],
    brokers: &str,
    topic: &str,
    mode: &str,
    properties: Option<&[(String, String)]>,
    datetime_format: Option<&str>,
) -> Result<usize, String> {
    let producer = create_kafka_producer(brokers, properties)?;

    match mode {
        "json" => write_json_mode(&producer, topic, batches, datetime_format),
        "raw" => write_raw_mode(&producer, topic, batches),
        _ => Err(format!("mode must be 'json' or 'raw', got '{mode}'")),
    }
}

fn create_kafka_producer(
    brokers: &str,
    properties: Option<&[(String, String)]>,
) -> Result<BaseProducer, String> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);

    if let Some(props) = properties {
        for (k, v) in props {
            config.set(k, v);
        }
    }

    config.create().map_err(|e| format!("Kafka producer creation failed: {e}"))
}

fn write_json_mode(
    producer: &BaseProducer,
    topic: &str,
    batches: &[RecordBatch],
    datetime_format: Option<&str>,
) -> Result<usize, String> {
    if batches.is_empty() {
        return Ok(0);
    }

    let format = datetime_format.unwrap_or("%Y-%m-%d %H:%M:%S").to_string();
    let options = EncoderOptions::default()
        .with_timestamp_format(format.clone())
        .with_timestamp_tz_format(format);

    let mut buffer = Vec::with_capacity(1024);    
    let mut total_rows = 0usize;

    for batch in batches {
        let rows = batch.num_rows();
        if rows == 0 {
            continue;
        }

        let array = StructArray::from(batch.clone());
        let field = Arc::new(Field::new_struct(
            "",
            batch.schema().fields().clone(),
            false,
        ));

        let mut encoder = make_encoder(&field, &array, &options)
            .map_err(|e| format!("failed to create JSON encoder: {e}"))?;

        assert!(!encoder.has_nulls(), "root cannot be nullable");

        for idx in 0..rows {
            buffer.clear();
            encoder.encode(idx, &mut buffer);

            if !buffer.is_empty() {
                let record = BaseRecord::<(), [u8], ()>::to(topic).payload(&buffer);
                producer
                    .send(record)
                    .map_err(|(e, _)| format!("Kafka send failed: {e}"))?;
            }
        }

        total_rows += rows;
    }

    producer.flush(None).map_err(|e| format!("Kafka flush failed: {e}"))?;
    Ok(total_rows)
}

fn write_raw_mode(
    producer: &BaseProducer,
    topic: &str,
    batches: &[RecordBatch],
) -> Result<usize, String> {
    if batches.is_empty() {
        return Ok(0);
    }

    let schema = batches[0].schema();
    if schema.fields().len() != 1 {
        return Err(format!(
            "raw mode requires exactly one column, got {} columns",
            schema.fields().len()
        ));
    }

    let field = schema.field(0);
    let data_type = field.data_type();

    let mut total_rows = 0usize;

    for batch in batches {
        let column = batch.column(0);
        let rows = batch.num_rows();

        match data_type {
            DataType::Utf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("failed to downcast to StringArray")?;
                for i in 0..rows {
                    let payload = if arr.is_null(i) {
                        b""
                    } else {
                        arr.value(i).as_bytes()
                    };
                    let record = BaseRecord::<(), [u8], ()>::to(topic).payload(payload);
                    producer
                        .send(record)
                        .map_err(|(e, _)| format!("Kafka send failed: {e}"))?;
                }
            }
            DataType::LargeUtf8 => {
                let arr = column
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or("failed to downcast to LargeStringArray")?;
                for i in 0..rows {
                    let payload = if arr.is_null(i) {
                        b""
                    } else {
                        arr.value(i).as_bytes()
                    };
                    let record = BaseRecord::<(), [u8], ()>::to(topic).payload(payload);
                    producer
                        .send(record)
                        .map_err(|(e, _)| format!("Kafka send failed: {e}"))?;
                }
            }
            DataType::Binary => {
                let arr = column
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .ok_or("failed to downcast to BinaryArray")?;
                for i in 0..rows {
                    let payload = if arr.is_null(i) {
                        &[][..]
                    } else {
                        arr.value(i)
                    };
                    let record = BaseRecord::<(), [u8], ()>::to(topic).payload(payload);
                    producer
                        .send(record)
                        .map_err(|(e, _)| format!("Kafka send failed: {e}"))?;
                }
            }
            DataType::LargeBinary => {
                let arr = column
                    .as_any()
                    .downcast_ref::<arrow_array::LargeBinaryArray>()
                    .ok_or("failed to downcast to LargeBinaryArray")?;
                for i in 0..rows {
                    let payload = if arr.is_null(i) {
                        &[][..]
                    } else {
                        arr.value(i)
                    };
                    let record = BaseRecord::<(), [u8], ()>::to(topic).payload(payload);
                    producer
                        .send(record)
                        .map_err(|(e, _)| format!("Kafka send failed: {e}"))?;
                }
            }
            _ => {
                return Err(format!(
                    "raw mode requires column type Utf8, LargeUtf8, Binary, or LargeBinary, got {:?}",
                    data_type
                ));
            }
        }

        total_rows += rows;
    }

    producer.flush(Duration::from_secs(10)).map_err(|e| format!("Kafka flush failed: {e}"))?;
    Ok(total_rows)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{BinaryArray, Int32Array, RecordBatch, StringArray};

    use super::*;

    fn build_string_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            true,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("message1"),
                Some("message2"),
                None,
                Some("message3"),
            ]))],
        )
        .unwrap()
    }

    fn build_binary_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Binary,
            true,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from_opt_vec(vec![
                Some(b"bytes1".as_slice()),
                Some(b"bytes2".as_slice()),
                None,
            ]))],
        )
        .unwrap()
    }

    fn build_multi_column_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn rejects_multi_column_in_raw_mode() {
        let batch = build_multi_column_batch();
        let result = write_raw_mode_impl_test(&[batch]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly one column"));
    }

    #[test]
    fn rejects_wrong_type_in_raw_mode() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
        let result = write_raw_mode_impl_test(&[batch]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Utf8, LargeUtf8, Binary, or LargeBinary"));
    }

    fn write_raw_mode_impl_test(batches: &[RecordBatch]) -> Result<usize, String> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", "localhost:9092");
        let producer: BaseProducer = config.create().map_err(|e| e.to_string())?;
        write_raw_mode(&producer, "test-topic", batches)
    }

    #[test]
    #[ignore = "requires a reachable Kafka instance"]
    fn live_kafka_write_raw_mode() {
        let batch = build_string_batch();
        let properties = vec![
            ("compression.type".to_string(), "snappy".to_string()),
            //("compression.type".to_string(), "lz4".to_string()),
        ];
        let producer = create_kafka_producer("localhost:9092", Some(&properties)).unwrap();
        let rows = write_raw_mode(&producer, "test-raw-topic", &[batch]).unwrap();
        assert_eq!(rows, 4);
    }

    #[test]
    #[ignore = "requires a reachable Kafka instance"]
    fn live_kafka_write_json_mode() {
        let batch = build_multi_column_batch();
        let producer = create_kafka_producer("localhost:9092", None).unwrap();
        let rows = write_json_mode(&producer, "test-json-topic", &[batch], None).unwrap();
        assert_eq!(rows, 2);
    }
}
