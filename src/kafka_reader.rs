//! Read Kafka messages into a PyArrow Table.
//! - raw: one column "value" (utf8), one row per message.
//! - json: NDJSON per message, parsed with arrow_json; schema is required.
//! Schema can be a PyArrow schema (pa.schema([...])) or a list of (name, type_str) tuples.
//! properties: optional dict of Kafka consumer config (e.g. bootstrap.servers, group.id); keys and values must be str.
//! Stop when max_messages and/or max_duration_seconds is reached.

use std::io::{BufReader, Cursor};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{RecordBatch, StringArray};
use arrow_json::reader::ReaderBuilder;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;
use pyo3_arrow::{PySchema, PyTable};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Message};

const DEFAULT_MAX_MESSAGES: usize = 500_000;
const DEFAULT_MAX_DURATION_SECS: u64 = 180;
static POLL_TIMEOUT: Duration = Duration::from_millis(200);

/// Read Kafka messages into a PyArrow Table.
///
/// Supports two modes:
/// - "raw": One column "value" (utf8), one row per message. No schema required.
/// - "json": NDJSON per message, parsed with arrow_json. Schema is required.
///
/// Args:
///     brokers: Kafka bootstrap servers (e.g., "localhost:9092")
///     topic: Kafka topic name to consume from
///     mode: Read mode, either "raw" or "json" (default: "raw")
///     max_messages: Maximum number of messages to read (default: 500,000)
///     max_duration_seconds: Maximum duration in seconds to poll (default: 180)
///     schema: Required for "json" mode. Can be PyArrow schema (pa.schema([...])) or list of (name, type_str) tuples.
///              Supported type_str: bool/int8/int16/int32/int64/uint8/uint16/uint32/uint64/float32/float64/utf8/date32/date64/timestamp_ms/timestamp_us
///     properties: Optional dict of Kafka consumer config (e.g., {"group.id": "my-group", "auto.offset.reset": "earliest"})
///
/// Returns:
///     PyArrow Table containing the consumed messages
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> # Raw mode - read as strings
///     >>> table = prus.read_kafka_to_arrow("localhost:9092", "logs", mode="raw", max_messages=100)
///     >>> # JSON mode - parse structured data
///     >>> schema = pa.schema([("id", "int64"), ("name", "utf8"), ("timestamp", "timestamp_ms")])
///     >>> table = prus.read_kafka_to_arrow("localhost:9092", "events", mode="json", schema=schema)
#[pyfunction]
#[pyo3(signature = (
    brokers,
    topic,
    mode="raw",
    max_messages=500_000,
    max_duration_seconds=180,
    schema=None,
    properties=None
))]
pub fn read_kafka_to_arrow<'py>(
    py: Python<'py>,
    brokers: &str,
    topic: &str,
    mode: &str,
    max_messages: Option<usize>,
    max_duration_seconds: Option<u64>,
    schema: Option<&Bound<'_, PyAny>>,
    properties: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let topic = topic.to_string();
    let schema_ref = schema.map(resolve_schema).transpose()?;
    let properties_vec = properties.map(py_dict_to_properties).transpose()?;
    let brokers = brokers.to_string();
    let mode = mode.to_string();
    let (schema, batches) = py
        .detach(|| {
            read_kafka_to_arrow_impl(
                &brokers,
                &topic,
                &mode,
                max_messages,
                max_duration_seconds,
                schema_ref.as_ref(),
                properties_vec.as_deref(),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    PyTable::try_new(batches, schema)?
        .into_pyarrow(py)
        .map_err(|e| PyRuntimeError::new_err(format!("failed to convert to pyarrow.Table: {e}")))
}

/// Resolve schema from either PyArrow schema (pa.schema([...])) or list of (name, type_str).
fn resolve_schema(schema: &Bound<'_, PyAny>) -> PyResult<Arc<Schema>> {
    if let Ok(py_schema) = schema.extract::<PySchema>() {
        return Ok(py_schema.into_inner());
    }
    let pairs = py_schema_to_pairs(schema)?;
    build_schema_from_hint(&pairs).map_err(PyRuntimeError::new_err)
}

fn py_dict_to_properties(d: &Bound<'_, PyAny>) -> PyResult<Vec<(String, String)>> {
    let dict = d.downcast::<pyo3::types::PyDict>()?;
    let mut out = Vec::with_capacity(dict.len());
    for (k, v) in dict.iter() {
        let key = k.extract::<String>()?;
        let val = v.extract::<String>()?;
        out.push((key, val));
    }
    Ok(out)
}

fn py_schema_to_pairs(schema: &Bound<'_, PyAny>) -> PyResult<Vec<(String, String)>> {
    let list: &Bound<'_, pyo3::types::PyList>  = schema.cast()?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let tup = item.downcast::<pyo3::types::PyTuple>()?;
        if tup.len() != 2 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "schema item must be (name, type_str) tuple",
            ));
        }
        let name = tup.get_item(0)?.extract::<String>()?;
        let type_str = tup.get_item(1)?.extract::<String>()?;
        out.push((name, type_str));
    }
    Ok(out)
}

fn py_list_to_strings(topics: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = topics.downcast::<pyo3::types::PyString>() {
        return Ok(vec![s.to_string()]);
    }
    let list = topics.downcast::<pyo3::types::PyList>()?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let s = item.downcast::<pyo3::types::PyString>()?.to_string();
        out.push(s);
    }
    Ok(out)
}

fn read_kafka_to_arrow_impl(
    brokers: &str,
    topic: &str,
    mode: &str,
    max_messages: Option<usize>,
    max_duration_seconds: Option<u64>,
    schema: Option<&Arc<Schema>>,
    properties: Option<&[(String, String)]>,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let max_msgs = max_messages.unwrap_or(DEFAULT_MAX_MESSAGES);
    let max_dur = max_duration_seconds
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_MAX_DURATION_SECS));

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    config.set("group.id", format!("prus-kafka-{}", uuid_simple()));
    if let Some(props) = properties {
        for (k, v) in props {
            config.set(k, v);
        }
    }
    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("Kafka consumer creation failed: {e}"))?;

    let topic_refs: Vec<&str> = vec![topic];
    consumer
        .subscribe(&topic_refs)
        .map_err(|e| format!("Kafka subscribe failed: {e}"))?;

    let deadline = Instant::now() + max_dur;
    let mut empty_cnt = 0;

    match mode {
        "raw" => {
            let mut messages: Vec<String> = Vec::new();
            while messages.len() < max_msgs && Instant::now() < deadline {
                match consumer.poll(POLL_TIMEOUT) {
                    None => {
                        empty_cnt += 1;
                        if empty_cnt > 50 {
                            break;
                        }
                    },
                    Some(Ok(m)) => {
                        empty_cnt = 0;
                        if let Some(payload) = m.payload() {
                            messages.push(String::from_utf8_lossy(payload).to_string());
                        }
                    }
                    Some(Err(e)) => return Err(format!("Kafka poll error: {e}")),
                }
            }
            build_raw_batches(messages)
        },
        "json" => {
            let s = schema
                .ok_or("json mode requires schema (PyArrow schema or list of (name, type_str))")?
                .clone();
            let mut bytes: Vec<u8> = Vec::new();
            let mut msgs = 0;
            while msgs < max_msgs && Instant::now() < deadline {
                msgs += 1;
                match consumer.poll(POLL_TIMEOUT) {
                    None => {
                        empty_cnt += 1;
                        if empty_cnt > 50 {
                            break;
                        }
                    },
                    Some(Ok(m)) => {
                        empty_cnt = 0;
                        if let Some(payload) = m.payload() {
                            bytes.extend_from_slice(payload);
                            bytes.push(b'\n');
                        }
                    }
                    Some(Err(e)) => return Err(format!("Kafka poll error: {e}")),
                }
            }
            build_json_batches_with_schema(bytes, s)
        }
        _ => Err(format!("mode must be 'raw' or 'json', got '{mode}'")),
    }
}

fn uuid_simple() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn build_raw_batches(messages: Vec<String>) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let arr = StringArray::from(messages);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)])
        .map_err(|e| format!("RecordBatch build failed: {e}"))?;
    Ok((schema, vec![batch]))
}

fn build_json_batches_with_schema(
    bytes: Vec<u8>,
    schema: Arc<Schema>,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    if bytes.is_empty() {
        return Ok((schema, vec![]));
    }

    let cursor = Cursor::new(&bytes);
    let mut reader = ReaderBuilder::new(schema.clone())
        .build(BufReader::new(cursor))
        .map_err(|e| format!("JSON reader build failed: {e}"))?;

    let mut batches = Vec::new();
    while let Some(batch_result) = reader.next() {
        let batch = batch_result.map_err(|e| format!("JSON read failed: {e}"))?;
        batches.push(batch);
    }
    Ok((schema, batches))
}

fn build_schema_from_hint(pairs: &[(String, String)]) -> Result<Arc<Schema>, String> {
    let fields: Vec<Field> = pairs
        .iter()
        .map(|(name, type_str)| {
            let dt = parse_type_str(type_str)?;
            Ok(Field::new(name.as_str(), dt, true))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn parse_type_str(s: &str) -> Result<DataType, String> {
    let t = s.trim().to_lowercase();
    Ok(match t.as_str() {
        "bool" | "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" | "int" => DataType::Int32,
        "int64" | "long" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" | "uint" => DataType::UInt32,
        "uint64" | "ulong" => DataType::UInt64,
        "float32" | "float" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "utf8" | "string" => DataType::Utf8,
        "large_utf8" => DataType::LargeUtf8,
        "binary" => DataType::Binary,
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        "timestamp_us" | "timestamp_microsecond" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        }
        "timestamp_ms" | "timestamp_millisecond" => {
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        }
        _ => return Err(format!("unsupported type for schema: '{s}'")),
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(read_kafka_to_arrow, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_kafka_to_arrow_mode_raw() {
        let (schema, batches) = read_kafka_to_arrow_impl(
            "localhost:9092",
            "logs",
            "raw",
            None,
            None,
            None,
            None,
        )
        .unwrap();
        println!("schema: {:?}", schema);
        for batch in batches {
            println!("batch: {:?}", batch);
        }
    }

    #[test]
    fn test_read_kafka_to_arrow_mode_json() {
        let in_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let (schema, batches) = read_kafka_to_arrow_impl(
            "localhost:9092",
            "logs",
            "json",
            None,
            None,
            Some(&in_schema),
            None,
        )
        .unwrap();
        println!("schema: {:?}", schema);
        for batch in batches {
            println!("batch: {:?}", batch);
        }
    }
}