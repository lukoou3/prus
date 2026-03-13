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
use rdkafka::ClientConfig;

const DEFAULT_MAX_MESSAGES: usize = 100_000;
const DEFAULT_MAX_DURATION_SECS: u64 = 30;

#[pyfunction]
#[pyo3(signature = (
    brokers,
    topics,
    mode="raw",
    max_messages=None,
    max_duration_seconds=None,
    schema=None,
    properties=None
))]
pub fn read_kafka_to_arrow<'py>(
    py: Python<'py>,
    brokers: &str,
    topics: &Bound<'_, PyAny>,
    mode: &str,
    max_messages: Option<usize>,
    max_duration_seconds: Option<f64>,
    schema: Option<&Bound<'_, PyAny>>,
    properties: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let topics = py_list_to_strings(topics)?;
    let schema_ref = schema.map(resolve_schema).transpose()?;
    let properties_vec = properties.map(py_dict_to_properties).transpose()?;
    let brokers = brokers.to_string();
    let mode = mode.to_string();
    let (schema, batches) = py
        .detach(|| {
            read_kafka_to_arrow_impl(
                &brokers,
                topics.as_slice(),
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
    for (k, v) in dict.iter()? {
        let key = k.extract::<String>()?;
        let val = v.extract::<String>()?;
        out.push((key, val));
    }
    Ok(out)
}

fn py_schema_to_pairs(schema: &Bound<'_, PyAny>) -> PyResult<Vec<(String, String)>> {
    let list = schema.downcast::<pyo3::types::PyList>()?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter()? {
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
        return Ok(vec![s.to_string()?]);
    }
    let list = topics.downcast::<pyo3::types::PyList>()?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter()? {
        let s = item.downcast::<pyo3::types::PyString>()?.to_string()?;
        out.push(s);
    }
    Ok(out)
}

fn read_kafka_to_arrow_impl(
    brokers: &str,
    topics: &[String],
    mode: &str,
    max_messages: Option<usize>,
    max_duration_seconds: Option<f64>,
    schema: Option<&Arc<Schema>>,
    properties: Option<&[(String, String)]>,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let max_msgs = max_messages.unwrap_or(DEFAULT_MAX_MESSAGES);
    let max_dur = max_duration_seconds
        .map(Duration::from_secs_f64)
        .unwrap_or_else(|| Duration::from_secs(DEFAULT_MAX_DURATION_SECS));

    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    config.set("group.id", format!("prus-kafka-{}", uuid_simple()));
    config.set("auto.offset.reset", "earliest");
    config.set("enable.auto.commit", "false");
    if let Some(props) = properties {
        for (k, v) in props {
            config.set(k, v);
        }
    }
    let consumer: BaseConsumer = config
        .create()
        .map_err(|e| format!("Kafka consumer creation failed: {e}"))?;

    let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topic_refs)
        .map_err(|e| format!("Kafka subscribe failed: {e}"))?;

    let mut messages: Vec<Vec<u8>> = Vec::new();
    let deadline = Instant::now() + max_dur;
    let poll_timeout = Duration::from_millis(500);

    while messages.len() < max_msgs && Instant::now() < deadline {
        match consumer.poll(poll_timeout) {
            None => continue,
            Some(Ok(m)) => {
                if let Some(payload) = m.payload() {
                    messages.push(payload.to_vec());
                }
            }
            Some(Err(e)) => return Err(format!("Kafka poll error: {e}")),
        }
    }

    match mode {
        "raw" => build_raw_batches(messages),
        "json" => {
            let s = schema
                .ok_or("json mode requires schema (PyArrow schema or list of (name, type_str))")?
                .clone();
            build_json_batches_with_schema(messages, s)
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

fn build_raw_batches(messages: Vec<Vec<u8>>) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let values: Vec<Option<String>> = messages
        .into_iter()
        .map(|b| String::from_utf8(b).ok())
        .collect();
    let arr = StringArray::from(values);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr)])
        .map_err(|e| format!("RecordBatch build failed: {e}"))?;
    Ok((schema, vec![batch]))
}

fn build_json_batches_with_schema(
    messages: Vec<Vec<u8>>,
    schema: Arc<Schema>,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    if messages.is_empty() {
        return Ok((schema, vec![]));
    }
    let mut ndjson = messages.join(&b'\n');
    ndjson.push(b'\n');
    let bytes = ndjson;

    let cursor = Cursor::new(bytes);
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
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
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
