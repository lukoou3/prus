use arrow_array::RecordBatch;
use arrow_json::writer::{JsonArray, LineDelimited, WriterBuilder};
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};
use pyo3::wrap_pyfunction;
use pyo3_arrow::{PyRecordBatch, PyTable};
use std::fs::File;
use std::io::{BufWriter, Write};

/// Write PyArrow data to a JSON file.
///
/// Args:
///     data: PyArrow RecordBatch or Table to write
///     path: Output file path
///     lines: If True, write as NDJSON (one JSON object per line). If False, write as JSON array (default: True)
///     datetime_format: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")
///
/// Returns:
///     Number of rows written
///
/// Raises:
///     RuntimeError: If file creation or serialization fails
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> table = pa.table({"id": [1, 2], "name": ["a", "b"]})
///     >>> # Write as NDJSON
///     >>> prus.write_arrow_json(table, "output.json", lines=True)
///     2
///     >>> # Write as JSON array
///     >>> prus.write_arrow_json(table, "output.json", lines=False)
///     2
#[pyfunction]
#[pyo3(signature = (data, path, lines=true, datetime_format=None))]
pub fn write_arrow_json(
    data: &Bound<'_, PyAny>,
    path: &str,
    lines: bool,
    datetime_format: Option<&str>,
) -> PyResult<usize> {
    let batches = extract_record_batches(data)?;
    let path = path.to_string();
    let datetime_format = datetime_format.map(str::to_string);
    data.py()
        .detach(|| write_record_batches_json_impl(&path, &batches, lines, datetime_format.as_deref()))
        .map_err(PyRuntimeError::new_err)
}

/// Write PyArrow data to a file in NDJSON format (one JSON object per line).
///
/// This is a convenience function equivalent to write_arrow_json(data, path, lines=True).
///
/// Args:
///     data: PyArrow RecordBatch or Table to write
///     path: Output file path
///     datetime_format: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")
///
/// Returns:
///     Number of rows written
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> table = pa.table({"id": [1, 2], "name": ["a", "b"]})
///     >>> prus.write_arrow_ndjson(table, "output.json")
///     2
#[pyfunction]
#[pyo3(signature = (data, path, datetime_format=None))]
pub fn write_arrow_ndjson(
    data: &Bound<'_, PyAny>,
    path: &str,
    datetime_format: Option<&str>,
) -> PyResult<usize> {
    write_arrow_json(data, path, true, datetime_format)
}

/// Write PyArrow data to a file as JSON array.
///
/// This is a convenience function equivalent to write_arrow_json(data, path, lines=False).
///
/// Args:
///     data: PyArrow RecordBatch or Table to write
///     path: Output file path
///     datetime_format: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")
///
/// Returns:
///     Number of rows written
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> table = pa.table({"id": [1, 2], "name": ["a", "b"]})
///     >>> prus.write_arrow_json_array(table, "output.json")
///     2
#[pyfunction]
#[pyo3(signature = (data, path, datetime_format=None))]
pub fn write_arrow_json_array(
    data: &Bound<'_, PyAny>,
    path: &str,
    datetime_format: Option<&str>,
) -> PyResult<usize> {
    write_arrow_json(data, path, false, datetime_format)
}

/// Serialize PyArrow data to JSON string.
///
/// Args:
///     data: PyArrow RecordBatch or Table to serialize
///     lines: If True, return NDJSON (one JSON object per line). If False, return JSON array (default: True)
///     datetime_format: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")
///
/// Returns:
///     JSON string
///
/// Raises:
///     RuntimeError: If serialization fails
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> table = pa.table({"id": [1, 2], "name": ["a", "b"]})
///     >>> # Serialize as NDJSON
///     >>> json_str = prus.dumps_arrow_json(table, lines=True)
///     >>> print(json_str)
///     {"id":1,"name":"a"}
///     {"id":2,"name":"b"}
///     >>> # Serialize as JSON array
///     >>> json_str = prus.dumps_arrow_json(table, lines=False)
///     >>> print(json_str)
///     [{"id":1,"name":"a"},{"id":2,"name":"b"}]
#[pyfunction]
#[pyo3(signature = (data, lines=true, datetime_format=None))]
pub fn dumps_arrow_json(
    data: &Bound<'_, PyAny>,
    lines: bool,
    datetime_format: Option<&str>,
) -> PyResult<String> {
    let batches = extract_record_batches(data)?;
    let datetime_format = datetime_format.map(str::to_string);
    data.py()
        .detach(|| {
            let json_bytes =
                serialize_record_batches_json_bytes_impl(&batches, lines, datetime_format.as_deref())?;
            String::from_utf8(json_bytes).map_err(|e| format!("failed to build UTF-8 JSON string: {e}"))
        })
        .map_err(PyRuntimeError::new_err)
}

/// Serialize PyArrow data to NDJSON string (one JSON object per line).
///
/// This is a convenience function equivalent to dumps_arrow_json(data, lines=True).
///
/// Args:
///     data: PyArrow RecordBatch or Table to serialize
///     datetime_format: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")
///
/// Returns:
///     NDJSON string
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> table = pa.table({"id": [1, 2], "name": ["a", "b"]})
///     >>> json_str = prus.dumps_arrow_ndjson(table)
///     >>> print(json_str)
///     {"id":1,"name":"a"}
///     {"id":2,"name":"b"}
#[pyfunction]
#[pyo3(signature = (data, datetime_format=None))]
pub fn dumps_arrow_ndjson(
    data: &Bound<'_, PyAny>,
    datetime_format: Option<&str>,
) -> PyResult<String> {
    dumps_arrow_json(data, true, datetime_format)
}

/// Serialize PyArrow data to JSON array string.
///
/// This is a convenience function equivalent to dumps_arrow_json(data, lines=False).
///
/// Args:
///     data: PyArrow RecordBatch or Table to serialize
///     datetime_format: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")
///
/// Returns:
///     JSON array string
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> table = pa.table({"id": [1, 2], "name": ["a", "b"]})
///     >>> json_str = prus.dumps_arrow_json_array(table)
///     >>> print(json_str)
///     [{"id":1,"name":"a"},{"id":2,"name":"b"}]
#[pyfunction]
#[pyo3(signature = (data, datetime_format=None))]
pub fn dumps_arrow_json_array(
    data: &Bound<'_, PyAny>,
    datetime_format: Option<&str>,
) -> PyResult<String> {
    dumps_arrow_json(data, false, datetime_format)
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(write_arrow_json, module)?)?;
    module.add_function(wrap_pyfunction!(write_arrow_ndjson, module)?)?;
    module.add_function(wrap_pyfunction!(write_arrow_json_array, module)?)?;
    module.add_function(wrap_pyfunction!(dumps_arrow_json, module)?)?;
    module.add_function(wrap_pyfunction!(dumps_arrow_ndjson, module)?)?;
    module.add_function(wrap_pyfunction!(dumps_arrow_json_array, module)?)?;
    Ok(())
}

pub(crate) fn extract_record_batches(data: &Bound<'_, PyAny>) -> PyResult<Vec<RecordBatch>> {
    if let Ok(batch) = data.extract::<PyRecordBatch>() {
        return Ok(vec![batch.into_inner()]);
    }

    if let Ok(table) = data.extract::<PyTable>() {
        let (batches, _) = table.into_inner();
        return Ok(batches);
    }

    Err(PyTypeError::new_err(
        "expected a pyarrow/Arrow RecordBatch or Table object",
    ))
}

fn write_record_batches_json(
    path: &str,
    batches: &[RecordBatch],
    lines: bool,
    datetime_format: Option<&str>,
) -> PyResult<usize> {
    write_record_batches_json_impl(path, batches, lines, datetime_format)
        .map_err(PyRuntimeError::new_err)
}

/// Rust-only; returns Result<usize, String>. Safe inside py.detach() (no PyErr).
fn write_record_batches_json_impl(
    path: &str,
    batches: &[RecordBatch],
    lines: bool,
    datetime_format: Option<&str>,
) -> Result<usize, String> {
    let file = File::create(path)
        .map_err(|err| format!("failed to create JSON file '{path}': {err}"))?;
    let buffer = BufWriter::new(file);
    write_json_to_writer_impl(buffer, batches, lines, datetime_format)?;
    Ok(batches.iter().map(RecordBatch::num_rows).sum())
}

pub(crate) fn serialize_record_batches_json(
    batches: &[RecordBatch],
    lines: bool,
    datetime_format: Option<&str>,
) -> PyResult<String> {
    let json_bytes = serialize_record_batches_json_bytes(batches, lines, datetime_format)?;
    String::from_utf8(json_bytes)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to build UTF-8 JSON string: {err}")))
}

/// Returns raw bytes; use from outside detach. For use inside detach, use `serialize_record_batches_json_bytes_impl`.
pub(crate) fn serialize_record_batches_json_bytes(
    batches: &[RecordBatch],
    lines: bool,
    datetime_format: Option<&str>,
) -> PyResult<Vec<u8>> {
    serialize_record_batches_json_bytes_impl(batches, lines, datetime_format)
        .map_err(PyRuntimeError::new_err)
}

/// Rust-only serialization (Result<Vec<u8>, String>). Safe to call inside py.detach(); do not use PyErr in closure.
pub(crate) fn serialize_record_batches_json_bytes_impl(
    batches: &[RecordBatch],
    lines: bool,
    datetime_format: Option<&str>,
) -> Result<Vec<u8>, String> {
    let buffer = Vec::new();
    write_json_to_writer_impl(buffer, batches, lines, datetime_format)
}

fn writer_builder(datetime_format: Option<&str>) -> WriterBuilder {
    let format = datetime_format.unwrap_or("%Y-%m-%d %H:%M:%S").to_string();
    let builder = WriterBuilder::new()
        .with_timestamp_format(format.clone())
        .with_timestamp_tz_format(format);
    builder
}

fn write_json_to_writer<W: Write>(
    writer: W,
    batches: &[RecordBatch],
    lines: bool,
    datetime_format: Option<&str>,
) -> PyResult<W> {
    write_json_to_writer_impl(writer, batches, lines, datetime_format)
        .map_err(PyRuntimeError::new_err)
}

/// Rust-only; returns Result<W, String>. Safe inside py.detach() (no PyErr).
fn write_json_to_writer_impl<W: Write>(
    writer: W,
    batches: &[RecordBatch],
    lines: bool,
    datetime_format: Option<&str>,
) -> Result<W, String> {
    let batch_refs = batches.iter().collect::<Vec<_>>();
    let builder = writer_builder(datetime_format);

    if lines {
        let mut writer = builder.build::<_, LineDelimited>(writer);
        writer.write_batches(&batch_refs).map_err(|err| {
            format!("failed to write line-delimited JSON: {err}")
        })?;
        writer.finish().map_err(|err| format!("failed to finish JSON writer: {err}"))?;
        let mut inner = writer.into_inner();
        inner.flush().map_err(|err| format!("failed to flush JSON output: {err}"))?;
        Ok(inner)
    } else {
        let mut writer = builder.build::<_, JsonArray>(writer);
        writer
            .write_batches(&batch_refs)
            .map_err(|err| format!("failed to write JSON array: {err}"))?;
        writer.finish().map_err(|err| format!("failed to finish JSON writer: {err}"))?;
        let mut inner = writer.into_inner();
        inner.flush().map_err(|err| format!("failed to flush JSON output: {err}"))?;
        Ok(inner)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow_array::{Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use chrono::NaiveDateTime;

    use super::{serialize_record_batches_json, write_record_batches_json};

    fn build_batch() -> RecordBatch {
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

    fn build_datetime_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let value = NaiveDateTime::parse_from_str("2026-02-09 00:00:00", "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_micros();

        RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![Some(value)]))],
        )
        .unwrap()
    }

    fn temp_file_path(suffix: &str) -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!("prus_arrow_json_{unique}_{suffix}.json"))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn writes_line_delimited_json() {
        let batch = build_batch();
        let path = temp_file_path("lines");

        let rows =
            write_record_batches_json(&path, std::slice::from_ref(&batch), true, None).unwrap();
        let content = fs::read_to_string(&path).unwrap();

        assert_eq!(rows, 2);
        assert_eq!(
            content,
            "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn writes_json_array() {
        let batch = build_batch();
        let path = temp_file_path("array");

        let rows =
            write_record_batches_json(&path, std::slice::from_ref(&batch), false, None).unwrap();
        let content = fs::read_to_string(&path).unwrap();

        assert_eq!(rows, 2);
        assert_eq!(
            content,
            "[{\"id\":1,\"name\":\"alice\"},{\"id\":2,\"name\":\"bob\"}]"
        );

        let _ = fs::remove_file(path);
    }

    #[test]
    fn serializes_line_delimited_json_to_string() {
        let batch = build_batch();

        let content =
            serialize_record_batches_json(std::slice::from_ref(&batch), true, None).unwrap();

        assert_eq!(
            content,
            "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n"
        );
    }

    #[test]
    fn serializes_json_array_to_string() {
        let batch = build_batch();

        let content =
            serialize_record_batches_json(std::slice::from_ref(&batch), false, None).unwrap();

        assert_eq!(
            content,
            "[{\"id\":1,\"name\":\"alice\"},{\"id\":2,\"name\":\"bob\"}]"
        );
    }

    #[test]
    fn serializes_datetime_with_custom_format() {
        let batch = build_datetime_batch();

        let content = serialize_record_batches_json(
            std::slice::from_ref(&batch),
            false,
            Some("%Y-%m-%d %H:%M:%S"),
        )
        .unwrap();

        assert_eq!(content, "[{\"created_at\":\"2026-02-09 00:00:00\"}]");
    }
}
