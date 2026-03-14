use std::io::{BufReader, Read};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use base64::Engine;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;
use pyo3_arrow::PyTable;

/// Query ClickHouse and return a PyArrow Table.
///
/// Uses HTTP interface with ArrowStream format for efficient data transfer.
/// HTTP and stream parsing run without holding the GIL for better multithreading.
///
/// Args:
///     base_url: ClickHouse HTTP endpoint (e.g., "http://localhost:8123")
///     database: Database name to query
///     query: SQL query (FORMAT clause is automatically appended as "FORMAT ArrowStream")
///     username: ClickHouse username (default: "default")
///     password: ClickHouse password (default: "")
///
/// Returns:
///     PyArrow Table containing the query results
///
/// Raises:
///     RuntimeError: If query contains FORMAT clause or HTTP request fails
///
/// Examples:
///     >>> import prus
///     >>> # Simple query
///     >>> table = prus.query_clickhouse_arrow("http://localhost:8123", "my_db", "SELECT * FROM events LIMIT 1000")
///     >>> # With authentication
///     >>> table = prus.query_clickhouse_arrow("http://localhost:8123", "my_db", "SELECT count()", "admin", "secret")
#[pyfunction]
#[pyo3(signature = (base_url, database, query, username="default", password=""))]
pub fn query_clickhouse_arrow<'py>(
    py: Python<'py>,
    base_url: &str,
    database: &str,
    query: &str,
    username: &str,
    password: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let query = build_arrow_stream_query(query).map_err(PyRuntimeError::new_err)?;
    let base_url = base_url.to_string();
    let database = database.to_string();
    let username = username.to_string();
    let password = password.to_string();
    let (schema, batches) = py
        .detach(|| {
            query_clickhouse_arrow_impl(&base_url, &database, &query, &username, &password)
        })
        .map_err(PyRuntimeError::new_err)?;

    PyTable::try_new(batches, schema)?
        .into_pyarrow(py)
        .map_err(|err| {
            PyRuntimeError::new_err(format!("failed to convert to pyarrow.Table: {err}"))
        })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(query_clickhouse_arrow, module)?)?;
    Ok(())
}

fn build_arrow_stream_query(query: &str) -> Result<String, String> {
    let query = query.trim().trim_end_matches(';').trim();
    let normalized = query.to_ascii_lowercase();
    if normalized.contains(" format ") || normalized.ends_with(" format") {
        return Err("query should not include FORMAT; ArrowStream is appended automatically".to_string());
    }
    Ok(format!("{query} FORMAT ArrowStream"))
}

/// HTTP + stream parse. Returns Result so it can be used inside py.detach() without creating PyErr.
/// Reads directly from the response stream (no full body Vec<u8>).
fn query_clickhouse_arrow_impl(
    base_url: &str,
    database: &str,
    query: &str,
    username: &str,
    password: &str,
) -> Result<(arrow_schema::SchemaRef, Vec<RecordBatch>), String> {
    let base_url = base_url.trim_end_matches('/');
    let url = format!("{base_url}/");
    let credentials = format!("{username}:{password}");
    let authorization = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(credentials)
    );

    let response = ureq::post(&url)
        .header("Authorization", &authorization)
        .header("X-ClickHouse-Database", database)
        .header("Accept", "application/octet-stream")
        .send(query)
        .map_err(|err| format!("ClickHouse HTTP request failed: {err}"))?;

    let reader = BufReader::new(response.into_body().into_reader());
    parse_arrow_stream_impl(reader)
}

fn query_clickhouse_arrow_stream(
    base_url: &str,
    database: &str,
    query: &str,
    username: &str,
    password: &str,
) -> PyResult<(arrow_schema::SchemaRef, Vec<RecordBatch>)> {
    query_clickhouse_arrow_impl(base_url, database, query, username, password)
        .map_err(PyRuntimeError::new_err)
}

/// Returns Result for use inside py.detach(). Use parse_arrow_stream for PyResult (e.g. tests).
fn parse_arrow_stream_impl<R: Read>(
    reader: R,
) -> Result<(arrow_schema::SchemaRef, Vec<RecordBatch>), String> {
    let mut stream_reader = StreamReader::try_new(reader, None)
        .map_err(|err| format!("failed to parse ClickHouse ArrowStream schema: {err}"))?;
    let schema = stream_reader.schema();
    let batches = stream_reader
        .by_ref()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("failed to read ClickHouse ArrowStream batches: {err}"))?;
    Ok((schema, batches))
}

fn parse_arrow_stream<R: Read>(
    reader: R,
) -> PyResult<(arrow_schema::SchemaRef, Vec<RecordBatch>)> {
    parse_arrow_stream_impl(reader).map_err(PyRuntimeError::new_err)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, RecordBatch, StringArray, UInt64Array};
    use arrow_ipc::writer::StreamWriter;

    use super::{build_arrow_stream_query, parse_arrow_stream, query_clickhouse_arrow_stream};

    fn build_arrow_stream_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();

        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        bytes
    }

    #[test]
    fn appends_arrow_stream_format() {
        let query = build_arrow_stream_query("select * from events;").unwrap();
        assert_eq!(query, "select * from events FORMAT ArrowStream");
    }

    #[test]
    fn rejects_existing_format_clause() {
        let err = build_arrow_stream_query("select * from events format json").unwrap_err();
        assert!(err.to_string().contains("should not include FORMAT"));
    }

    #[test]
    fn parses_clickhouse_arrow_stream() {
        let bytes = build_arrow_stream_bytes();
        let (schema, batches) = parse_arrow_stream(Cursor::new(bytes)).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[test]
    #[ignore = "requires a reachable ClickHouse instance"]
    fn live_clickhouse_query_to_arrow_stream() {
        let url = "http://127.0.0.1:8123";
        let database = "default";
        let query = "select number from system.numbers limit 3 FORMAT ArrowStream";
        println!("query: {query}");

        let (schema, batches) =
            query_clickhouse_arrow_stream(url, database, query, "root", "123456").unwrap();
        println!("{schema:?}");
        println!("{batches:?}");

        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "number");
        assert_eq!(batches.len(), 1);

        let numbers = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(numbers.len(), 3);
        assert_eq!(numbers.value(0), 0);
        assert_eq!(numbers.value(1), 1);
        assert_eq!(numbers.value(2), 2);
    }

    #[test]
    #[ignore = "requires a reachable ClickHouse instance"]
    fn live_clickhouse_query_to_arrow_stream2() {
        let url = "http://127.0.0.1:8123";
        let database = "default";
        let query = "select * from test.test_ck_sink_local limit 100 FORMAT ArrowStream";
        println!("query: {query}");

        let (schema, batches) =
            query_clickhouse_arrow_stream(url, database, query, "root", "123456").unwrap();
        println!("{schema:?}");
        println!("{batches:?}");
        
    }

}
