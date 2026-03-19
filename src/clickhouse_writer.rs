use std::io::Read;

use base64::Engine;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};
use pyo3::wrap_pyfunction;
use ureq::Agent;
use ureq::config::Config;
use crate::json_writer::{extract_record_batches, serialize_record_batches_json_bytes_impl};

/// Write PyArrow data to ClickHouse table.
///
/// Uses HTTP interface with JSONEachRow format for efficient bulk insert.
/// Serialization and HTTP request run without holding the GIL for better multithreading.
///
/// Args:
///     data: PyArrow RecordBatch or Table to write
///     base_url: ClickHouse HTTP endpoint (e.g., "http://localhost:8123")
///     database: Database name
///     table: Target table name
///     username: ClickHouse username (default: "default")
///     password: ClickHouse password (default: "")
///     datetime_format: Optional datetime format string (default: "%Y-%m-%d %H:%M:%S")
///
/// Returns:
///     Number of rows written to ClickHouse
///
/// Raises:
///     RuntimeError: If HTTP request fails or table name is empty
///
/// Examples:
///     >>> import prus
///     >>> import pyarrow as pa
///     >>> table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
///     >>> rows = prus.write_arrow_to_clickhouse(table, "http://localhost:8123", "my_db", "my_table")
///     3
#[pyfunction]
#[pyo3(signature = (data, base_url, database, table, username="default", password="", datetime_format=None))]
pub fn write_arrow_to_clickhouse(
    data: &Bound<'_, PyAny>,
    base_url: &str,
    database: &str,
    table: &str,
    username: &str,
    password: &str,
    datetime_format: Option<&str>,
) -> PyResult<usize> {
    let batches = extract_record_batches(data)?;
    let rows = batches.iter().map(|batch| batch.num_rows()).sum();
    let query = build_insert_query(table).map_err(PyRuntimeError::new_err)?;
    let base_url = base_url.to_string();
    let database = database.to_string();
    let username = username.to_string();
    let password = password.to_string();
    let datetime_format = datetime_format.map(str::to_string);
    data.py().detach(|| {
        let json_rows =
            serialize_record_batches_json_bytes_impl(&batches, true, datetime_format.as_deref())?;
        execute_insert(&base_url, &database, &username, &password, &query, &json_rows)
    }).map_err(PyRuntimeError::new_err)?;
    Ok(rows)
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(write_arrow_to_clickhouse, module)?)?;
    Ok(())
}

fn build_insert_query(table: &str) -> Result<String, String> {
    let table = table.trim();
    if table.is_empty() {
        return Err("table name cannot be empty".to_string());
    }
    Ok(format!("INSERT INTO {table} FORMAT JSONEachRow"))
}

/// Returns Result so it can be used inside py.detach() without creating PyErr.
fn execute_insert(
    base_url: &str,
    database: &str,
    username: &str,
    password: &str,
    query: &str,
    body: &[u8],
) -> Result<String, String> {
    let base_url = base_url.trim_end_matches('/');
    let url = format!("{base_url}/");
    let credentials = format!("{username}:{password}");
    let authorization = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(credentials)
    );

    let agent = Agent::new_with_config(Config::builder().http_status_as_error(false).build());
    let response = agent
        .post(&url)
        .query("query", query)
        .header("Authorization", &authorization)
        .header("X-ClickHouse-Database", database)
        .header("Content-Type", "application/x-ndjson")
        .send(body)
        .map_err(|err| format!("ClickHouse HTTP insert failed: {err}"))?;
    let status = response.status().as_u16();
    let mut reader = response.into_body().into_reader();
    let mut response_body = String::new();
    reader
        .read_to_string(&mut response_body)
        .map_err(|err| format!("failed to read http response: {err}"))?;
    if status != 200 {
        return Err(format!(
            "ClickHouse insert failed with status {status}: {response_body}"
        ));
    }
    Ok(response_body)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use base64::Engine;

    use super::{build_insert_query, execute_insert};

    #[test]
    fn builds_json_each_row_insert_query() {
        let query = build_insert_query("events").unwrap();
        assert_eq!(query, "INSERT INTO events FORMAT JSONEachRow");
    }

    #[test]
    fn rejects_empty_table_name() {
        let err = build_insert_query("   ").unwrap_err();
        assert!(err.to_string().contains("table name cannot be empty"));
    }

    #[test]
    fn sends_clickhouse_insert_request() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let reader_stream = stream.try_clone().unwrap();
            let mut reader = BufReader::new(reader_stream);

            let mut request_line = String::new();
            reader.read_line(&mut request_line).unwrap();

            let mut headers: HashMap<String, String> = HashMap::new();
            let mut content_length = 0usize;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }

                let (name, value) = line.trim_end().split_once(':').unwrap();
                let value = value.trim().to_string();
                if name.eq_ignore_ascii_case("Content-Length") {
                    content_length = value.parse::<usize>().unwrap();
                }
                headers.insert(name.to_ascii_lowercase(), value);
            }

            let mut body = vec![0; content_length];
            reader.read_exact(&mut body).unwrap();
            let body = String::from_utf8(body).unwrap();

            // ureq may encode spaces as + or %20
            let ok_query = request_line.contains("INSERT")
                && request_line.contains("INTO")
                && request_line.contains("events")
                && request_line.contains("FORMAT")
                && request_line.contains("JSONEachRow")
                && request_line.ends_with(" HTTP/1.1\r\n");
            assert!(ok_query, "request_line = {:?}", request_line);
            assert_eq!(headers.get("x-clickhouse-database").unwrap(), "test");
            assert_eq!(headers.get("content-type").unwrap(), "application/x-ndjson");
            assert_eq!(
                headers.get("authorization").unwrap(),
                &format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("root:secret")
                )
            );
            assert_eq!(
                body,
                "{\"id\":1}\n{\"id\":2}\n"
            );

            let response_body = "Ok.\n";
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            )
            .unwrap();
        });

        let response = execute_insert(
            &format!("http://{addr}"),
            "test",
            "root",
            "secret",
            "INSERT INTO events FORMAT JSONEachRow",
            b"{\"id\":1}\n{\"id\":2}\n",
        )
        .unwrap();

        assert_eq!(response, "Ok.\n");
        server.join().unwrap();
    }

    #[test]
    #[ignore = "requires a reachable ClickHouse instance"]
    fn live_clickhouse_insert_json_each_row() {
        let response = execute_insert(
            "http://127.0.0.1:8123",
            "default",
            "root",
            "123456",
            "INSERT INTO test.test_json_insert FORMAT JSONEachRow",
            b"{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n",
        )
        .unwrap();

        assert!(response.is_empty() || response == "Ok.\n");
    }
}
