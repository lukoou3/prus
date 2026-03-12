use std::io::Read;

use base64::Engine;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyModule};
use pyo3::wrap_pyfunction;
use serde_json::Value;
use ureq::Agent;

use crate::json_writer::{extract_record_batches, serialize_record_batches_json_bytes};

#[pyfunction]
#[pyo3(signature = (data, base_url, database, table, username="root", password="", datetime_format=None, label=None))]
pub fn write_arrow_to_starrocks(
    data: &Bound<'_, PyAny>,
    base_url: &str,
    database: &str,
    table: &str,
    username: &str,
    password: &str,
    datetime_format: Option<&str>,
    label: Option<&str>,
) -> PyResult<String> {
    let batches = extract_record_batches(data)?;
    let body = serialize_record_batches_json_bytes(&batches, false, datetime_format)?;
    stream_load_json(
        base_url, database, table, username, password, body, label,
    )
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(write_arrow_to_starrocks, module)?)?;
    Ok(())
}

fn stream_load_json(
    base_url: &str,
    database: &str,
    table: &str,
    username: &str,
    password: &str,
    body: Vec<u8>,
    label: Option<&str>,
) -> PyResult<String> {
    let base_url = base_url.trim_end_matches('/');
    let url = format!("{base_url}/api/{database}/{table}/_stream_load");
    let credentials = format!("{username}:{password}");
    let authorization = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(credentials)
    );
    let agent: Agent = Agent::config_builder().max_redirects(0).build().into();

    let response = send_stream_load_request(
        &agent,
        &url,
        &authorization,
        body.as_slice(),
        label,
    )?;
    let response_body = match response {
        StreamLoadResponse::Completed(body) => body,
        StreamLoadResponse::Redirect(redirect_url) => match send_stream_load_request(
            &agent,
            &redirect_url,
            &authorization,
            body.as_slice(),
            label,
        )? {
            StreamLoadResponse::Completed(body) => body,
            StreamLoadResponse::Redirect(_) => {
                return Err(PyRuntimeError::new_err(
                    "StarRocks stream load received repeated redirect response",
                ));
            }
        },
    };

    validate_stream_load_response(&response_body)?;
    Ok(response_body)
}

enum StreamLoadResponse {
    Completed(String),
    Redirect(String),
}

fn send_stream_load_request(
    agent: &Agent,
    url: &str,
    authorization: &str,
    body: &[u8],
    label: Option<&str>,
) -> PyResult<StreamLoadResponse> {
    let mut request = agent
        .put(url)
        .header("Authorization", authorization)
        .header("Expect", "100-continue")
        .header("two_phase_commit", "false")
        .header("Content-Type", "application/json")
        .header("format", "json")
        .header("strip_outer_array", "true")
        .header("ignore_json_size", "true");

    if let Some(label) = label {
        request = request.header("label", label);
    }

    let response = request.send(body).map_err(|err| {
        PyRuntimeError::new_err(format!("StarRocks stream load request failed: {err}"))
    })?;

    if response.status().as_u16() == 307 {
        let location = response.headers().get("location").ok_or_else(|| {
            PyRuntimeError::new_err(
                "StarRocks stream load redirect response missing Location header",
            )
        })?;
        let redirect_url = location.to_str().map_err(|err| {
            PyRuntimeError::new_err(format!(
                "StarRocks stream load redirect Location header is invalid: {err}"
            ))
        })?;
        return Ok(StreamLoadResponse::Redirect(redirect_url.to_string()));
    }

    let mut reader = response.into_body().into_reader();
    let mut response_body = String::new();
    reader.read_to_string(&mut response_body).map_err(|err| {
        PyRuntimeError::new_err(format!("failed to read StarRocks stream load response: {err}"))
    })?;
    Ok(StreamLoadResponse::Completed(response_body))
}

fn validate_stream_load_response(response_body: &str) -> PyResult<()> {
    let response_body = response_body.trim();
    if response_body.is_empty() {
        return Err(PyRuntimeError::new_err(
            "StarRocks stream load returned an empty response body",
        ));
    }

    let value: Value = serde_json::from_str(response_body).map_err(|err| {
        PyRuntimeError::new_err(format!(
            "failed to parse StarRocks stream load response JSON: {err}"
        ))
    })?;

    if let Some(status) = value.get("Status").and_then(Value::as_str) {
        if status.eq_ignore_ascii_case("Success")
            || status.eq_ignore_ascii_case("Publish Timeout")
        {
            return Ok(());
        }

        let message = value
            .get("Message")
            .or_else(|| value.get("msg"))
            .and_then(Value::as_str)
            .unwrap_or("unknown StarRocks error");

        return Err(PyRuntimeError::new_err(format!(
            "StarRocks stream load failed: {status} - {message}"
        )));
    }

    Err(PyRuntimeError::new_err(
        "StarRocks stream load response does not contain a Status field",
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, mpsc};
    use std::thread;

    use base64::Engine;

    use super::{stream_load_json, validate_stream_load_response};

    #[test]
    fn accepts_success_response() {
        validate_stream_load_response(r#"{"Status":"Success","Message":"OK"}"#).unwrap();
        validate_stream_load_response(r#"{"Status":"Publish Timeout","Message":"visible later"}"#)
            .unwrap();
    }

    #[test]
    fn rejects_failed_response() {
        let err = validate_stream_load_response(r#"{"Status":"Fail","Message":"bad data"}"#)
            .unwrap_err();
        assert!(err.to_string().contains("bad data"));
    }

    #[test]
    fn sends_json_stream_load_request() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();

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

            tx.send((request_line, headers, body)).unwrap();

            let response_body = r#"{"Status":"Success","Message":"OK","NumberLoadedRows":2}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            )
            .unwrap();
        });

        let response = stream_load_json(
            &format!("http://{addr}"),
            "test_db",
            "test_table",
            "root",
            "secret",
            br#"[{"id":1},{"id":2}]"#.to_vec(),
            Some("load-1"),
        )
        .unwrap();

        let (request_line, headers, body) = rx.recv().unwrap();

        assert_eq!(request_line, "PUT /api/test_db/test_table/_stream_load HTTP/1.1\r\n");
        assert_eq!(body, r#"[{"id":1},{"id":2}]"#);
        assert_eq!(headers.get("content-type").unwrap(), "application/json");
        assert_eq!(headers.get("format").unwrap(), "json");
        assert_eq!(headers.get("strip_outer_array").unwrap(), "true");
        assert_eq!(headers.get("label").unwrap(), "load-1");
        assert_eq!(
            headers.get("authorization").unwrap(),
            &format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode("root:secret")
            )
        );
        assert!(response.contains(r#""Status":"Success""#));

        server.join().unwrap();
    }

    #[test]
    fn retries_stream_load_on_redirect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let redirect_path = "/api/test_db/test_table/_stream_load_be";
        let seen_requests: Arc<std::sync::Mutex<Vec<(String, HashMap<String, String>, String)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_requests_for_server = Arc::clone(&seen_requests);

        let server = thread::spawn(move || {
            for request_index in 0..2 {
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

                seen_requests_for_server
                    .lock()
                    .unwrap()
                    .push((request_line.clone(), headers, body));

                if request_index == 0 {
                    let location = format!("http://{addr}{redirect_path}");
                    write!(
                        stream,
                        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        location
                    )
                    .unwrap();
                } else {
                    let response_body =
                        r#"{"Status":"Success","Message":"OK","NumberLoadedRows":2}"#;
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    )
                    .unwrap();
                }
            }
        });

        let response = stream_load_json(
            &format!("http://{addr}"),
            "test_db",
            "test_table",
            "root",
            "secret",
            br#"[{"id":1},{"id":2}]"#.to_vec(),
            Some("load-redirect"),
        )
        .unwrap();

        let seen_requests = seen_requests.lock().unwrap();
        assert_eq!(seen_requests.len(), 2);
        assert_eq!(
            seen_requests[0].0,
            "PUT /api/test_db/test_table/_stream_load HTTP/1.1\r\n"
        );
        assert_eq!(
            seen_requests[1].0,
            format!("PUT {redirect_path} HTTP/1.1\r\n")
        );
        assert_eq!(seen_requests[0].2, r#"[{"id":1},{"id":2}]"#);
        assert_eq!(seen_requests[1].2, r#"[{"id":1},{"id":2}]"#);
        assert_eq!(seen_requests[1].1.get("label").unwrap(), "load-redirect");
        assert!(response.contains(r#""Status":"Success""#));

        drop(seen_requests);
        server.join().unwrap();
    }
}
