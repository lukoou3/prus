//! StarRocks query: stream NDJSON into Arrow column builders (no intermediate Vec<Vec<Value>>).

use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;

use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, ListArray, RecordBatch};
use base64::Engine;
use chrono::{NaiveDate, NaiveDateTime};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;
use pyo3_arrow::PyTable;
use serde_json::{Value, json};
use ureq::Agent;
use ureq::config::Config;

#[derive(Debug, Clone)]
struct StarRocksColumn {
    name: String,
    starrocks_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StarRocksType {
    Boolean,
    Date,
    Int32,
    Int64,
    Decimal128(u8, i8),
    Float64,
    Datetime,
    String,
    Array(Box<StarRocksType>),
    Unknown(String),
}

// ----- trait: build one Arrow column from a stream of JSON values -----

/// Builds an Arrow column by appending one value at a time (no intermediate Vec<Vec<Value>>).
/// Returns Result (not PyResult) so the same impl can be used inside py.detach() without creating PyErr.
trait ArrowColumnBuilder {
    fn append(&mut self, value: Option<&Value>, column_name: &str) -> Result<(), String>;
    fn finish(self) -> Result<ArrayRef, String>;
}

/// One builder per column type; list uses inner builder + lengths/null bitmap.
enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Date32(Date32Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Decimal128(Decimal128Builder, u8, i8),
    Float64(Float64Builder),
    Datetime(TimestampMicrosecondBuilder),
    String(StringBuilder),
    List {
        inner: Box<ColumnBuilder>,
        lengths: Vec<usize>,
        nulls: Vec<bool>,
        item_type: StarRocksType,
    },
}

impl ArrowColumnBuilder for ColumnBuilder {
    fn append(&mut self, value: Option<&Value>, column_name: &str) -> Result<(), String> {
        match self {
            ColumnBuilder::Boolean(b) => {
                match value {
                    Some(Value::Null) | None => b.append_null(),
                    Some(Value::Bool(v)) => b.append_value(*v),
                    Some(v) => {
                        let parsed = value_to_string(v)
                            .parse::<bool>()
                            .map_err(|e| format!("failed to parse boolean for column '{}': {}", column_name, e))?;
                        b.append_value(parsed);
                    }
                }
                Ok(())
            }
            ColumnBuilder::Date32(b) => {
                match value {
                    Some(Value::Null) | None => b.append_null(),
                    Some(Value::String(s)) => {
                        let parsed = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .map_err(|e| format!("failed to parse date for column '{}': {}", column_name, e))?;
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let days = parsed.signed_duration_since(epoch).num_days();
                        let days = i32::try_from(days)
                            .map_err(|e| format!("date overflow for column '{}': {}", column_name, e))?;
                        b.append_value(days);
                    }
                    Some(v) => {
                        return Err(format!("unsupported date JSON value for column '{}': {}", column_name, v));
                    }
                }
                Ok(())
            }
            ColumnBuilder::Int32(b) => append_int32(b, value, column_name),
            ColumnBuilder::Int64(b) => append_int64(b, value, column_name),
            ColumnBuilder::Decimal128(b, _p, scale) => append_decimal128(b, *scale, value, column_name),
            ColumnBuilder::Float64(b) => append_float64(b, value, column_name),
            ColumnBuilder::Datetime(b) => append_datetime(b, value, column_name),
            ColumnBuilder::String(b) => {
                match value {
                    Some(Value::Null) | None => b.append_null(),
                    Some(v) => b.append_value(value_to_string(v)),
                }
                Ok(())
            }
            ColumnBuilder::List { inner, lengths, nulls, .. } => {
                match value {
                    Some(Value::Null) | None => {
                        lengths.push(0);
                        nulls.push(false);
                    }
                    Some(Value::Array(arr)) => {
                        for item in arr.iter() {
                            inner.append(Some(item), column_name)?;
                        }
                        lengths.push(arr.len());
                        nulls.push(true);
                    }
                    Some(v) => {
                        return Err(format!("unsupported array JSON value for column '{}': {}", column_name, v));
                    }
                }
                Ok(())
            }
        }
    }

    fn finish(self) -> Result<ArrayRef, String> {
        match self {
            ColumnBuilder::Boolean(mut b) => Ok(Arc::new(b.finish())),
            ColumnBuilder::Date32(mut b) => Ok(Arc::new(b.finish())),
            ColumnBuilder::Int32(mut b) => Ok(Arc::new(b.finish())),
            ColumnBuilder::Int64(mut b) => Ok(Arc::new(b.finish())),
            ColumnBuilder::Decimal128(mut b, _p, _s) => Ok(Arc::new(b.finish())),
            ColumnBuilder::Float64(mut b) => Ok(Arc::new(b.finish())),
            ColumnBuilder::Datetime(mut b) => Ok(Arc::new(b.finish())),
            ColumnBuilder::String(mut b) => Ok(Arc::new(b.finish())),
            ColumnBuilder::List {
                inner,
                lengths,
                nulls,
                item_type,
            } => {
                let child = inner.finish()?;
                let offsets = OffsetBuffer::from_lengths(lengths);
                let null_buffer = if nulls.iter().all(|x| *x) {
                    None
                } else {
                    Some(NullBuffer::new(BooleanBuffer::from(nulls)))
                };
                let field = Arc::new(Field::new(
                    "item",
                    starrocks_type_to_arrow_data_type(&item_type),
                    true,
                ));
                Ok(Arc::new(ListArray::new(
                    field, offsets, child, null_buffer,
                )))
            }
        }
    }
}

fn append_int32(b: &mut Int32Builder, value: Option<&Value>, column_name: &str) -> Result<(), String> {
    match value {
        Some(Value::Null) | None => b.append_null(),
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                let i = i32::try_from(i)
                    .map_err(|e| format!("integer overflow for column '{}': {}", column_name, e))?;
                b.append_value(i);
            } else if let Some(u) = n.as_u64() {
                let i = i32::try_from(u)
                    .map_err(|e| format!("integer overflow for column '{}': {}", column_name, e))?;
                b.append_value(i);
            } else {
                return Err(format!("unsupported integer value for column '{}'", column_name));
            }
        }
        Some(Value::String(s)) => {
            let i = s
                .parse::<i32>()
                .map_err(|e| format!("failed to parse integer for column '{}': {}", column_name, e))?;
            b.append_value(i);
        }
        Some(v) => return Err(format!("unsupported integer JSON value for column '{}': {}", column_name, v)),
    }
    Ok(())
}

fn append_int64(b: &mut Int64Builder, value: Option<&Value>, column_name: &str) -> Result<(), String> {
    match value {
        Some(Value::Null) | None => b.append_null(),
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                b.append_value(i);
            } else if let Some(u) = n.as_u64() {
                let i = i64::try_from(u)
                    .map_err(|e| format!("integer overflow for column '{}': {}", column_name, e))?;
                b.append_value(i);
            } else {
                return Err(format!("unsupported integer value for column '{}'", column_name));
            }
        }
        Some(Value::String(s)) => {
            let i = s
                .parse::<i64>()
                .map_err(|e| format!("failed to parse integer for column '{}': {}", column_name, e))?;
            b.append_value(i);
        }
        Some(v) => return Err(format!("unsupported integer JSON value for column '{}': {}", column_name, v)),
    }
    Ok(())
}

fn append_decimal128(
    b: &mut Decimal128Builder,
    scale: i8,
    value: Option<&Value>,
    column_name: &str,
) -> Result<(), String> {
    match value {
        Some(Value::Null) | None => b.append_null(),
        Some(Value::Number(n)) => {
            let parsed = parse_decimal_to_i128(&n.to_string(), scale)
                .map_err(|e| format!("failed to parse decimal for column '{}': {}", column_name, e))?;
            b.append_value(parsed);
        }
        Some(Value::String(s)) => {
            let parsed = parse_decimal_to_i128(s, scale)
                .map_err(|e| format!("failed to parse decimal for column '{}': {}", column_name, e))?;
            b.append_value(parsed);
        }
        Some(v) => {
            return Err(format!("unsupported decimal JSON value for column '{}': {}", column_name, v));
        }
    }
    Ok(())
}

fn append_float64(b: &mut Float64Builder, value: Option<&Value>, column_name: &str) -> Result<(), String> {
    match value {
        Some(Value::Null) | None => b.append_null(),
        Some(Value::Number(n)) => {
            let f = n.as_f64()
                .ok_or_else(|| format!("failed to convert numeric value for column '{}'", column_name))?;
            b.append_value(f);
        }
        Some(Value::String(s)) => {
            let f = s
                .parse::<f64>()
                .map_err(|e| format!("failed to parse float for column '{}': {}", column_name, e))?;
            b.append_value(f);
        }
        Some(v) => {
            return Err(format!("unsupported float JSON value for column '{}': {}", column_name, v));
        }
    }
    Ok(())
}

fn append_datetime(
    b: &mut TimestampMicrosecondBuilder,
    value: Option<&Value>,
    column_name: &str,
) -> Result<(), String> {
    match value {
        Some(Value::Null) | None => b.append_null(),
        Some(Value::String(s)) => {
            let parsed = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                .map_err(|e| format!("failed to parse datetime for column '{}': {}", column_name, e))?;
            b.append_value(parsed.and_utc().timestamp_micros());
        }
        Some(v) => {
            return Err(format!("unsupported datetime JSON value for column '{}': {}", column_name, v));
        }
    }
    Ok(())
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn parse_decimal_to_i128(value: &str, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err("negative decimal scale is not supported".to_string());
    }
    let value = value.trim();
    if value.is_empty() {
        return Err("empty decimal string".to_string());
    }
    let negative = value.starts_with('-');
    let digits = if negative || value.starts_with('+') {
        &value[1..]
    } else {
        value
    };
    let mut parts = digits.split('.');
    let integer_part = parts.next().unwrap_or("");
    let fractional_part = parts.next().unwrap_or("");
    if parts.next().is_some() {
        return Err(format!("invalid decimal literal '{}'", value));
    }
    if !integer_part.chars().all(|c| c.is_ascii_digit())
        || !fractional_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid decimal literal '{}'", value));
    }
    let scale_u = usize::try_from(scale).map_err(|e| e.to_string())?;
    let integer_value = if integer_part.is_empty() {
        0_i128
    } else {
        integer_part.parse::<i128>().map_err(|e| e.to_string())?
    };
    let mut fraction = fractional_part.to_string();
    if fraction.len() > scale_u {
        fraction.truncate(scale_u);
    } else {
        fraction.extend(std::iter::repeat_n('0', scale_u - fraction.len()));
    }
    let fractional_value = if fraction.is_empty() {
        0_i128
    } else {
        fraction.parse::<i128>().map_err(|e| e.to_string())?
    };
    let scaled = integer_value
        .checked_mul(10_i128.pow(scale_u as u32))
        .and_then(|v| v.checked_add(fractional_value))
        .ok_or_else(|| format!("decimal overflow for '{}'", value))?;
    Ok(if negative { -scaled } else { scaled })
}

fn parse_starrocks_type(starrocks_type: &str) -> Result<StarRocksType, String> {
    let normalized = starrocks_type.trim().to_ascii_lowercase();
    if normalized.starts_with("array<") && normalized.ends_with('>') {
        let inner = &normalized["array<".len()..normalized.len() - 1];
        return Ok(StarRocksType::Array(Box::new(parse_starrocks_type(inner)?)));
    }
    let base = normalized.split('(').next().unwrap_or(&normalized).trim();
    Ok(match base {
        "bool" | "boolean" => StarRocksType::Boolean,
        "date" => StarRocksType::Date,
        "tinyint" | "smallint" | "int" | "integer" => StarRocksType::Int32,
        "bigint" => StarRocksType::Int64,
        "decimal" | "decimal32" | "decimal64" | "decimal128" => {
            let (precision, scale) = parse_decimal_precision_scale(&normalized).ok_or_else(|| {
                format!("failed to parse decimal precision/scale from type '{}'", starrocks_type)
            })?;
            StarRocksType::Decimal128(precision, scale)
        }
        "float" | "double" => StarRocksType::Float64,
        "datetime" => StarRocksType::Datetime,
        "char" | "varchar" | "string" | "text" => StarRocksType::String,
        _ => StarRocksType::Unknown(starrocks_type.to_string()),
    })
}

fn parse_decimal_precision_scale(decimal_type: &str) -> Option<(u8, i8)> {
    let start = decimal_type.find('(')?;
    let end = decimal_type.rfind(')')?;
    let params = &decimal_type[start + 1..end];
    let mut parts = params.split(',').map(|p| p.trim());
    let precision = parts.next()?.parse::<u8>().ok()?;
    let scale = parts.next()?.parse::<i8>().ok()?;
    Some((precision, scale))
}

fn starrocks_type_to_arrow_data_type(starrocks_type: &StarRocksType) -> DataType {
    match starrocks_type {
        StarRocksType::Boolean => DataType::Boolean,
        StarRocksType::Date => DataType::Date32,
        StarRocksType::Int32 => DataType::Int32,
        StarRocksType::Int64 => DataType::Int64,
        StarRocksType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
        StarRocksType::Float64 => DataType::Float64,
        StarRocksType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
        StarRocksType::String | StarRocksType::Unknown(_) => DataType::Utf8,
        StarRocksType::Array(inner) => DataType::List(Arc::new(Field::new(
            "item",
            starrocks_type_to_arrow_data_type(inner),
            true,
        ))),
    }
}

fn build_schema(columns: &[StarRocksColumn]) -> Result<SchemaRef, String> {
    let fields = columns
        .iter()
        .map(|c| {
            let t = parse_starrocks_type(&c.starrocks_type)?;
            Ok(Field::new(
                &c.name,
                starrocks_type_to_arrow_data_type(&t),
                true,
            ))
        })
        .collect::<Result<Vec<Field>, String>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn parse_col_types_from_meta(meta_str: &str) -> Result<Vec<StarRocksColumn>, String> {
    let json: Value = serde_json::from_str(meta_str)
        .map_err(|e| format!("failed to parse StarRocks meta line: {e}"))?;
    let meta_array = json
        .get("meta")
        .and_then(Value::as_array)
        .ok_or("missing 'meta' or not array")?;
    let mut result = Vec::with_capacity(meta_array.len());
    for item in meta_array {
        let obj = item.as_object().ok_or("meta item not object")?;
        let name = obj
            .get("name")
            .and_then(Value::as_str)
            .ok_or("missing column name")?
            .to_string();
        let starrocks_type = obj
            .get("type")
            .and_then(Value::as_str)
            .ok_or("missing column type")?
            .to_string();
        result.push(StarRocksColumn {
            name,
            starrocks_type,
        });
    }
    Ok(result)
}

/// Create one column builder from column metadata (no row count yet; we grow as we append).
fn make_builder(column: &StarRocksColumn) -> Result<ColumnBuilder, String> {
    let t = parse_starrocks_type(&column.starrocks_type)?;
    Ok(match &t {
        StarRocksType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new()),
        StarRocksType::Date => ColumnBuilder::Date32(Date32Builder::new()),
        StarRocksType::Int32 => ColumnBuilder::Int32(Int32Builder::new()),
        StarRocksType::Int64 => ColumnBuilder::Int64(Int64Builder::new()),
        StarRocksType::Decimal128(p, s) => ColumnBuilder::Decimal128(
            Decimal128Builder::new().with_data_type(DataType::Decimal128(*p, *s)),
            *p,
            *s,
        ),
        StarRocksType::Float64 => ColumnBuilder::Float64(Float64Builder::new()),
        StarRocksType::Datetime => ColumnBuilder::Datetime(TimestampMicrosecondBuilder::new()),
        StarRocksType::String | StarRocksType::Unknown(_) => {
            ColumnBuilder::String(StringBuilder::new())
        }
        StarRocksType::Array(inner) => ColumnBuilder::List {
            inner: Box::new(make_builder_for_type(inner, "item")?),
            lengths: Vec::new(),
            nulls: Vec::new(),
            item_type: *inner.clone(),
        },
    })
}

fn make_builder_for_type(item_type: &StarRocksType, _name: &str) -> Result<ColumnBuilder, String> {
    match item_type {
        StarRocksType::Boolean => Ok(ColumnBuilder::Boolean(BooleanBuilder::new())),
        StarRocksType::Date => Ok(ColumnBuilder::Date32(Date32Builder::new())),
        StarRocksType::Int32 => Ok(ColumnBuilder::Int32(Int32Builder::new())),
        StarRocksType::Int64 => Ok(ColumnBuilder::Int64(Int64Builder::new())),
        StarRocksType::Decimal128(p, s) => Ok(ColumnBuilder::Decimal128(
            Decimal128Builder::new().with_data_type(DataType::Decimal128(*p, *s)),
            *p,
            *s,
        )),
        StarRocksType::Float64 => Ok(ColumnBuilder::Float64(Float64Builder::new())),
        StarRocksType::Datetime => Ok(ColumnBuilder::Datetime(TimestampMicrosecondBuilder::new())),
        StarRocksType::String | StarRocksType::Unknown(_) => {
            Ok(ColumnBuilder::String(StringBuilder::new()))
        }
        StarRocksType::Array(inner) => Ok(ColumnBuilder::List {
            inner: Box::new(make_builder_for_type(inner, "item")?),
            lengths: Vec::new(),
            nulls: Vec::new(),
            item_type: *inner.clone(),
        }),
    }
}

/// Parse StarRocks NDJSON response from a reader into (schema, record batch).
/// Returns PyResult for use when holding GIL (e.g. tests).
fn parse_starrocks_response<R: BufRead>(reader: R) -> PyResult<(SchemaRef, RecordBatch)> {
    parse_starrocks_response_impl(reader).map_err(PyRuntimeError::new_err)
}

/// Same as parse_starrocks_response but returns Result<_, String>. Safe to call inside py.detach().
fn parse_starrocks_response_impl<R: BufRead>(reader: R) -> Result<(SchemaRef, RecordBatch), String> {
    let mut columns: Option<Vec<StarRocksColumn>> = None;
    let mut builders: Vec<ColumnBuilder> = Vec::new();

    for line_result in reader.lines() {
        let line = line_result.map_err(|e| format!("failed to read StarRocks response line: {e}"))?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with("{\"status\"") {
            let value: Value = serde_json::from_str(line)
                .map_err(|e| format!("failed to parse StarRocks status line: {e}"))?;
            let status = value.get("status").and_then(Value::as_str).unwrap_or("");
            if status.eq_ignore_ascii_case("FAILED") {
                let msg = value
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown StarRocks error");
                return Err(format!("StarRocks query failed: {msg}"));
            }
            continue;
        }
        if line.starts_with("{\"meta\"") {
            columns = Some(parse_col_types_from_meta(line)?);
            builders = columns
                .as_ref()
                .unwrap()
                .iter()
                .map(make_builder)
                .collect::<Result<Vec<ColumnBuilder>, String>>()?;
            continue;
        }
        if !line.starts_with("{\"data\"") {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .map_err(|e| format!("failed to parse StarRocks data line: {e}"))?;
        let data = value
            .get("data")
            .and_then(Value::as_array)
            .ok_or("StarRocks data line does not contain an array")?;
        for (i, b) in builders.iter_mut().enumerate() {
            let cell = data.get(i);
            let column_name = &columns.as_ref().unwrap()[i].name;
            b.append(cell, column_name)?;
        }
    }

    let columns = columns.ok_or("StarRocks response does not contain column metadata")?;
    let schema = build_schema(&columns)?;
    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(ArrowColumnBuilder::finish)
        .collect::<Result<Vec<ArrayRef>, String>>()?;
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| format!("failed to build Arrow batch: {e}"))?;
    Ok((schema, batch))
}

/// HTTP + stream parse. Returns Result so it can be used inside py.detach() without creating PyErr.
/// Reads directly from the response stream (no full body Vec<u8>).
fn query_starrocks_arrow_impl(
    base_url: &str,
    catalog: &str,
    database: &str,
    username: &str,
    password: &str,
    query: &str,
) -> Result<(SchemaRef, RecordBatch), String> {
    let base_url = base_url.trim_end_matches('/');
    let url = format!("{base_url}/api/v1/catalogs/{catalog}/databases/{database}/sql");
    let credentials = format!("{username}:{password}");
    let authorization = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(credentials)
    );

    let agent = Agent::new_with_config(Config::builder().http_status_as_error(false).build());
    let response = agent.post(&url)
        .header("Authorization", &authorization)
        .send_json(json!({ "query": query }))
        .map_err(|e| format!("StarRocks HTTP request failed: {e}"))?;
    let status = response.status().as_u16();
    if status != 200 {
        let mut response_body = String::new();
        response.into_body().into_reader()
            .read_to_string(&mut response_body)
            .map_err(|err| format!("failed to read http response: {err}"))?;
        return Err(format!(
            "StarRocks query failed with status {status}: {response_body}"
        ));
    }
    
    let reader = BufReader::new(response.into_body().into_reader());
    parse_starrocks_response_impl(reader)
}

/// Query StarRocks and return a PyArrow Table.
///
/// Uses HTTP interface with NDJSON format. Streams response and parses directly into Arrow builders
/// without intermediate JSON objects for better memory efficiency.
/// HTTP and parsing run without holding the GIL for better multithreading.
///
/// Args:
///     base_url: StarRocks HTTP endpoint (e.g., "http://localhost:8030")
///     database: Database name to query
///     query: SQL query
///     username: StarRocks username (default: "root")
///     password: StarRocks password (default: "")
///     catalog: Catalog name (default: "default_catalog")
///
/// Returns:
///     PyArrow Table containing the query results
///
/// Raises:
///     RuntimeError: If HTTP request fails or JSON parsing fails
///
/// Examples:
///     >>> import prus
///     >>> table = prus.query_starrocks_arrow("http://localhost:8030", "my_db", "SELECT * FROM events LIMIT 1000")
///     >>> # With authentication
///     >>> table = prus.query_starrocks_arrow("http://localhost:8030", "my_db", "SELECT count()", "admin", "secret")
#[pyfunction]
#[pyo3(signature = (base_url, database, query, username="root", password="", catalog="default_catalog"))]
pub fn query_starrocks_arrow<'py>(
    py: Python<'py>,
    base_url: &str,
    database: &str,
    query: &str,
    username: &str,
    password: &str,
    catalog: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let base_url = base_url.to_string();
    let catalog = catalog.to_string();
    let database = database.to_string();
    let username = username.to_string();
    let password = password.to_string();
    let query = query.to_string();
    let (schema, batch) = py
        .detach(|| {
            query_starrocks_arrow_impl(
                &base_url,
                &catalog,
                &database,
                &username,
                &password,
                &query,
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    PyTable::try_new(vec![batch], schema)?
        .into_pyarrow(py)
        .map_err(|e| {
            PyRuntimeError::new_err(format!("failed to convert to pyarrow.Table: {e}"))
        })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(query_starrocks_arrow, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, TimeUnit};
    use arrow_array::{
        Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, ListArray,
        StringArray, TimestampMicrosecondArray,
    };

    use super::{parse_col_types_from_meta, parse_starrocks_response, query_starrocks_arrow_impl};

    #[test]
    fn parses_meta_line() {
        let meta_line = r#"{"meta":[{"name":"id","type":"int(11)"},{"name":"amount","type":"double"},{"name":"name","type":"varchar"}]}"#;

        let columns = parse_col_types_from_meta(meta_line).unwrap();

        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].starrocks_type, "int(11)");
        assert_eq!(columns[2].name, "name");
        assert_eq!(columns[2].starrocks_type, "varchar");
    }

    #[test]
    fn parses_starrocks_ndjson_lines() {
        let response = concat!(
            "{\"connectionId\":49}\n",
            "{\"meta\":[",
            "{\"name\":\"id\",\"type\":\"int(11)\"},",
            "{\"name\":\"birthday\",\"type\":\"date\"},",
            "{\"name\":\"amount\",\"type\":\"decimal64(10, 2)\"},",
            "{\"name\":\"score\",\"type\":\"double\"},",
            "{\"name\":\"created_at\",\"type\":\"datetime\"},",
            "{\"name\":\"enabled\",\"type\":\"boolean\"},",
            "{\"name\":\"name\",\"type\":\"varchar\"}",
            "]}\n",
            "{\"data\":[1,\"2024-01-02\",\"123.45\",12.5,\"2024-01-02 03:04:05\",true,\"alice\"]}\n",
            "{\"data\":[2,\"2024-01-03\",\"-0.12\",\"13.75\",\"2024-01-02 03:04:05.123456\",false,\"bob\"]}\n",
            "{\"statistics\":{\"returnRows\":2}}\n"
        );

        let (schema, batch) = parse_starrocks_response(Cursor::new(response)).unwrap();

        assert_eq!(schema.fields().len(), 7);
        assert_eq!(batch.num_rows(), 2);
        let name_col = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_col.value(0), "alice");
        assert_eq!(name_col.value(1), "bob");
    }

    #[test]
    fn converts_rows_to_arrow_arrays() {
        let response = concat!(
            "{\"connectionId\":49}\n",
            "{\"meta\":[",
            "{\"name\":\"id\",\"type\":\"int(11)\"},",
            "{\"name\":\"birthday\",\"type\":\"date\"},",
            "{\"name\":\"amount\",\"type\":\"decimal64(10, 2)\"},",
            "{\"name\":\"score\",\"type\":\"double\"},",
            "{\"name\":\"created_at\",\"type\":\"datetime\"},",
            "{\"name\":\"enabled\",\"type\":\"boolean\"},",
            "{\"name\":\"name\",\"type\":\"varchar\"}",
            "]}\n",
            "{\"data\":[1,\"2024-01-02\",\"123.45\",12.5,\"2024-01-02 03:04:05\",true,\"alice\"]}\n",
            "{\"data\":[2,\"2024-01-03\",\"-0.12\",\"13.75\",\"2024-01-02 03:04:05.123456\",false,\"bob\"]}\n",
            "{\"data\":[null,null,null,null,null,null,null]}\n"
        );

        let (schema, batch) = parse_starrocks_response(Cursor::new(response)).unwrap();
        let arrays = batch.columns();

        assert_eq!(schema.fields().len(), 7);
        assert_eq!(arrays.len(), 7);
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).data_type(), &DataType::Date32);
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(
            schema.field(4).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        let id_array = arrays[0].as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert!(id_array.is_null(2));

        let birthday_array = arrays[1].as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(birthday_array.value(0), 19724);
        assert_eq!(birthday_array.value(1), 19725);
        assert!(birthday_array.is_null(2));

        let amount_array = arrays[2]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(amount_array.value(0), 12345);
        assert_eq!(amount_array.value(1), -12);
        assert!(amount_array.is_null(2));

        let score_array = arrays[3].as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(score_array.value(0), 12.5);
        assert_eq!(score_array.value(1), 13.75);
        assert!(score_array.is_null(2));

        let created_at_array = arrays[4]
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(created_at_array.value(0), 1704164645000000);
        assert_eq!(created_at_array.value(1), 1704164645123456);
        assert!(created_at_array.is_null(2));

        let enabled_array = arrays[5].as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(enabled_array.value(0));
        assert!(!enabled_array.value(1));
        assert!(enabled_array.is_null(2));

        let name_array = arrays[6].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_array.value(0), "alice");
        assert_eq!(name_array.value(1), "bob");
        assert!(name_array.is_null(2));
    }

    #[test]
    fn converts_array_rows_to_arrow_lists() {
        let response = concat!(
            "{\"connectionId\":49}\n",
            "{\"meta\":[",
            "{\"name\":\"ids\",\"type\":\"array<int(11)>\"},",
            "{\"name\":\"tags\",\"type\":\"array<varchar>\"}",
            "]}\n",
            "{\"data\":[[1,2,3],[\"a\",\"b\"]]}\n",
            "{\"data\":[[],[]]}\n",
            "{\"data\":[null,null]}\n"
        );

        let (schema, batch) = parse_starrocks_response(Cursor::new(response)).unwrap();
        let arrays = batch.columns();

        assert_eq!(
            schema.field(0).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );

        let ids = arrays[0].as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids.value_offsets(), &[0, 3, 3, 3]);
        assert!(ids.is_null(2));
        let ids_values = ids.values();
        let ids_values = ids_values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids_values.value(0), 1);
        assert_eq!(ids_values.value(1), 2);
        assert_eq!(ids_values.value(2), 3);

        let tags = arrays[1].as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(tags.value_offsets(), &[0, 2, 2, 2]);
        assert!(tags.is_null(2));
        let tags_values = tags.values();
        let tags_values = tags_values.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(tags_values.value(0), "a");
        assert_eq!(tags_values.value(1), "b");
    }

    #[test]
    #[ignore = "requires a reachable StarRocks instance"]
    fn live_starrocks_query_to_arrow() {
        let url = "http://127.0.0.1:8030";
        let database = "test";
        let username = "root";
        let password = "";
        let catalog = "default_catalog";
        let sql = "select * from test_types limit 10";

        let (schema, batch) =
            query_starrocks_arrow_impl(url, catalog, database, username, password, sql).unwrap();

        assert!(!schema.fields().is_empty());
        assert_eq!(batch.num_columns(), schema.fields().len());
        for (i, field) in schema.fields().iter().enumerate() {
            println!("{}: {:?}", field.name(), batch.column(i));
        }
    }
}
