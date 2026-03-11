use std::io::{BufRead, BufReader};
use std::sync::Arc;

use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int32Builder,
    Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
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
    let (columns, rows) = query_starrocks_rows(
        base_url,
        catalog,
        database,
        username,
        password,
        query,
    )?;
    let schema = build_schema(&columns)?;
    let arrays = build_arrays(&columns, &rows)?;
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to build Arrow batch: {err}")))?;

    PyTable::try_new(vec![batch], schema)?
        .into_pyarrow(py)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to convert to pyarrow.Table: {err}")))
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(query_starrocks_arrow, module)?)?;
    Ok(())
}

fn query_starrocks_rows(
    base_url: &str,
    catalog: &str,
    database: &str,
    username: &str,
    password: &str,
    query: &str,
) -> PyResult<(Vec<StarRocksColumn>, Vec<Vec<Value>>)> {
    let base_url = base_url.trim_end_matches('/');
    let url = format!(
        "{base_url}/api/v1/catalogs/{catalog}/databases/{database}/sql"
    );
    let credentials = format!("{username}:{password}");
    let authorization = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(credentials)
    );
    let response = ureq::post(&url)
        .header("Authorization", &authorization)
        .send_json(json!({ "query": query }))
        .map_err(|err| PyRuntimeError::new_err(format!("StarRocks HTTP request failed: {err}")))?;

    let reader = BufReader::new(response.into_body().into_reader());
    parse_starrocks_lines(reader)
}

fn parse_starrocks_lines<R: BufRead>(reader: R) -> PyResult<(Vec<StarRocksColumn>, Vec<Vec<Value>>)> {
    let mut columns: Option<Vec<StarRocksColumn>> = None;
    let mut rows = Vec::new();

    for line_result in reader.lines() {
        let line = line_result.map_err(|err| {
            PyRuntimeError::new_err(format!("failed to read StarRocks response line: {err}"))
        })?;
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        if line.starts_with("{\"status\"") {
            let value: Value = serde_json::from_str(line).map_err(|err| {
                PyRuntimeError::new_err(format!("failed to parse StarRocks status line: {err}"))
            })?;
            let status = value.get("status").and_then(Value::as_str).unwrap_or("");
            if status.eq_ignore_ascii_case("FAILED") {
                let msg = value
                    .get("msg")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown StarRocks error");
                return Err(PyRuntimeError::new_err(format!("StarRocks query failed: {msg}")));
            }
            continue;
        }

        if line.starts_with("{\"meta\"") {
            columns = Some(parse_col_types_from_meta(line)?);
            continue;
        }

        if !line.starts_with("{\"data\"") {
            continue;
        }

        let value: Value = serde_json::from_str(line).map_err(|err| {
            PyRuntimeError::new_err(format!("failed to parse StarRocks data line: {err}"))
        })?;
        let data = value
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| PyRuntimeError::new_err("StarRocks data line does not contain an array"))?;
        rows.push(data.clone());
    }

    let columns = columns
        .ok_or_else(|| PyRuntimeError::new_err("StarRocks response does not contain column metadata"))?;

    Ok((columns, rows))
}

fn parse_col_types_from_meta(meta_str: &str) -> PyResult<Vec<StarRocksColumn>> {
    let json: Value = serde_json::from_str(meta_str)
        .map_err(|err| PyRuntimeError::new_err(format!("failed to parse StarRocks meta line: {err}")))?;

    let meta_array = json
        .get("meta")
        .and_then(Value::as_array)
        .ok_or_else(|| PyRuntimeError::new_err("missing 'meta' field or 'meta' is not an array"))?;

    let mut result = Vec::with_capacity(meta_array.len());

    for item in meta_array {
        let object = item
            .as_object()
            .ok_or_else(|| PyRuntimeError::new_err("meta array item is not an object"))?;

        let name = object
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| PyRuntimeError::new_err("missing column name in meta"))?
            .to_string();

        let starrocks_type = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| PyRuntimeError::new_err("missing column type in meta"))?
            .to_string();

        result.push(StarRocksColumn {
            name,
            starrocks_type,
        });
    }

    Ok(result)
}

fn build_schema(columns: &[StarRocksColumn]) -> PyResult<SchemaRef> {
    let fields = columns
        .iter()
        .map(|column| {
            let starrocks_type = parse_starrocks_type(&column.starrocks_type)?;
            Ok(Field::new(
                &column.name,
                starrocks_type_to_arrow_data_type(&starrocks_type),
                true,
            ))
        })
        .collect::<PyResult<Vec<_>>>()?;

    Ok(Arc::new(Schema::new(fields)))
}

fn build_arrays(columns: &[StarRocksColumn], rows: &[Vec<Value>]) -> PyResult<Vec<ArrayRef>> {
    columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| build_array_for_column(column_index, column, rows))
        .collect()
}

fn build_array_for_column(
    column_index: usize,
    column: &StarRocksColumn,
    rows: &[Vec<Value>],
) -> PyResult<ArrayRef> {
    let starrocks_type = parse_starrocks_type(&column.starrocks_type)?;
    let values = rows.iter().map(|row| row.get(column_index)).collect::<Vec<_>>();

    build_array_from_values(&starrocks_type, &values, &column.name)
}

fn build_array_from_values(
    starrocks_type: &StarRocksType,
    values: &[Option<&Value>],
    column_name: &str,
) -> PyResult<ArrayRef> {
    let array: ArrayRef = match starrocks_type {
        StarRocksType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::Bool(value)) => builder.append_value(*value),
                    Some(value) => {
                        let parsed = value_to_string(value).parse::<bool>().map_err(|err| {
                            PyRuntimeError::new_err(format!(
                                "failed to parse boolean for column '{}': {err}",
                                column_name
                            ))
                        })?;
                        builder.append_value(parsed);
                    }
                }
            }
            Arc::new(builder.finish())
        }
        StarRocksType::Date => {
            let mut builder = Date32Builder::with_capacity(values.len());
            for value in values {
                match value {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::String(value)) => {
                        let parsed = NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|err| {
                            PyRuntimeError::new_err(format!(
                                "failed to parse date for column '{}': {err}",
                                column_name
                            ))
                        })?;
                        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                        let days = parsed.signed_duration_since(epoch).num_days();
                        let days = i32::try_from(days).map_err(|err| {
                            PyRuntimeError::new_err(format!(
                                "date overflow for column '{}': {err}",
                                column_name
                            ))
                        })?;
                        builder.append_value(days);
                    }
                    Some(value) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "unsupported date JSON value for column '{}': {}",
                            column_name,
                            value
                        )));
                    }
                }
            }
            Arc::new(builder.finish())
        }
        StarRocksType::Int32 => {
            let mut builder = Int32Builder::with_capacity(values.len());
            for value in values {
                match value {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::Number(value)) => {
                        if let Some(int_value) = value.as_i64() {
                            let int_value = i32::try_from(int_value).map_err(|err| {
                                PyRuntimeError::new_err(format!(
                                    "integer overflow for column '{}': {err}",
                                    column_name
                                ))
                            })?;
                            builder.append_value(int_value);
                        } else if let Some(uint_value) = value.as_u64() {
                            let int_value = i32::try_from(uint_value).map_err(|err| {
                                PyRuntimeError::new_err(format!(
                                    "integer overflow for column '{}': {err}",
                                    column_name
                                ))
                            })?;
                            builder.append_value(int_value);
                        } else {
                            return Err(PyRuntimeError::new_err(format!(
                                "unsupported integer value for column '{}'",
                                column_name
                            )));
                        }
                    }
                    Some(Value::String(value)) => {
                        let parsed = value.parse::<i32>().map_err(|err| {
                            PyRuntimeError::new_err(format!(
                                "failed to parse integer for column '{}': {err}",
                                column_name
                            ))
                        })?;
                        builder.append_value(parsed);
                    }
                    Some(value) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "unsupported integer JSON value for column '{}': {}",
                            column_name,
                            value
                        )));
                    }
                }
            }
            Arc::new(builder.finish())
        }
        StarRocksType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for value in values {
                match value {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::Number(value)) => {
                        if let Some(int_value) = value.as_i64() {
                            builder.append_value(int_value);
                        } else if let Some(uint_value) = value.as_u64() {
                            let int_value = i64::try_from(uint_value).map_err(|err| {
                                PyRuntimeError::new_err(format!(
                                    "integer overflow for column '{}': {err}",
                                    column_name
                                ))
                            })?;
                            builder.append_value(int_value);
                        } else {
                            return Err(PyRuntimeError::new_err(format!(
                                "unsupported integer value for column '{}'",
                                column_name
                            )));
                        }
                    }
                    Some(Value::String(value)) => {
                        let parsed = value.parse::<i64>().map_err(|err| {
                            PyRuntimeError::new_err(format!(
                                "failed to parse integer for column '{}': {err}",
                                column_name
                            ))
                        })?;
                        builder.append_value(parsed);
                    }
                    Some(value) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "unsupported integer JSON value for column '{}': {}",
                            column_name,
                            value
                        )));
                    }
                }
            }
            Arc::new(builder.finish())
        }
        StarRocksType::Decimal128(precision, scale) => {
            let mut builder = Decimal128Builder::with_capacity(values.len())
                .with_data_type(DataType::Decimal128(*precision, *scale));
            for value in values {
                match value {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::Number(value)) => {
                        let parsed = parse_decimal_to_i128(&value.to_string(), *scale).map_err(|err| {
                            PyRuntimeError::new_err(format!(
                                "failed to parse decimal for column '{}': {err}",
                                column_name
                            ))
                        })?;
                        builder.append_value(parsed);
                    }
                    Some(Value::String(value)) => {
                        let parsed = parse_decimal_to_i128(value, *scale).map_err(|err| {
                            PyRuntimeError::new_err(format!(
                                "failed to parse decimal for column '{}': {err}",
                                column_name
                            ))
                        })?;
                        builder.append_value(parsed);
                    }
                    Some(value) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "unsupported decimal JSON value for column '{}': {}",
                            column_name,
                            value
                        )));
                    }
                }
            }
            Arc::new(builder.finish())
        }
        StarRocksType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for value in values {
                match value {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::Number(value)) => {
                        let parsed = value.as_f64().ok_or_else(|| {
                            PyRuntimeError::new_err(format!(
                                "failed to convert numeric value for column '{}'",
                                column_name
                            ))
                        })?;
                        builder.append_value(parsed);
                    }
                    Some(Value::String(value)) => {
                        let parsed = value.parse::<f64>().map_err(|err| {
                            PyRuntimeError::new_err(format!(
                                "failed to parse float for column '{}': {err}",
                                column_name
                            ))
                        })?;
                        builder.append_value(parsed);
                    }
                    Some(value) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "unsupported float JSON value for column '{}': {}",
                            column_name,
                            value
                        )));
                    }
                }
            }
            Arc::new(builder.finish())
        }
        StarRocksType::Datetime => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
            for value in values {
                match value {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(Value::String(value)) => {
                        let parsed = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                            .map_err(|err| {
                                PyRuntimeError::new_err(format!(
                                    "failed to parse datetime for column '{}': {err}",
                                    column_name
                                ))
                            })?;
                        builder.append_value(parsed.and_utc().timestamp_micros());
                    }
                    Some(value) => {
                        return Err(PyRuntimeError::new_err(format!(
                            "unsupported datetime JSON value for column '{}': {}",
                            column_name,
                            value
                        )));
                    }
                }
            }
            Arc::new(builder.finish())
        }
        StarRocksType::String | StarRocksType::Unknown(_) => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for value in values {
                match value {
                    Some(Value::Null) | None => builder.append_null(),
                    Some(value) => builder.append_value(value_to_string(value)),
                }
            }
            Arc::new(builder.finish())
        }
        StarRocksType::Array(item_type) => build_list_array(item_type, values, column_name)?,
    };

    Ok(array)
}

fn build_list_array(
    item_type: &StarRocksType,
    values: &[Option<&Value>],
    column_name: &str,
) -> PyResult<ArrayRef> {
    let mut flattened = Vec::new();
    let mut lengths = Vec::with_capacity(values.len());
    let mut validity = Vec::with_capacity(values.len());

    for value in values {
        match value {
            Some(Value::Null) | None => {
                validity.push(false);
                lengths.push(0_usize);
            }
            Some(Value::Array(items)) => {
                validity.push(true);
                lengths.push(items.len());
                flattened.extend(items.iter().cloned());
            }
            Some(other) => {
                return Err(PyRuntimeError::new_err(format!(
                    "unsupported array JSON value for column '{}': {}",
                    column_name,
                    other
                )));
            }
        }
    }

    let flattened_refs = flattened.iter().map(Some).collect::<Vec<_>>();
    let child_array = build_array_from_values(item_type, &flattened_refs, column_name)?;
    let offsets = OffsetBuffer::from_lengths(lengths);
    let nulls = if validity.iter().all(|is_valid| *is_valid) {
        None
    } else {
        Some(NullBuffer::new(BooleanBuffer::from(validity)))
    };
    let field = Arc::new(Field::new(
        "item",
        starrocks_type_to_arrow_data_type(item_type),
        true,
    ));

    Ok(Arc::new(ListArray::new(field, offsets, child_array, nulls)))
}

fn parse_starrocks_type(starrocks_type: &str) -> PyResult<StarRocksType> {
    let normalized = starrocks_type.trim().to_ascii_lowercase();

    if normalized.starts_with("array<") && normalized.ends_with('>') {
        let inner = &normalized["array<".len()..normalized.len() - 1];
        return Ok(StarRocksType::Array(Box::new(parse_starrocks_type(inner)?)));
    }

    let base = normalized
        .split('(')
        .next()
        .unwrap_or(&normalized)
        .trim();

    Ok(match base {
        "bool" | "boolean" => StarRocksType::Boolean,
        "date" => StarRocksType::Date,
        "tinyint" | "smallint" | "int" | "integer" => StarRocksType::Int32,
        "bigint" => StarRocksType::Int64,
        "decimal" | "decimal32" | "decimal64" | "decimal128" => {
            let (precision, scale) = parse_decimal_precision_scale(&normalized).ok_or_else(|| {
                PyRuntimeError::new_err(format!(
                    "failed to parse decimal precision/scale from type '{}'",
                    starrocks_type
                ))
            })?;
            StarRocksType::Decimal128(precision, scale)
        }
        "float" | "double" => StarRocksType::Float64,
        "datetime" => StarRocksType::Datetime,
        "char" | "varchar" | "string" | "text" => StarRocksType::String,
        _ => StarRocksType::Unknown(starrocks_type.to_string()),
    })
}

fn starrocks_type_to_arrow_data_type(starrocks_type: &StarRocksType) -> DataType {
    match starrocks_type {
        StarRocksType::Boolean => DataType::Boolean,
        StarRocksType::Date => DataType::Date32,
        StarRocksType::Int32 => DataType::Int32,
        StarRocksType::Int64 => DataType::Int64,
        StarRocksType::Decimal128(precision, scale) => DataType::Decimal128(*precision, *scale),
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

fn parse_decimal_precision_scale(decimal_type: &str) -> Option<(u8, i8)> {
    let start = decimal_type.find('(')?;
    let end = decimal_type.rfind(')')?;
    let params = &decimal_type[start + 1..end];
    let mut parts = params.split(',').map(|part| part.trim());
    let precision = parts.next()?.parse::<u8>().ok()?;
    let scale = parts.next()?.parse::<i8>().ok()?;
    Some((precision, scale))
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

    if !integer_part.chars().all(|ch| ch.is_ascii_digit())
        || !fractional_part.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(format!("invalid decimal literal '{}'", value));
    }

    let scale = usize::try_from(scale).map_err(|err| err.to_string())?;
    let integer_value = if integer_part.is_empty() {
        0_i128
    } else {
        integer_part.parse::<i128>().map_err(|err| err.to_string())?
    };
    let mut fraction = fractional_part.to_string();
    if fraction.len() > scale {
        fraction.truncate(scale);
    } else {
        fraction.extend(std::iter::repeat_n('0', scale - fraction.len()));
    }
    let fractional_value = if fraction.is_empty() {
        0_i128
    } else {
        fraction.parse::<i128>().map_err(|err| err.to_string())?
    };

    let scaled = integer_value
        .checked_mul(10_i128.pow(scale as u32))
        .and_then(|value| value.checked_add(fractional_value))
        .ok_or_else(|| format!("decimal overflow for '{}'", value))?;

    Ok(if negative { -scaled } else { scaled })
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, TimeUnit};
    use arrow_array::{
        Array, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
        ListArray, StringArray, TimestampMicrosecondArray,
    };
    use serde_json::json;

    use super::{
        build_arrays, build_schema, parse_col_types_from_meta, parse_starrocks_lines,
    };

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

        let (columns, rows) = parse_starrocks_lines(Cursor::new(response)).unwrap();

        assert_eq!(columns.len(), 7);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], json!(1));
        assert_eq!(rows[0][6], json!("alice"));
        assert_eq!(rows[1][2], json!("-0.12"));
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

        let (columns, rows) = parse_starrocks_lines(Cursor::new(response)).unwrap();
        let schema = build_schema(&columns).unwrap();
        let arrays = build_arrays(&columns, &rows).unwrap();

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

        let amount_array = arrays[2].as_any().downcast_ref::<Decimal128Array>().unwrap();
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

        let (columns, rows) = parse_starrocks_lines(Cursor::new(response)).unwrap();
        let schema = build_schema(&columns).unwrap();
        let arrays = build_arrays(&columns, &rows).unwrap();

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
        let sql = "select * from test_types limit 10";

        let (columns, rows) = super::query_starrocks_rows(
            url,
            "default_catalog",
            database,
            username,
            password,
            sql,
        )
        .unwrap();
        let arrays = build_arrays(&columns, &rows).unwrap();

        assert!(!columns.is_empty());
        assert_eq!(arrays.len(), columns.len());
        for (column, array) in columns.iter().zip(arrays.iter()) {
            println!("{:?}: {:?}", column.name, array);
        }
    }
}
