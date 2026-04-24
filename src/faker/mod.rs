mod number;
mod string;
mod complex;
mod timestamp;
mod internet;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::NullBufferBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_array::builder::{ArrayBuilder, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;
use pyo3_arrow::PyRecordBatch;
use serde::{Deserialize, Serialize};

pub use number::*;
pub use string::*;
pub use complex::*;

enum DataBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Boolean(BooleanBuilder),
    TimestampSeconds(TimestampSecondBuilder),
    TimestampMillis(TimestampMillisecondBuilder),
    TimestampMicros(TimestampMicrosecondBuilder),
    TimestampNanos(TimestampNanosecondBuilder),
    List {
        item_type: DataType,
        item_builder: Box<DataBuilder>,
        offsets_builder: Vec<i32>,
        null_buffer_builder: NullBufferBuilder,
    },
    Struct {
        fields: Fields,
        field_builders: Vec<DataBuilder>,
        null_buffer_builder: NullBufferBuilder,
    },
}

impl DataBuilder {
    fn append_default_value(&mut self) {
        match self {
            DataBuilder::Int32(builder) => builder.append_value(0),
            DataBuilder::Int64(builder) => builder.append_value(0),
            DataBuilder::Float32(builder) => builder.append_value(0.0),
            DataBuilder::Float64(builder) => builder.append_value(0.0),
            DataBuilder::String(builder) => builder.append_value(""),
            DataBuilder::Boolean(builder) => builder.append_value(false),
            DataBuilder::TimestampSeconds(builder) => builder.append_value(0),
            DataBuilder::TimestampMillis(builder) => builder.append_value(0),
            DataBuilder::TimestampMicros(builder) => builder.append_value(0),
            DataBuilder::TimestampNanos(builder) => builder.append_value(0),
            DataBuilder::List { item_type, item_builder, offsets_builder, null_buffer_builder, } => {
                offsets_builder.push(item_builder.len() as i32);
                null_buffer_builder.append(true);
            },
            DataBuilder::Struct { fields, field_builders, null_buffer_builder, } => {
                for field_builder in field_builders {
                    field_builder.append_default_value();
                }
                null_buffer_builder.append(true);
            },
        }
    }

    fn append_null(&mut self) {
        match self {
            DataBuilder::Int32(builder) => builder.append_null(),
            DataBuilder::Int64(builder) => builder.append_null(),
            DataBuilder::Float32(builder) => builder.append_null(),
            DataBuilder::Float64(builder) => builder.append_null(),
            DataBuilder::String(builder) => builder.append_null(),
            DataBuilder::Boolean(builder) => builder.append_null(),
            DataBuilder::TimestampSeconds(builder) => builder.append_null(),
            DataBuilder::TimestampMillis(builder) => builder.append_null(),
            DataBuilder::TimestampMicros(builder) => builder.append_null(),
            DataBuilder::TimestampNanos(builder) => builder.append_null(),
            DataBuilder::List { item_type, item_builder, offsets_builder, null_buffer_builder, } => {
                offsets_builder.push(item_builder.len() as i32);
                null_buffer_builder.append(false);
            },
            DataBuilder::Struct { fields, field_builders, null_buffer_builder, } => {
                for field_builder in field_builders {
                    field_builder.append_default_value();
                }
                null_buffer_builder.append(false);
            },
        }
    }

    fn len(&self) -> usize {
        match self {
            DataBuilder::Int32(builder) => builder.len(),
            DataBuilder::Int64(builder) => builder.len(),
            DataBuilder::Float32(builder) => builder.len(),
            DataBuilder::Float64(builder) => builder.len(),
            DataBuilder::String(builder) => builder.len(),
            DataBuilder::Boolean(builder) => builder.len(),
            DataBuilder::TimestampSeconds(builder) => builder.len(),
            DataBuilder::TimestampMillis(builder) => builder.len(),
            DataBuilder::TimestampMicros(builder) => builder.len(),
            DataBuilder::TimestampNanos(builder) => builder.len(),
            DataBuilder::List { item_type, item_builder, offsets_builder, null_buffer_builder, } => null_buffer_builder.len(),
            DataBuilder::Struct { fields, field_builders, null_buffer_builder, } => null_buffer_builder.len(),
        }
    }
}

fn make_data_builder(data_type: &DataType, capacity: usize) -> Result<DataBuilder> {
    match data_type {
        DataType::Int32 => Ok(DataBuilder::Int32(Int32Builder::with_capacity(capacity))),
        DataType::Int64 => Ok(DataBuilder::Int64(Int64Builder::with_capacity(capacity))),
        DataType::Float32 => Ok(DataBuilder::Float32(Float32Builder::with_capacity(capacity))),
        DataType::Float64 => Ok(DataBuilder::Float64(Float64Builder::with_capacity(capacity))),
        DataType::Utf8 => Ok(DataBuilder::String(StringBuilder::with_capacity(capacity, capacity))),
        DataType::Boolean => Ok(DataBuilder::Boolean(BooleanBuilder::with_capacity(capacity))),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => Ok(DataBuilder::TimestampSeconds(TimestampSecondBuilder::with_capacity(capacity))),
            TimeUnit::Millisecond => Ok(DataBuilder::TimestampMillis(TimestampMillisecondBuilder::with_capacity(capacity))),
            TimeUnit::Microsecond => Ok(DataBuilder::TimestampMicros(TimestampMicrosecondBuilder::with_capacity(capacity))),
            TimeUnit::Nanosecond => Ok(DataBuilder::TimestampNanos(TimestampNanosecondBuilder::with_capacity(capacity))),
        },
        DataType::List(f) => {
            let item_type = f.data_type().clone();
            let item_builder = Box::new(make_data_builder(f.data_type(), capacity)?);
            let mut offsets_builder = Vec::with_capacity(capacity + 1);
            let null_buffer_builder = NullBufferBuilder::new(capacity);
            offsets_builder.push(0);
            Ok(DataBuilder::List {item_type, item_builder, offsets_builder, null_buffer_builder, })
        },
        DataType::Struct(fields) => {
            let fields = fields.clone();
            let field_builders  = fields.iter().map(|f| make_data_builder(f.data_type(), capacity)).collect::<Result<Vec<_>>>()?;
            let null_buffer_builder = NullBufferBuilder::new(capacity);
            Ok(DataBuilder::Struct {fields, field_builders, null_buffer_builder})
        },
        _ => Err(anyhow::anyhow!("not support data type: {:?}", data_type)),
    }
}

#[inline]
fn builder_int32_append_value(builder: &mut DataBuilder, value: i32) {
    if let DataBuilder::Int32(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
}

#[inline]
fn builder_int64_append_value(builder: &mut DataBuilder, value: i64) {
    if let DataBuilder::Int64(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
}

#[inline]
fn builder_float32_append_value(builder: &mut DataBuilder, value: f32) {
    if let DataBuilder::Float32(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
}

#[inline]
fn builder_float64_append_value(builder: &mut DataBuilder, value: f64) {
    if let DataBuilder::Float64(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
}

 #[inline]
 fn builder_string_append_value(builder: &mut DataBuilder, value: &str) {
     if let DataBuilder::String(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
 }

 #[inline]
 fn builder_boolean_append_value(builder: &mut DataBuilder, value: bool) {
     if let DataBuilder::Boolean(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
 }

 #[inline]
 fn builder_timestamp_seconds_append_value(builder: &mut DataBuilder, value: i64) {
     if let DataBuilder::TimestampSeconds(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
 }

 #[inline]
 fn builder_timestamp_millis_append_value(builder: &mut DataBuilder, value: i64) {
     if let DataBuilder::TimestampMillis(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
 }

 #[inline]
 fn builder_timestamp_micros_append_value(builder: &mut DataBuilder, value: i64) {
     if let DataBuilder::TimestampMicros(builder) = builder {
        builder.append_value(value);
    } else {
        unreachable!()
    }
 }


pub trait Faker: Debug {
    fn data_type(&self) -> DataType;

    fn init(&mut self, capacity: usize) -> Result<()> {
        Ok(())
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> Result<()>;

    fn gene_null(&mut self) -> Result<()> {
        Ok(())
    }

    fn len(&self) -> usize {
        0
    }

    fn finish(&mut self) -> Result<ArrayRef> {
        Err(anyhow::anyhow!("not support finish"))
    }

    fn should_flatten_fields(&self) -> bool {
        false
    }

    fn is_compute_faker(&self) -> bool {
        false
    }

    fn compute(&self, fields: & HashMap<&str, ArrayRef>) -> Result<ArrayRef> {
        Err(anyhow::anyhow!("not support compute"))
    }
}

#[derive(Debug, Serialize,Deserialize)]
pub struct FieldFakerConfig {
    pub name: String,
    #[serde(flatten)]
    pub config: Box<dyn FakerConfig>,
}

#[typetag::serde(tag = "type")]
pub trait FakerConfig: Debug + Send + Sync {
    fn build(&self) -> Result<Box<dyn Faker>>;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct WrapConfig {
    #[serde(default)]
    null_rate: f32,
    #[serde(default)]
    array: bool,
    #[serde(default = "default_array_len_min")]
    array_len_min: i32,
    #[serde(default = "default_array_len_max")]
    array_len_max: i32,
    #[serde(default)]
    array_item_null_rate: f32,
}

fn wrap_faker_necessary(mut faker: Box<dyn Faker>, wrap_config: &WrapConfig,) -> Box<dyn Faker> {
    if wrap_config.array {
        if wrap_config.array_item_null_rate > 0f32 {
            faker = Box::new(NullAbleFaker::new(faker, wrap_config.array_item_null_rate))
        }
        faker = Box::new(ArrayFaker::new(faker, wrap_config.array_len_min, wrap_config.array_len_max))
    }
    if wrap_config.null_rate > 0f32 {
        faker = Box::new(NullAbleFaker::new(faker, wrap_config.null_rate))
    }
    faker
}

fn default_array_len_min() -> i32 {
    0
}

fn default_array_len_max() -> i32 {
    5
}

/// Build each column faker from `configs`, fill `num_rows` with `gene_value` per row (same row
/// index across columns), then build one [`RecordBatch`].
///
/// JSON shape per field (when deserialized into [`FieldFakerConfig`]): `{ "name": "...", "type":
/// "int"|"bigint"|"float"|"double", ... }` with `type` and other keys flattened at the top level
/// of each object.
pub fn fake_record_batch_from_field_configs(
    configs: &[FieldFakerConfig],
    num_rows: usize,
) -> Result<RecordBatch> {
    let names: Vec<String> = configs.iter().map(|c| c.name.clone()).collect();
    let mut fakers: Vec<Box<dyn Faker>> = configs
        .iter()
        .map(|c| c.config.build())
        .collect::<Result<Vec<_>>>()?;

    for f in &mut fakers {
        f.init(num_rows)?;
    }
    for _ in 0..num_rows {
        for f in &mut fakers {
            f.gene_value()?;
        }
    }

    let arrays: Vec<ArrayRef> = fakers.iter_mut().map(|f| f.finish()).collect::<Result<_>>()?;

    let fields: Vec<Field> = names
        .into_iter()
        .zip(arrays.iter())
        .map(|(name, arr)| Field::new(name, arr.data_type().clone(), true))
        .collect();

    let schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(schema, arrays)?)
}

/// Build a PyArrow ``RecordBatch`` from a JSON array of field faker configs.
///
/// Args:
///     field_configs_json: JSON string: a list of objects, each with ``name``, ``type``
///         (``int``, ``bigint``, ``float``, ``double``), and type-specific fields such as
///         ``min``/``max``/``random`` or ``options``.
///     num_rows: Number of rows to generate.
///
/// Returns:
///     ``pyarrow.RecordBatch``
///
/// Raises:
///     RuntimeError: If JSON is invalid or batch construction fails.
///
/// Note:
///     Heavy work runs under ``Python::detach`` (GIL released). A plain ``&str`` argument is
///     allowed inside that closure (see PyO3 guide *Parallelism*); ``Bound`` / ``Py`` handles to
///     Python objects must not be used there without ``Py``-style ownership. Other modules often
///     clone ``&str`` to ``String`` when the same value is reused many times in the closure, not
///     because ``&str`` alone is forbidden.
#[pyfunction]
#[pyo3(signature = (field_configs_json, num_rows))]
pub fn fake_arrow_record_batch_from_json<'py>(
    py: Python<'py>,
    field_configs_json: &str,
    num_rows: usize,
) -> PyResult<Bound<'py, PyAny>> {
    let batch = py
        .detach(|| {
            let configs: Vec<FieldFakerConfig> = serde_json::from_str(field_configs_json)
                .map_err(|e| format!("invalid field faker config JSON: {e}"))?;
            fake_record_batch_from_field_configs(&configs, num_rows)
                .map_err(|e| format!("failed to build RecordBatch: {e}"))
        })
        .map_err(PyRuntimeError::new_err)?;

    PyRecordBatch::new(batch)
        .into_pyarrow(py)
        .map_err(|e| {
            PyRuntimeError::new_err(format!("failed to convert to pyarrow RecordBatch: {e}"))
        })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(fake_arrow_record_batch_from_json, module)?)?;
    Ok(())
}

#[cfg(test)]
mod record_batch_from_json_tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;

    use super::*;

    #[test]
    fn deserialize_build_batch_from_json() {
        let json = r#"[
            {"name": "id", "type": "int", "min": 0, "max": 3, "random": false},
            {"name": "score", "type": "float", "min": 0.0, "max": 1.0},
            {"name": "big", "type": "bigint", "min": 10, "max": 13, "random": false}
        ]"#;

        let configs: Vec<FieldFakerConfig> = serde_json::from_str(json).expect("json");
        let batch = fake_record_batch_from_field_configs(&configs, 7).expect("batch");

        assert_eq!(batch.num_rows(), 7);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "id");
        assert_eq!(batch.schema().field(1).name(), "score");
        assert_eq!(batch.schema().field(2).name(), "big");

        let id = batch.column(0).as_primitive::<Int32Type>();
        assert_eq!(id.values(), &[0, 1, 2, 0, 1, 2, 0]);

        let scores = batch.column(1).as_primitive::<arrow_array::types::Float32Type>();
        assert!(scores.values().iter().all(|&v| (0.0..1.0).contains(&v)));

        let big = batch.column(2).as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(big.values(), &[10, 11, 12, 10, 11, 12, 10]);
    }

    #[test]
    fn json_int_options_mode() {
        let json = r#"[
            {"name": "tag", "type": "int", "options": [10, 20], "random": false, "min": 0, "max": 1}
        ]"#;
        let configs: Vec<FieldFakerConfig> = serde_json::from_str(json).unwrap();
        let batch = fake_record_batch_from_field_configs(&configs, 4).unwrap();
        let col = batch.column(0).as_primitive::<Int32Type>();
        assert_eq!(col.values(), &[10, 20, 10, 20]);
    }
}
