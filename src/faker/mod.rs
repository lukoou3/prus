mod number;
mod string;
mod complex;

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;
use pyo3_arrow::PyRecordBatch;
use serde::{Deserialize, Serialize};

pub use number::*;
pub use string::*;
pub use complex::*;

pub trait Faker: Debug {
    fn data_type(&self) -> DataType;

    fn init(&mut self, capacity: usize) -> Result<()>;

    fn gene_value(&mut self) -> Result<()>;

    fn gene_null(&mut self) -> Result<()>;

    fn len(&self) -> usize;

    fn finish(&mut self) -> Result<ArrayRef>;
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
