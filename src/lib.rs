use pyo3::prelude::*;
use pyo3::types::PyModule;
mod json_writer;
mod starrocks;

#[pymodule]
fn prus(module: &Bound<'_, PyModule>) -> PyResult<()> {
    json_writer::register(module)?;
    starrocks::register(module)?;
    Ok(())
}
