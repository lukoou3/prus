use pyo3::prelude::*;
use pyo3::types::PyModule;
mod starrocks;

#[pymodule]
fn prus(module: &Bound<'_, PyModule>) -> PyResult<()> {
    starrocks::register(module)?;
    Ok(())
}
