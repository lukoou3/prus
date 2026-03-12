use pyo3::prelude::*;
use pyo3::types::PyModule;
mod json_writer;
mod starrocks_reader;
mod starrocks_writer;

#[pymodule]
fn prus(module: &Bound<'_, PyModule>) -> PyResult<()> {
    json_writer::register(module)?;
    starrocks_reader::register(module)?;
    starrocks_writer::register(module)?;
    Ok(())
}
