use pyo3::prelude::*;
use pyo3::types::PyModule;
mod clickhouse_reader;
mod clickhouse_writer;
mod json_writer;
mod kafka_reader;
mod starrocks_reader;
mod starrocks_writer;

#[pymodule]
fn prus(module: &Bound<'_, PyModule>) -> PyResult<()> {
    clickhouse_reader::register(module)?;
    clickhouse_writer::register(module)?;
    json_writer::register(module)?;
    kafka_reader::register(module)?;
    starrocks_reader::register(module)?;
    starrocks_writer::register(module)?;
    Ok(())
}
