use pyo3::prelude::*;
use pyo3::types::PyModule;
mod clickhouse_reader;
mod clickhouse_writer;
mod json_writer;
#[cfg(feature = "kafka")]
mod kafka_reader;
#[cfg(feature = "kafka")]
mod kafka_writer;
#[cfg(feature = "kafka")]
mod kafka_tools;
mod starrocks_reader;
mod starrocks_writer;
mod faker;
pub mod sketch;

#[pymodule]
fn prus(module: &Bound<'_, PyModule>) -> PyResult<()> {
    clickhouse_reader::register(module)?;
    clickhouse_writer::register(module)?;
    json_writer::register(module)?;
    #[cfg(feature = "kafka")]
    kafka_reader::register(module)?;
    #[cfg(feature = "kafka")]
    kafka_writer::register(module)?;
    #[cfg(feature = "kafka")]
    kafka_tools::register(module)?;
    starrocks_reader::register(module)?;
    starrocks_writer::register(module)?;
    faker::register(module)?;
    Ok(())
}
