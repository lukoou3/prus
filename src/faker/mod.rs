use std::fmt::Debug;
use arrow::buffer::NullBuffer;
use arrow_array::ArrayRef;
use arrow_schema::DataType;

pub trait Faker: Debug {
    fn data_types(&self) -> Vec<DataType>;
    fn gene_arrays(&self, count: usize, nulls: Option<NullBuffer>) -> anyhow::Result<Vec<ArrayRef>>;
}