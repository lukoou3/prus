use std::sync::Arc;
use arrow::array::NullBufferBuilder;
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow_array::{ArrayRef, ListArray};
use arrow_schema::{DataType, Field};
use rand::Rng;
use crate::faker::{DataBuilder, Faker};

#[derive(Debug)]
pub struct ArrayFaker {
    ele_faker: Box<dyn Faker>,
    array_len_min: i32,
    array_len_max: i32,
}

impl ArrayFaker {
    pub fn new(ele_faker: Box<dyn Faker>, array_len_min: i32, array_len_max: i32) -> Self {
        ArrayFaker {
            ele_faker,
            array_len_min,
            array_len_max,
        }
    }
}

impl Faker for ArrayFaker {
    fn data_type(&self) -> DataType {
        DataType::new_list(self.ele_faker.data_type(), true)
    }

    fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
        self.ele_faker.init(capacity)?;
        Ok(())
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        let len = rand::rng().random_range(self.array_len_min..=self.array_len_max);
        match builder {
            DataBuilder::List{item_type, item_builder, offsets_builder, null_buffer_builder} => {
                for _ in 0..len {
                    self.ele_faker.gene_value(item_builder)?;
                }
                offsets_builder.push(item_builder.len() as i32);
                null_buffer_builder.append(true);
                Ok(())
            },
            _ => Err(anyhow::anyhow!("unexpected builder type"))
        }
    }
}

/// Wraps a child faker so each `gene_value` becomes a null with probability `null_rate`,
/// otherwise delegates to the child’s `gene_value`. `gene_null` always forwards to the child.
#[derive(Debug)]
pub struct NullAbleFaker {
    ele_faker: Box<dyn Faker>,
    null_rate: f32,
}

impl NullAbleFaker {
    pub fn new(ele_faker: Box<dyn Faker>, null_rate: f32) -> Self {
        Self {
            ele_faker,
            null_rate: if null_rate.is_finite() {
                null_rate.clamp(0.0, 1.0)
            } else {
                0.0
            },
        }
    }
}

impl Faker for NullAbleFaker {
    fn data_type(&self) -> DataType {
        self.ele_faker.data_type()
    }

    fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
        self.ele_faker.init(capacity)
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        if rand::rng().random_range(0.0f32..1.0f32) < self.null_rate {
            builder.append_null();
            Ok(())
        } else {
            self.ele_faker.gene_value(builder)
        }
    }

    fn gene_null(&mut self) -> anyhow::Result<()> {
        self.ele_faker.gene_null()
    }

    fn len(&self) -> usize {
        self.ele_faker.len()
    }

    fn finish(&mut self) -> anyhow::Result<ArrayRef> {
        self.ele_faker.finish()
    }
}

#[derive(Debug)]
pub struct FieldsFaker {
    field_fakers: Vec<(String, Box<dyn Faker>)>,
    weight: u32,
    weight_index: u32,
}

impl FieldsFaker {
    pub fn new(field_fakers: Vec<(String, Box<dyn Faker>)>, weight: u32) -> Self {
        FieldsFaker {
            field_fakers,
            weight,
            weight_index: 0,
        }
    }
}

#[derive(Debug)]
pub struct UnionFaker {
    fields_fakers: Vec<FieldsFaker>,
    random: bool,
    weights: Vec<u32>,
    weight_max: u32,
    index: usize,
}

impl UnionFaker {
    pub fn new(fields_fakers: Vec<FieldsFaker>, random: bool) -> Self {
        let mut weights = Vec::with_capacity(fields_fakers.len());
        for (i, faker) in fields_fakers.iter().enumerate() {
            if i == 0 {
                weights.push(faker.weight);
            } else {
                weights.push(faker.weight + weights[i - 1]);
            }
        }
        let weight_max = weights[weights.len() - 1];
        UnionFaker {
            fields_fakers,
            random,
            weights,
            weight_max,
            index: 0,
        }
    }
}

impl Faker for UnionFaker {
    fn data_type(&self) -> DataType {
        todo!()
    }

    fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
        todo!()
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        todo!()
    }

    fn gene_null(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn finish(&mut self) -> anyhow::Result<ArrayRef> {
        todo!()
    }

    fn should_flatten_fields(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

    use crate::faker::{OptionIntFaker, RangeIntFaker};
    use super::*;

    #[test]
    fn test_array_faker() {
        let mut faker = ArrayFaker::new(Box::new(OptionIntFaker::new(vec![Some(1), Some(2), Some(3), None], true).unwrap()), 1, 5);
        faker.init(10).unwrap();
        for _ in 0..10 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        println!("{}", array.len());
        println!("{:?}", array);
    }

    #[test]
    fn null_able_all_nulls_when_rate_one() {
        let mut faker = NullAbleFaker::new(
            Box::new(RangeIntFaker::new(1, 100, false).unwrap()),
            1.0,
        );
        faker.init(20).unwrap();
        for _ in 0..20 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        assert_eq!(array.len(), 20);
        assert_eq!(array.null_count(), 20);
    }

    #[test]
    fn null_able_no_random_null_when_rate_zero() {
        let mut faker = NullAbleFaker::new(
            Box::new(RangeIntFaker::new(7, 8, false).unwrap()),
            0.0,
        );
        faker.init(5).unwrap();
        for _ in 0..5 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        assert_eq!(array.null_count(), 0);
        assert!(array
            .as_primitive::<arrow_array::types::Int32Type>()
            .values()
            .iter()
            .all(|&v| v == 7));
    }

    #[test]
    fn null_able_gene_null_delegates() {
        let mut faker = NullAbleFaker::new(
            Box::new(RangeIntFaker::new(1, 10, true).unwrap()),
            0.0,
        );
        faker.init(2).unwrap();
        faker.gene_null().unwrap();
        faker.gene_value().unwrap();
        let array = faker.finish().unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.null_count(), 1);
    }
}