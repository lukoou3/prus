use arrow_schema::DataType;
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::faker::{
    builder_date32_append_value, builder_int32_append_value, wrap_faker_necessary, DataBuilder,
    Faker, FakerConfig, WrapConfig,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DateFakerConfig {
    #[serde(default)]
    date_type: DateKind,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "date")]
impl FakerConfig for DateFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let faker = Box::new(DateFaker::new(self.date_type));
        Ok(wrap_faker_necessary(faker, &self.wrap_config))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DateSequenceFakerConfig {
    #[serde(default)]
    date_type: DateKind,
    #[serde(default)]
    start: String,
    #[serde(default = "default_date_sequence_step")]
    step: i32,
    #[serde(default = "default_date_sequence_batch")]
    batch: u32,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "date_sequence")]
impl FakerConfig for DateSequenceFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let start_d = parse_start_date(self.start.as_str());
        let start = days_since_epoch(start_d);
        let faker = Box::new(DateSequenceFaker::new(self.date_type, start, self.step, self.batch));
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Copy, Deserialize, Serialize)]
pub enum DateKind {
    /// Int32: days since Unix epoch (same numeric domain as Arrow Date32).
    #[serde(rename = "number")]
    Number,
    #[serde(rename = "date")]
    Date,
}

impl Default for DateKind {
    fn default() -> Self {
        DateKind::Date
    }
}

fn default_date_sequence_step() -> i32 {
    1
}

fn default_date_sequence_batch() -> u32 {
    1
}

fn epoch_date() -> NaiveDate {
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch date")
}

fn days_since_epoch(d: NaiveDate) -> i32 {
    (d - epoch_date()).num_days() as i32
}

fn parse_start_date(s: &str) -> NaiveDate {
    if s.is_empty() {
        return Utc::now().date_naive();
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap_or_else(|_| Utc::now().date_naive())
}

#[derive(Debug)]
pub struct DateFaker {
    date_type: DateKind,
}

impl DateFaker {
    pub fn new(date_type: DateKind) -> Self {
        DateFaker { date_type }
    }
}

impl Faker for DateFaker {
    fn data_type(&self) -> DataType {
        match self.date_type {
            DateKind::Number => DataType::Int32,
            DateKind::Date => DataType::Date32,
        }
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        let v = days_since_epoch(Utc::now().date_naive());
        match self.date_type {
            DateKind::Number => builder_int32_append_value(builder, v),
            DateKind::Date => builder_date32_append_value(builder, v),
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct DateSequenceFaker {
    date_type: DateKind,
    start: i32,
    step: i32,
    batch: u32,
    cnt: u32,
    value: i32,
}

impl DateSequenceFaker {
    pub fn new(date_type: DateKind, start: i32, step: i32, batch: u32) -> Self {
        DateSequenceFaker {
            date_type,
            start,
            step,
            batch,
            cnt: 0,
            value: start,
        }
    }
}

impl Faker for DateSequenceFaker {
    fn data_type(&self) -> DataType {
        match self.date_type {
            DateKind::Number => DataType::Int32,
            DateKind::Date => DataType::Date32,
        }
    }

    fn init(&mut self) -> anyhow::Result<()> {
        self.cnt = 0;
        self.value = self.start;
        Ok(())
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        if self.cnt >= self.batch {
            self.value += self.step;
            self.cnt = 0;
        }
        self.cnt += 1;
        let v = self.value.clamp(i32::MIN, i32::MAX) ;
        match self.date_type {
            DateKind::Number => builder_int32_append_value(builder, v),
            DateKind::Date => builder_date32_append_value(builder, v),
        }
        Ok(())
    }
}

mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Date32Type, Int32Type};

    use super::*;

    #[test]
    fn test_date_faker_number() {
        let mut faker = DateFaker::new(DateKind::Number);
        let mut builder = faker.data_builder(10).unwrap();
        for _ in 0..10 {
            faker.gene_value(&mut builder).unwrap();
        }
        let array = builder.finish().unwrap();
        assert_eq!(array.data_type(), &DataType::Int32);
        let _ = array.as_primitive::<Int32Type>();
    }

    #[test]
    fn test_date_faker_date32() {
        let mut faker = DateFaker::new(DateKind::Date);
        let mut builder = faker.data_builder(10).unwrap();
        for _ in 0..10 {
            faker.gene_value(&mut builder).unwrap();
        }
        let array = builder.finish().unwrap();
        assert_eq!(array.data_type(), &DataType::Date32);
        let _ = array.as_primitive::<Date32Type>();
    }

    #[test]
    fn test_date_sequence_faker() {
        let mut faker = DateSequenceFaker::new(DateKind::Number, 0, 10, 5);
        let mut builder = faker.data_builder(20).unwrap();
        for _ in 0..20 {
            faker.gene_value(&mut builder).unwrap();
        }
        let array = builder.finish().unwrap();
        let arr = array.as_primitive::<Int32Type>();
        assert_eq!(
            arr.values(),
            &[0, 0, 0, 0, 0, 10, 10, 10, 10, 10, 20, 20, 20, 20, 20, 30, 30, 30, 30, 30]
        );
    }

    #[test]
    fn test_date_sequence_date32() {
        let mut faker = DateSequenceFaker::new(DateKind::Date, 0, 10, 5);
        let mut builder = faker.data_builder(20).unwrap();
        for _ in 0..20 {
            faker.gene_value(&mut builder).unwrap();
        }
        let array = builder.finish().unwrap();
        let arr = array.as_primitive::<Date32Type>();
        assert_eq!(
            arr.values(),
            &[0, 0, 0, 0, 0, 10, 10, 10, 10, 10, 20, 20, 20, 20, 20, 30, 30, 30, 30, 30]
        );
    }
}
