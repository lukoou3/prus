use std::sync::Arc;
use arrow::array::NullBufferBuilder;
use arrow_array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::faker::{wrap_faker_necessary, Faker, FakerConfig, WrapConfig};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TimestampFakerConfig {
    #[serde(default)]
    unit: TimestampUnit,
    #[serde(default)]
    timestamp_type: TimestampType,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "timestamp")]
impl FakerConfig for TimestampFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let faker = Box::new(TimestampFaker::new(self.unit, self.timestamp_type));
        Ok(wrap_faker_necessary(faker, &self.wrap_config))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TimestampSequenceFakerConfig {
    #[serde(default)]
    unit: TimestampUnit,
    #[serde(default)]
    timestamp_type: TimestampType,
    #[serde(default)]
    start: String,
    #[serde(default = "default_timestamp_sequence_step")]
    step: i64,
    #[serde(default = "default_timestamp_sequence_batch")]
    batch: u32,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "timestamp_sequence")]
impl FakerConfig for TimestampSequenceFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let start_dt = match NaiveDateTime::parse_from_str(self.start.as_str(), "%Y-%m-%d %H:%M:%S%.f") {
            Ok(dt) => dt.and_utc(),
            Err(_) => Utc::now(),
        };
        let start = match self.unit {
            TimestampUnit::Seconds => start_dt.timestamp(),
            TimestampUnit::Millis => start_dt.timestamp_millis(),
            TimestampUnit::Micros => start_dt.timestamp_micros(),
        };
        let faker = Box::new(TimestampSequenceFaker::new(self.unit, self.timestamp_type, start, self.step, self.batch));
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}


#[derive(Clone, Debug, Copy, Deserialize, Serialize)]
pub enum TimestampUnit {
    #[serde(rename = "seconds")]
    Seconds,
    #[serde(rename = "millis")]
    Millis,
    #[serde(rename = "micros")]
    Micros,
}

impl Default for TimestampUnit {
    fn default() -> Self {
        TimestampUnit::Seconds
    }
}

#[derive(Clone, Debug, Copy, Deserialize, Serialize)]
pub enum TimestampType {
    #[serde(rename = "number")]
    Number,
    #[serde(rename = "datetime")]
    Datetime,
}

impl Default for TimestampType {
    fn default() -> Self {
        TimestampType::Number
    }
}

fn default_timestamp_sequence_step() -> i64 {
    1
}

fn default_timestamp_sequence_batch() -> u32 {
    1
}

#[derive(Debug)]
pub struct TimestampFaker {
    unit: TimestampUnit,
    timestamp_type: TimestampType,
    values_builder: Vec<i64>,
    null_buffer_builder: NullBufferBuilder,
}

impl TimestampFaker {
    pub fn new(unit: TimestampUnit, timestamp_type: TimestampType) -> Self {
        TimestampFaker {
            unit,
            timestamp_type,
            values_builder: vec![],
            null_buffer_builder: NullBufferBuilder::new(0),
        }
    }
}

impl Faker for TimestampFaker {
    fn data_type(&self) -> DataType {
        match self.timestamp_type {
            TimestampType::Number => DataType::Int64,
            TimestampType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }

    fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
        self.values_builder = Vec::with_capacity(capacity);
        self.null_buffer_builder = NullBufferBuilder::new(capacity);
        Ok(())
    }

    fn gene_value(&mut self) -> anyhow::Result<()> {
        let  v = match self.timestamp_type {
            TimestampType::Number => {
                match self.unit {
                    TimestampUnit::Seconds => Utc::now().timestamp(),
                    TimestampUnit::Millis => Utc::now().timestamp_millis(),
                    TimestampUnit::Micros => Utc::now().timestamp_micros(),
                }
            },
            TimestampType::Datetime => {
                match self.unit {
                    TimestampUnit::Seconds => Utc::now().timestamp() * 1000_000,
                    TimestampUnit::Millis => Utc::now().timestamp_millis() * 1000,
                    TimestampUnit::Micros => Utc::now().timestamp_micros(),
                }
            },
        };
        self.null_buffer_builder.append_non_null();
        self.values_builder.push(v);
        Ok(())
    }

    fn gene_null(&mut self) -> anyhow::Result<()> {
        self.null_buffer_builder.append_null();
        self.values_builder.push(0);
        Ok(())
    }

    fn len(&self) -> usize {
        self.values_builder.len()
    }

    fn finish(&mut self) -> anyhow::Result<ArrayRef> {
        let values = std::mem::take(&mut self.values_builder).into();
        let nulls = self.null_buffer_builder.finish();
        match self.timestamp_type {
            TimestampType::Number => Ok(Arc::new(Int64Array::try_new(values, nulls)?)),
            TimestampType::Datetime => Ok(Arc::new(TimestampMicrosecondArray::try_new(values, nulls)?)),
        }
    }
}

#[derive(Debug)]
pub struct TimestampSequenceFaker {
    pub unit: TimestampUnit,
    pub timestamp_type: TimestampType,
    start: i64,
    step: i64,
    batch: u32,
    cnt: u32,
    value: i64,
    values_builder: Vec<i64>,
    null_buffer_builder: NullBufferBuilder,
}

impl TimestampSequenceFaker {
    pub fn new(unit: TimestampUnit, timestamp_type: TimestampType, start: i64, step: i64, batch: u32) -> Self {
        TimestampSequenceFaker {
            unit,
            timestamp_type,
            start,
            step,
            batch,
            cnt: 0,
            value: start,
            values_builder: vec![],
            null_buffer_builder: NullBufferBuilder::new(0),
        }
    }
}

impl Faker for TimestampSequenceFaker {
    fn data_type(&self) -> DataType {
        match self.timestamp_type {
            TimestampType::Number => DataType::Int64,
            TimestampType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }

    fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
        self.cnt = 0;
        self.value = self.start;
        self.values_builder = Vec::with_capacity(capacity);
        self.null_buffer_builder = NullBufferBuilder::new(capacity);
        Ok(())
    }

    fn gene_value(&mut self) -> anyhow::Result<()> {
        if self.cnt >= self.batch {
            self.value += self.step;
            self.cnt = 0;
        }
        self.cnt += 1;
        let  v = match self.timestamp_type {
            TimestampType::Number => self.value,
            TimestampType::Datetime => match self.unit {
                TimestampUnit::Seconds => self.value * 1000_000,
                TimestampUnit::Millis => self.value * 1000,
                TimestampUnit::Micros => self.value,
            },
        };
        self.null_buffer_builder.append_non_null();
        self.values_builder.push(v);
        Ok(())
    }

    fn gene_null(&mut self) -> anyhow::Result<()> {
        self.null_buffer_builder.append_null();
        self.values_builder.push(0);
        Ok(())
    }

    fn len(&self) -> usize {
        self.values_builder.len()
    }

    fn finish(&mut self) -> anyhow::Result<ArrayRef> {
        let values = std::mem::take(&mut self.values_builder).into();
        let nulls = self.null_buffer_builder.finish();
        match self.timestamp_type {
            TimestampType::Number => Ok(Arc::new(Int64Array::try_new(values, nulls)?)),
            TimestampType::Datetime => Ok(Arc::new(TimestampMicrosecondArray::try_new(values, nulls)?)),
        }
    }

}

mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int64Type, TimestampMicrosecondType};
    use super::*;
    #[test]
    fn test_timestamp_faker() {
        let mut faker = TimestampFaker::new(TimestampUnit::Micros, TimestampType::Number);
        faker.init(100).unwrap();
        for _ in 0..100 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        assert_eq!(array.len(), 100);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.data_type(), &DataType::Int64);
    }

    #[test]
    fn test_timestamp_sequence_faker() {
        let mut faker = TimestampSequenceFaker::new(TimestampUnit::Micros, TimestampType::Number, 0, 10, 5);
        faker.init(20).unwrap();
        for _ in 0..20 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Int64Type>();
        assert_eq!(arr.values(), &[0, 0, 0, 0, 0, 10, 10, 10, 10, 10, 20, 20, 20, 20, 20, 30, 30, 30, 30, 30])
    }

    #[test]
    fn test_timestamp_datetime_faker() {
        let mut faker = TimestampFaker::new(TimestampUnit::Micros, TimestampType::Datetime);
        faker.init(10).unwrap();
        for _ in 0..10 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        assert_eq!(array.len(), 10);
        assert_eq!(array.null_count(), 0);
        assert_eq!(array.data_type(), &DataType::Timestamp(TimeUnit::Microsecond, None));
        let arr = array.as_primitive::<TimestampMicrosecondType>();
        println!("{:?}", arr);
        println!("{:?}", arr.values());
    }

    #[test]
    fn test_timestamp_datetime_sequence_faker() {
        let mut faker = TimestampSequenceFaker::new(TimestampUnit::Micros, TimestampType::Datetime, 0, 10, 5);
        faker.init(20).unwrap();
        for _ in 0..20 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<TimestampMicrosecondType>();
        println!("{:?}", arr);
        println!("{:?}", arr.values());
        assert_eq!(arr.values(), &[0, 0, 0, 0, 0, 10, 10, 10, 10, 10, 20, 20, 20, 20, 20, 30, 30, 30, 30, 30])
    }
}