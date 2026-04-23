use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder};
use arrow_schema::DataType;
use rand::Rng;
use serde::{Deserialize, Serialize};
use super::{wrap_faker_necessary, Faker, FakerConfig, WrapConfig};

#[derive(Debug, Serialize, Deserialize)]
struct IntFakerConfig {
    #[serde(default)]
    min: i32,
    #[serde(default)]
    max: i32,
    #[serde(default)]
    options: Vec<Option<i32>>,
    #[serde(default = "default_random")]
    random: bool,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "int")]
impl FakerConfig for IntFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let faker = Box::new(OptionIntFaker::new(self.options.clone(), self.random)?);
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        } else {
            let faker = Box::new(RangeIntFaker::new(self.min, self.max, self.random)?);
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BigintFakerConfig {
    #[serde(default)]
    min: i64,
    #[serde(default)]
    max: i64,
    #[serde(default)]
    options: Vec<Option<i64>>,
    #[serde(default = "default_random")]
    random: bool,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "bigint")]
impl FakerConfig for BigintFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let faker = Box::new(OptionBigintFaker::new(self.options.clone(), self.random)?);
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        } else {
            let faker = Box::new(RangeBigintFaker::new(self.min, self.max, self.random)?);
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct FloatFakerConfig {
    #[serde(default)]
    min: f32,
    #[serde(default)]
    max: f32,
    #[serde(default)]
    options: Vec<Option<f32>>,
    /// Used only when `options` is non-empty (`OptionFloatFaker`). Range mode is always uniform random in `[min, max)`.
    #[serde(default = "default_random")]
    random: bool,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "float")]
impl FakerConfig for FloatFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let faker = Box::new(OptionFloatFaker::new(self.options.clone(), self.random)?);
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        } else {
            let faker = Box::new(RangeFloatFaker::new(self.min, self.max)?);
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DoubleFakerConfig {
    #[serde(default)]
    min: f64,
    #[serde(default)]
    max: f64,
    #[serde(default)]
    options: Vec<Option<f64>>,
    /// Used only when `options` is non-empty (`OptionDoubleFaker`). Range mode is always uniform random in `[min, max)`.
    #[serde(default = "default_random")]
    random: bool,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "double")]
impl FakerConfig for DoubleFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        if !self.options.is_empty() {
            let faker = Box::new(OptionDoubleFaker::new(self.options.clone(), self.random)?);
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        } else {
            let faker = Box::new(RangeDoubleFaker::new(self.min, self.max)?);
            Ok(wrap_faker_necessary(faker, &self.wrap_config))
        }
    }
}

fn default_random() -> bool {
    true
}

/// Picks from a fixed list of optional values (null = explicit `None` in the list).
macro_rules! impl_option_faker {
    ($name:ident, $native:ty, $data_ty:expr, $builder:ty) => {
        #[derive(Debug)]
        pub struct $name {
            options: Vec<Option<$native>>,
            random: bool,
            index: usize,
            builder: $builder,
        }

        impl $name {
            pub fn new(options: Vec<Option<$native>>, random: bool) -> anyhow::Result<Self> {
                if options.is_empty() {
                    return Err(anyhow::anyhow!(concat!(
                        stringify!($name),
                        ": options must not be empty"
                    )));
                }
                Ok(Self {
                    options,
                    random,
                    index: 0,
                    builder: <$builder>::with_capacity(0),
                })
            }
        }

        impl Faker for $name {
            fn data_type(&self) -> DataType {
                $data_ty
            }

            fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
                self.index = 0;
                self.builder = <$builder>::with_capacity(capacity);
                Ok(())
            }

            fn gene_value(&mut self) -> anyhow::Result<()> {
                let p = if !self.random {
                    if self.index == self.options.len() {
                        self.index = 0;
                    }
                    let value = self.options[self.index];
                    self.index += 1;
                    value
                } else {
                    self.options[rand::rng().random_range(0..self.options.len())]
                };
                self.builder.append_option(p);
                Ok(())
            }

            fn gene_null(&mut self) -> anyhow::Result<()> {
                self.builder.append_null();
                Ok(())
            }

            fn len(&self) -> usize {
                self.builder.len()
            }

            fn finish(&mut self) -> anyhow::Result<ArrayRef> {
                Ok(Arc::new(self.builder.finish()))
            }
        }
    };
}

/// Half-open integer range `[start, end)`: random uniform, or sequential cycle through every integer.
macro_rules! impl_range_integer_faker {
    ($name:ident, $native:ty, $data_ty:expr, $builder:ty) => {
        #[derive(Debug)]
        pub struct $name {
            start: $native,
            end: $native,
            random: bool,
            one_value: bool,
            value: $native,
            builder: $builder,
        }

        impl $name {
            pub fn new(start: $native, end: $native, random: bool) -> anyhow::Result<Self> {
                if start >= end {
                    return Err(anyhow::anyhow!(
                        "{}: start must be less than end (start={:?}, end={:?})",
                        stringify!($name),
                        start,
                        end
                    ));
                }
                Ok(Self {
                    start,
                    end,
                    random,
                    one_value: start + 1 == end,
                    value: start,
                    builder: <$builder>::with_capacity(0),
                })
            }
        }

        impl Faker for $name {
            fn data_type(&self) -> DataType {
                $data_ty
            }

            fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
                self.value = self.start;
                self.builder = <$builder>::with_capacity(capacity);
                Ok(())
            }

            fn gene_value(&mut self) -> anyhow::Result<()> {
                let v = if self.one_value {
                    self.start
                } else if self.random {
                    rand::rng().random_range(self.start..self.end)
                } else {
                    if self.value == self.end {
                        self.value = self.start;
                    }
                    let v = self.value;
                    self.value += 1;
                    v
                };
                self.builder.append_value(v);
                Ok(())
            }

            fn gene_null(&mut self) -> anyhow::Result<()> {
                self.builder.append_null();
                Ok(())
            }

            fn len(&self) -> usize {
                self.builder.len()
            }

            fn finish(&mut self) -> anyhow::Result<ArrayRef> {
                Ok(Arc::new(self.builder.finish()))
            }
        }
    };
}

/// Half-open float range `[start, end)`: each `gene_value` draws uniformly from `[start, end)` (no extra flag).
macro_rules! impl_range_float_faker {
    ($name:ident, $native:ty, $data_ty:expr, $builder:ty) => {
        #[derive(Debug)]
        pub struct $name {
            start: $native,
            end: $native,
            builder: $builder,
        }

        impl $name {
            pub fn new(start: $native, end: $native) -> anyhow::Result<Self> {
                if !(start < end) {
                    return Err(anyhow::anyhow!(concat!(
                        stringify!($name),
                        ": start must be less than end"
                    )));
                }
                Ok(Self {
                    start,
                    end,
                    builder: <$builder>::with_capacity(0),
                })
            }
        }

        impl Faker for $name {
            fn data_type(&self) -> DataType {
                $data_ty
            }

            fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
                self.builder = <$builder>::with_capacity(capacity);
                Ok(())
            }

            fn gene_value(&mut self) -> anyhow::Result<()> {
                let v = rand::rng().random_range(self.start..self.end);
                self.builder.append_value(v);
                Ok(())
            }

            fn gene_null(&mut self) -> anyhow::Result<()> {
                self.builder.append_null();
                Ok(())
            }

            fn len(&self) -> usize {
                self.builder.len()
            }

            fn finish(&mut self) -> anyhow::Result<ArrayRef> {
                Ok(Arc::new(self.builder.finish()))
            }
        }
    };
}

impl_option_faker!(OptionIntFaker, i32, DataType::Int32, Int32Builder);
impl_option_faker!(OptionBigintFaker, i64, DataType::Int64, Int64Builder);
impl_option_faker!(OptionFloatFaker, f32, DataType::Float32, Float32Builder);
impl_option_faker!(OptionDoubleFaker, f64, DataType::Float64, Float64Builder);

impl_range_integer_faker!(RangeIntFaker, i32, DataType::Int32, Int32Builder);
impl_range_integer_faker!(RangeBigintFaker, i64, DataType::Int64, Int64Builder);

impl_range_float_faker!(RangeFloatFaker, f32, DataType::Float32, Float32Builder);
impl_range_float_faker!(RangeDoubleFaker, f64, DataType::Float64, Float64Builder);

mod test {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float32Type, Float64Type, Int32Type, Int64Type};

    use super::*;

    #[test]
    fn test_option_int_faker() {
        let mut faker = OptionIntFaker::new(vec![Some(1), Some(2), Some(3), None], true).unwrap();
        faker.init(10).unwrap();
        for _ in 0..10 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        println!("{}", array.len());
        println!("{:?}", array);
    }

    #[test]
    fn range_int_sequential_wraps() {
        let mut faker = RangeIntFaker::new(0, 3, false).unwrap();
        faker.init(7).unwrap();
        for _ in 0..7 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Int32Type>();
        assert_eq!(arr.values(), &[0, 1, 2, 0, 1, 2, 0]);
    }

    #[test]
    fn range_int_one_value() {
        let mut faker = RangeIntFaker::new(5, 6, false).unwrap();
        faker.init(4).unwrap();
        for _ in 0..4 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Int32Type>();
        assert!(arr.values().iter().all(|&v| v == 5));
    }

    #[test]
    fn range_int_random_stays_in_range() {
        let mut faker = RangeIntFaker::new(10, 20, true).unwrap();
        faker.init(200).unwrap();
        for _ in 0..200 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Int32Type>();
        assert!(arr.values().iter().all(|&v| (10..20).contains(&v)));
    }

    #[test]
    fn range_bigint_sequential() {
        let mut faker = RangeBigintFaker::new(100_i64, 103_i64, false).unwrap();
        faker.init(7).unwrap();
        for _ in 0..7 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Int64Type>();
        assert_eq!(arr.values(), &[100, 101, 102, 100, 101, 102, 100]);
    }

    #[test]
    fn option_bigint_empty_err() {
        assert!(OptionBigintFaker::new(vec![], false).is_err());
    }

    #[test]
    fn option_float_sequential() {
        let mut faker = OptionFloatFaker::new(vec![Some(1.0), Some(2.0)], false).unwrap();
        faker.init(4).unwrap();
        for _ in 0..4 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Float32Type>();
        assert_eq!(arr.values(), &[1.0, 2.0, 1.0, 2.0]);
    }

    #[test]
    fn option_double_random_picks() {
        let mut faker = OptionDoubleFaker::new(vec![Some(0.25), Some(0.75)], true).unwrap();
        faker.init(20).unwrap();
        for _ in 0..20 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Float64Type>();
        assert!(arr.values().iter().all(|&v| v == 0.25 || v == 0.75));
    }

    #[test]
    fn range_float_random_stays_in_range() {
        let mut faker = RangeFloatFaker::new(0.0_f32, 1.0_f32).unwrap();
        faker.init(500).unwrap();
        for _ in 0..500 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Float32Type>();
        assert!(arr.values().iter().all(|&v| v >= 0.0 && v < 1.0));
    }

    #[test]
    fn range_double_random_stays_in_range() {
        let mut faker = RangeDoubleFaker::new(0.0_f64, 4.0_f64).unwrap();
        faker.init(300).unwrap();
        for _ in 0..300 {
            faker.gene_value().unwrap();
        }
        let array = faker.finish().unwrap();
        let arr = array.as_primitive::<Float64Type>();
        assert!(arr.values().iter().all(|&v| (0.0..4.0).contains(&v)));
    }
}
