use crate::faker::{builder_string_append_value, wrap_faker_necessary, DataBuilder, FakerConfig};
use std::fmt::Write;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;
use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, StringBuilder};
use arrow_schema::DataType;
use rand::Rng;
use serde::{Deserialize, Serialize};
use crate::faker::{Faker, WrapConfig};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ipv4Config {
    #[serde(default = "default_ipv4_start")]
    start: String,
    #[serde(default = "default_ipv4_end")]
    end: String,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "ipv4")]
impl FakerConfig for Ipv4Config {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let start = Ipv4Addr::from_str(self.start.as_str())?;
        let end = Ipv4Addr::from_str(self.end.as_str())?;
        let start = u32::from(start);
        let end = u32::from(end);
        if start >= end {
            return Err(anyhow::anyhow!("start must less than end"));
        }
        let faker = Box::new(Ipv4Faker::new(start, end));
        Ok(wrap_faker_necessary(faker, &self.wrap_config))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ipv6Config {
    #[serde(default = "default_ipv6_start")]
    start: String,
    #[serde(default = "default_ipv6_end")]
    end: String,
    #[serde(flatten, default)]
    wrap_config: WrapConfig,
}

#[typetag::serde(name = "ipv6")]
impl FakerConfig for Ipv6Config {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let start = Ipv6Addr::from_str(self.start.as_str())?;
        let end = Ipv6Addr::from_str(self.end.as_str())?;
        let start = u128::from(start);
        let end = u128::from(end);
        if start >= end {
            return Err(anyhow::anyhow!("start must less than end"));
        }
        let faker = Box::new(Ipv6Faker::new(start, end));
        Ok(wrap_faker_necessary(faker, &self.wrap_config))
    }
}

fn default_ipv6_start() -> String {
    "::".to_string()
}

fn default_ipv6_end() -> String {
    "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".to_string()
}

fn default_ipv4_start() -> String {
    "0.0.0.0".to_string()
}

fn default_ipv4_end() -> String {
    "255.255.255.255".to_string()
}

#[derive(Debug)]
pub struct Ipv4Faker {
    start: u32,
    end: u32,
    buf: String,
}

impl Ipv4Faker {
    pub fn new(start: u32, end: u32) -> Self {
        Self { start, end, buf: String::with_capacity(15), }
    }
}

impl Faker for Ipv4Faker {
    fn data_type(&self) -> DataType {
        DataType::Utf8
    }
    
    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        self.buf.clear();
        let ip = rand::rng().random_range(self.start..=self.end);
        let a = (ip >> 24) & 0xff;
        let b = (ip >> 16) & 0xff;
        let c = (ip >> 8) & 0xff;
        let d = ip & 0xff;
        let _ = write!(&mut self.buf, "{}.{}.{}.{}", a, b, c, d);
        builder_string_append_value(builder, self.buf.as_str());
        Ok(())
    }
}

#[derive(Debug)]
pub struct Ipv6Faker {
    start: u128,
    end: u128,
    buf: String,
}

impl Ipv6Faker {
    pub fn new(start: u128, end: u128) -> Self {
        Self {
            start,
            end,
            buf: String::with_capacity(40),
        }
    }
}

impl Faker for Ipv6Faker {
    fn data_type(&self) -> DataType {
        DataType::Utf8
    }
    
    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        self.buf.clear();

        let ip = rand::rng().random_range(self.start..=self.end);

        let addr = Ipv6Addr::from(ip);
        let _ = write!(self.buf, "{}", addr);
        
        builder_string_append_value(builder, self.buf.as_str());
        Ok(())
    }
    
}
