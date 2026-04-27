use arrow_schema::DataType;
use base64::Engine;
use rand::Rng;
use serde::{Deserialize, Serialize};
use crate::faker::{builder_binary_append_value, builder_string_append_value, wrap_faker_necessary, DataBuilder, Faker, FakerConfig, WrapConfig};
use crate::sketch::hll::Hll;
use crate::sketch::tdigest::TDigest;

#[derive(Debug, Serialize, Deserialize)]
struct BinaryFakerConfig {
    #[serde(default)]
    bytes: Vec<u8>,
    #[serde(default)]
    len: usize,
    #[serde(default)]
    options: Vec<Option<String>>,
    #[serde(default = "default_random")]
    random: bool,
    #[serde(default)]
    out_type: OutType,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "binary")]
impl FakerConfig for BinaryFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let faker: Box<dyn Faker> = if !self.options.is_empty() {
            // 解析options：尝试十六进制、base64或UTF8
            let parsed_options: Vec<Option<Vec<u8>>> = self.options.iter().map(|opt| {
                opt.as_ref().map(|s| {
                    // 首先尝试作为十六进制解析（检查是否全是十六进制字符）
                    let is_hex = s.chars().all(|c| c.is_ascii_hexdigit());
                    if is_hex && s.len() % 2 == 0 {
                        if let Ok(bytes) = hex::decode(s) {
                            return bytes;
                        }
                    }
                    
                    // 然后尝试作为base64解析
                    if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s) {
                        return bytes;
                    }
                    
                    // 最后作为UTF8字符串转为bytes
                    s.as_bytes().to_vec()
                })
            }).collect();
            Box::new(OptionBinaryFaker::new(parsed_options, self.random, self.out_type)?)
        } else if !self.bytes.is_empty() {
            Box::new(BytesBinaryFaker::new(self.bytes.clone(), self.len, self.out_type)?)
        } else {
            Box::new(BytesBinaryFaker::new("abcdefg".as_bytes().to_vec(), self.len, self.out_type)?)
        };
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HllConfig {
    #[serde(default = "default_hll_item_count")]
    item_count: u64,
    #[serde(default = "default_hll_batch_count")]
    batch_count: u32,
    #[serde(default = "default_hll_log2m")]
    log2m: u32,
    #[serde(default = "default_hll_regwidth")]
    regwidth: u32,
    #[serde(default)]
    out_type: OutType,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "hll")]
impl FakerConfig for HllConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let faker: Box<dyn Faker> = Box::new(HllFaker::new(self.item_count, self.batch_count, self.log2m, self.regwidth, self.out_type));
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TDigestConfig {
    #[serde(default = "default_tdigest_max")]
    max: u32,
    #[serde(default = "default_tdigest_batch_count")]
    batch_count: u32,
    #[serde(default = "default_tdigest_compression")]
    compression: u32,
    #[serde(default)]
    out_type: OutType,
    #[serde(flatten, default)]
    array_config: WrapConfig,
}

#[typetag::serde(name = "tdigest")]
impl FakerConfig for TDigestConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let faker: Box<dyn Faker> = Box::new(TDigestFaker::new(self.max, self.batch_count, self.compression, self.out_type));
        Ok(wrap_faker_necessary(faker, &self.array_config))
    }
}

#[derive(Clone, Debug, Copy, Deserialize, Serialize)]
enum OutType {
    #[serde(rename = "binary")]
    Binary,
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "hex")]
    Hex,
}

impl Default for OutType {
    fn default() -> Self {
        OutType::Binary
    }
}

fn default_random() -> bool {
    true
}

fn default_hll_item_count() -> u64 {
    1000000
}

fn default_hll_batch_count() -> u32 {
    1000
}

fn default_hll_log2m() -> u32 {
    12
}

fn default_hll_regwidth() -> u32 {
    6
}

fn default_tdigest_max() -> u32 {
    1000000
}

fn default_tdigest_batch_count() -> u32 {
    1000
}

fn default_tdigest_compression() -> u32 {
    100
}

#[derive(Debug)]
struct OptionBinaryFaker {
    options: Vec<Option<Vec<u8>>>,
    random: bool,
    out_type: OutType,
    index: usize,
}

impl OptionBinaryFaker {
    fn new(options: Vec<Option<Vec<u8>>>, random: bool, out_type: OutType) -> anyhow::Result<Self> {
        if options.is_empty() {
            return Err(anyhow::anyhow!("OptionBinaryFaker: options must not be empty"));
        }
        Ok(Self {
            options,
            random,
            index: 0,
            out_type,
        })
    }
}

impl Faker for OptionBinaryFaker {
    fn data_type(&self) -> DataType {
        match self.out_type {
            OutType::Binary => DataType::Binary,
            OutType::Base64 | OutType::Hex => DataType::Utf8,
        }
    }

    fn init(&mut self) -> anyhow::Result<()> {
        self.index = 0;
        Ok(())
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        let p = if self.options.len() == 1 {
            self.options[0].as_deref()
        } else {
            if !self.random {
                if self.index == self.options.len() {
                    self.index = 0;
                }
                let index = self.index;
                self.index += 1;
                self.options[index].as_deref()
            } else {
                self.options[rand::random_range(0..self.options.len())].as_deref()
            }
        };
        match p {
            Some(v) => match self.out_type {
                OutType::Binary => builder_binary_append_value(builder, v),
                OutType::Base64 => builder_string_append_value(builder, & base64::engine::general_purpose::STANDARD.encode(v)),
                OutType::Hex => builder_string_append_value(builder, & hex::encode(v)),
            }
            None => builder.append_null(),
        }
        Ok(())
    }
}

#[derive(Debug)]
struct BytesBinaryFaker {
    bytes: Vec<u8>,
    len: usize,
    binary: Vec<u8>,
    out_type: OutType,
}

impl BytesBinaryFaker {
    fn new(bytes: Vec<u8>, len: usize, out_type: OutType) -> anyhow::Result<Self> {
        if bytes.is_empty() {
            return Err(anyhow::anyhow!("BytesBinaryFaker: bytes must not be empty"));
        }
        Ok(Self {
            bytes,
            len,
            binary: vec![0; len],
            out_type,
        })
    }
}

impl Faker for BytesBinaryFaker { 
    fn data_type(&self) -> DataType {
        match self.out_type {
            OutType::Binary => DataType::Binary,
            OutType::Base64 | OutType::Hex => DataType::Utf8,
        }
    }
    
    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        self.binary.clear();
        for _ in 0..self.len {
            let i = rand::rng().random_range(0..self.bytes.len());
            self.binary.push(self.bytes[i]);
        }
        match self.out_type {
            OutType::Binary => builder_binary_append_value(builder, &self.binary),
            OutType::Base64 => builder_string_append_value(builder, & base64::engine::general_purpose::STANDARD.encode(&self.binary)),
            OutType::Hex => builder_string_append_value(builder, & hex::encode(&self.binary)),
        }
        Ok(())
    }
}

#[derive(Debug)]
struct HllFaker {
    item_count: u64,
    batch_count: u32,
    log2m: u32,
    regwidth: u32,
    hll: Hll,
    cache_batch_count: u32,
    max_cache_count: u32,
    cache_count: u32,
    out_type: OutType,
}

impl HllFaker {
    fn new(item_count: u64, batch_count: u32, log2m: u32, regwidth: u32, out_type: OutType) -> Self {
        let mut hll = Hll::new(log2m, regwidth);
        let cache_batch_count = batch_count * 8 / 10;
        let max_cache_count = 10000;
        let cache_count = 0;
        let mut rng = rand::rng();
        for _ in 0..cache_batch_count {
            hll.add(&rng.random_range(0..item_count));
        }
        Self {item_count, batch_count, log2m, regwidth, hll, cache_batch_count, max_cache_count, cache_count, out_type}
    }
}

impl Faker for HllFaker {
    fn data_type(&self) -> DataType {
        match self.out_type {
            OutType::Binary => DataType::Binary,
            OutType::Base64 | OutType::Hex => DataType::Utf8,
        }
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        let item_count = self.item_count;
        let mut rng = rand::rng();
        if self.cache_count >= self.max_cache_count {
            self.cache_count = 0;
            self.hll.clear();
            for _ in 0..self.cache_batch_count {
                self.hll.add(&rng.random_range(0..item_count));
            }
        }
        let mut hll = self.hll.clone();
        for _ in self.cache_batch_count..self.batch_count {
            hll.add(&rng.random_range(0..item_count));
        }
        self.cache_count += 1;
        let bytes = hll.to_bytes();
        match self.out_type {
            OutType::Binary => builder_binary_append_value(builder, &bytes),
            OutType::Base64 => builder_string_append_value(builder, & base64::engine::general_purpose::STANDARD.encode(&bytes)),
            OutType::Hex => builder_string_append_value(builder, & hex::encode(&bytes)),
        }
        Ok(())
    }
}

#[derive(Debug)]
struct TDigestFaker {
    max: u32,
    batch_count: u32,
    compression: u32,
    tdigest: TDigest,
    cache_batch_count: u32,
    max_cache_count: u32,
    cache_count: u32,
    out_type: OutType,
}

impl TDigestFaker {
    fn new(max: u32, batch_count: u32, compression: u32, out_type: OutType) -> Self {
        let mut tdigest = TDigest::new(compression as usize);
        let cache_batch_count = batch_count * 9 / 10;
        let max_cache_count = 10000;
        let cache_count = 0;
        let mut rng = rand::rng();
        let values: Vec<f64> = (0..cache_batch_count).map(|_| rng.random_range(0..max) as f64).collect();
        tdigest = tdigest.merge_unsorted_f64(values);
        Self {max, batch_count, compression, tdigest, cache_batch_count, max_cache_count, cache_count, out_type}
    }
}

impl Faker for TDigestFaker {
    fn data_type(&self) -> DataType {
        match self.out_type {
            OutType::Binary => DataType::Binary,
            OutType::Base64 | OutType::Hex => DataType::Utf8,
        }
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        let max = self.max;
        let mut rng = rand::rng();
        if self.cache_count >= self.max_cache_count {
            self.cache_count = 0;
            self.tdigest = TDigest::new(self.compression as usize);
            let values: Vec<f64> = (0..self.cache_batch_count).map(|_| rng.random_range(0..max) as f64).collect();
            self.tdigest.merge_unsorted_f64(values);
        }
        let mut tdigest = self.tdigest.clone();
        let values: Vec<f64> = (self.cache_batch_count..self.batch_count).map(|_| rng.random_range(0..max) as f64).collect();
        tdigest = tdigest.merge_unsorted_f64(values);
        self.cache_count += 1;
        let bytes = tdigest.to_bytes();
        match self.out_type {
            OutType::Binary => builder_binary_append_value(builder, &bytes),
            OutType::Base64 => builder_string_append_value(builder, & base64::engine::general_purpose::STANDARD.encode(&bytes)),
            OutType::Hex => builder_string_append_value(builder, & hex::encode(&bytes)),
        }
        Ok(())
    }
}
