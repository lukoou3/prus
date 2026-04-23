use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{ArrayBuilder, StringBuilder};
use arrow_schema::DataType;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rand_regex::Regex as RandRegex;
use serde::{Deserialize, Serialize};

use super::{wrap_faker_necessary, Faker, FakerConfig, WrapConfig};

/// Picks from a fixed list of optional strings (`None` in the list = explicit null row).
#[derive(Debug)]
pub struct OptionStringFaker {
    options: Vec<Option<String>>,
    random: bool,
    index: usize,
    builder: StringBuilder,
}

impl OptionStringFaker {
    pub fn new(options: Vec<Option<String>>, random: bool) -> anyhow::Result<Self> {
        if options.is_empty() {
            return Err(anyhow::anyhow!("OptionStringFaker: options must not be empty"));
        }
        Ok(Self {
            options,
            random,
            index: 0,
            builder: StringBuilder::with_capacity(0, 0),
        })
    }
}

impl Faker for OptionStringFaker {
    fn data_type(&self) -> DataType {
        DataType::Utf8
    }

    fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
        self.index = 0;
        self.builder = StringBuilder::with_capacity(capacity, capacity);
        Ok(())
    }

    fn gene_value(&mut self) -> anyhow::Result<()> {
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

/// Random UTF-8 strings of fixed length, each codepoint drawn uniformly from `chars`.
#[derive(Debug)]
pub struct CharsStringFaker {
    chars: Box<[char]>,
    len: usize,
    s: String,
    builder: StringBuilder,
}

impl CharsStringFaker {
    pub fn new(chars: Vec<char>, len: usize) -> anyhow::Result<Self> {
        if chars.is_empty() {
            return Err(anyhow::anyhow!(
                "CharsStringFaker: chars must not be empty"
            ));
        }
        Ok(Self {
            chars: chars.into_boxed_slice(),
            len,
            s: String::with_capacity(len),
            builder: StringBuilder::with_capacity(0, 0),
        })
    }
}

impl Faker for CharsStringFaker {
    fn data_type(&self) -> DataType {
        DataType::Utf8
    }

    fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
        self.builder = StringBuilder::with_capacity(capacity, capacity);
        Ok(())
    }

    fn gene_value(&mut self) -> anyhow::Result<()> {
        self.s.clear();
        for _ in 0..self.len {
            let i = rand::rng().random_range(0..self.chars.len());
            self.s.push(self.chars[i]);
        }
        self.builder.append_value(self.s.as_str());
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

/// Strings generated from a `regex-syntax` pattern via `rand_regex` (no anchors / word boundaries).
#[derive(Debug)]
pub struct RegexStringFaker {
    dist: RandRegex,
    rng: StdRng,
    builder: StringBuilder,
}

impl RegexStringFaker {
    pub fn new(pattern: impl Into<String>, max_repeat: u32) -> anyhow::Result<Self> {
        let pattern = pattern.into();
        let dist = compile_rand_regex(&pattern, max_repeat)?;
        Ok(Self {
            dist,
            rng: StdRng::seed_from_u64(42),
            builder: StringBuilder::with_capacity(0, 0),
        })
    }
}

fn compile_rand_regex(pattern: &str, max_repeat: u32) -> anyhow::Result<RandRegex> {
    let mut parser = regex_syntax::ParserBuilder::new()
        .unicode(false)
        .build();
    let hir = parser
        .parse(pattern)
        .map_err(|e| anyhow::anyhow!("regex parse error: {e}"))?;
    RandRegex::with_hir(hir, max_repeat).map_err(|e| anyhow::anyhow!("rand_regex: {e}"))
}

impl Faker for RegexStringFaker {
    fn data_type(&self) -> DataType {
        DataType::Utf8
    }

    fn init(&mut self, capacity: usize) -> anyhow::Result<()> {
        self.builder = StringBuilder::with_capacity(capacity, capacity);
        Ok(())
    }

    fn gene_value(&mut self) -> anyhow::Result<()> {
        let s: String = self.rng.sample(&self.dist);
        self.builder.append_value(s.as_str());
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


#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

    use super::*;

    #[test]
    fn option_string_empty_all_null() {
        let mut f = OptionStringFaker::new(vec![], false).unwrap();
        f.init(3).unwrap();
        for _ in 0..3 {
            f.gene_value().unwrap();
        }
        let arr = f.finish().unwrap();
        assert_eq!(arr.null_count(), 3);
    }

    #[test]
    fn option_string_cycle() {
        let mut f = OptionStringFaker::new(
            vec![Some("a".into()), Some("b".into()), None],
            false,
        ).unwrap();
        f.init(7).unwrap();
        for _ in 0..7 {
            f.gene_value().unwrap();
        }
        let a = f.finish().unwrap();
        let v = a.as_string::<i32>();
        assert_eq!(v.value(0), "a");
        assert_eq!(v.value(1), "b");
        assert!(v.is_null(2));
        assert_eq!(v.value(3), "a");
    }

    #[test]
    fn chars_string_fixed_len() {
        let mut f = CharsStringFaker::new(vec!['x', 'y'], 4).unwrap();
        f.init(10).unwrap();
        for _ in 0..10 {
            f.gene_value().unwrap();
        }
        let arr = f.finish().unwrap();
        let v = arr.as_string::<i32>();
        for i in 0..v.len() {
            let s = v.value(i);
            assert_eq!(s.len(), 4);
            assert!(s.chars().all(|c| c == 'x' || c == 'y'));
        }
    }

    #[test]
    fn regex_string_digits() {
        let mut f = RegexStringFaker::new(r"\d{3}-\d{2}", 32).unwrap();
        f.init(5).unwrap();
        for _ in 0..5 {
            f.gene_value().unwrap();
        }
        let arr = f.finish().unwrap();
        let v = arr.as_string::<i32>();
        for i in 0..5 {
            let s = v.value(i);
            assert_eq!(s.len(), 6);
            assert!(s.as_bytes().iter().all(|&b| b.is_ascii_digit() || b == b'-'));
        }
    }
}
