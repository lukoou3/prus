use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::io::Cursor;
use ahash::AHasher;
use murmur3::murmur3_x64_128;
use anyhow::Result;

const MAXIMUM_EXPLICIT_THRESHOLD: u32 = 1 << (18 - 1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hll {
    log2m: u32,
    regwidth: u32,
    m: u32,
    m_bits_mask: u32,
    value_mask: u32,
    pw_max_mask: u64,
    explicit_threshold: u32,
    sparse_threshold: u32,
    storage: Storage,
}

impl Hll {
    pub fn new(log2m: u32, regwidth: u32) -> Self {
        assert!(log2m >= 8 && log2m <= 20);
        assert!(regwidth >= 4 && regwidth <= 8);
        let m = 1 << log2m;
        let m_bits_mask = m - 1;
        let value_mask = (1 << regwidth) - 1;
        let max_register_value = (1 << regwidth) - 1;
        let pw_max_mask = !((1u64 << (max_register_value - 1)) - 1);
        let explicit_threshold = calculate_explicit_threshold(log2m, regwidth);
        let sparse_threshold = calculate_sparse_threshold(log2m, regwidth);
        Hll {
            log2m,
            regwidth,
            m,
            m_bits_mask,
            value_mask,
            pw_max_mask,
            explicit_threshold,
            sparse_threshold,
            storage: Storage::EMPTY,
        }
    }

    pub fn add<T: Hash>(&mut self, val: &T) {
        let mut hasher = ahash::RandomState::with_seeds(1, 2, 3, 4).build_hasher();
        val.hash(&mut hasher);
        let hash = hasher.finish();
        self.add_raw(hash);
    }

    pub fn add_str(&mut self, val: &str) {
        let hash =  murmur3_x64_128(&mut Cursor::new(val.as_bytes()), 0).unwrap() as u64;
        self.add_raw(hash);
    }

    pub fn add_u32(&mut self, val: u32) {
        let hash =  murmur3_x64_128(&mut Cursor::new(val.to_le_bytes()), 0).unwrap() as u64;
        self.add_raw(hash);
    }

    pub fn add_u64(&mut self, val: u64) {
        let hash =  murmur3_x64_128(&mut Cursor::new(val.to_le_bytes()), 0).unwrap() as u64;
        self.add_raw(hash);
    }

    pub fn add_raw(&mut self, hash: u64) {
        match &mut self.storage {
            Storage::EMPTY => {
                let mut set = HashSet::with_hasher(BuildHasherDefault::<AHasher>::default());
                set.insert(hash);
                self.storage = Storage::EXPLICIT(Explicit {data: set});
            },
            Storage::EXPLICIT(explicit) => {
                explicit.data.insert(hash);
                if explicit.data.len() > self.explicit_threshold as usize {
                    let mut sparse:HashMap<u32, u8, _> = HashMap::with_hasher(BuildHasherDefault::<AHasher>::default());
                    for &hash in &explicit.data {
                        let (index, reg) = Self::compute_index_and_reg(hash, self.log2m, self.pw_max_mask, self.m_bits_mask);
                        if reg == 0 {
                            continue;
                        }
                        sparse.entry(index as u32)
                            .and_modify(|v| *v = (*v).max(reg))
                            .or_insert(reg);
                    }
                    self.storage = Storage::SPARSE(Sparse {data: sparse});
                }
            },
            Storage::SPARSE(sparse) => {
                let (index, reg) = Self::compute_index_and_reg(hash, self.log2m, self.pw_max_mask, self.m_bits_mask);
                if reg == 0 {
                    return;
                }
                let entry = sparse.data.entry(index as u32).or_insert(0);
                *entry = (*entry).max(reg);
                if sparse.data.len() > self.sparse_threshold as usize {
                    let mut full = Full::new(self.m, self.regwidth);
                    for (&index, &reg) in &sparse.data {
                        full.set_reg(index as usize, reg, self.regwidth);
                    }
                    self.storage = Storage::FULL(full);
                }
            },
            Storage::FULL(full) => {
                let (index, reg) = Self::compute_index_and_reg(hash, self.log2m, self.pw_max_mask, self.m_bits_mask);
                if reg == 0 {
                    return;
                }
                full.set_reg(index, reg, self.regwidth);
            },
        }
    }

    pub fn merge(&mut self, other: &Hll) -> Result<()> {
        if self.log2m != other.log2m || self.regwidth != other.regwidth {
            return Err(anyhow::anyhow!("log2m and regwidth must be the same"));
        }

        if let Storage::EMPTY = other.storage {
            return Ok(());
        }

        if let Storage::EMPTY = self.storage {
            self.storage = other.storage.clone();
            return Ok(());
        }

        match &other.storage {
            Storage::EMPTY => (),
            Storage::EXPLICIT(explicit) => match &mut self.storage {
                Storage::FULL(full) => {
                    for &hash in &explicit.data {
                        let (index, reg) = Self::compute_index_and_reg(hash, self.log2m, self.pw_max_mask, self.m_bits_mask);
                        full.set_reg(index, reg, self.regwidth);
                    }
                },
                _ => {
                    for &hash in &explicit.data {
                        self.add_raw(hash);
                    }
                }
            },
            Storage::SPARSE(sparse) => match &mut self.storage {
                Storage::FULL(full) => {
                    for (&index, &reg) in &sparse.data {
                        full.set_reg(index as usize, reg, self.regwidth);
                    }
                },
                _ => {
                    for (&index, &reg) in &sparse.data {
                        let hash = (index as u64) | (1u64 << (reg as u32 + other.log2m - 1));
                        self.add_raw(hash);
                    }
                }
            },
            Storage::FULL(full) => match &mut self.storage {
                Storage::FULL(f) => {
                    f.merge(full, self.m, self.regwidth, self.value_mask);
                },
                _ => {
                    for i in 0..other.m as usize{
                        let reg = full.get_reg(i, other.regwidth);
                        let hash = (i as u64) | (1u64 << (reg as u32 + other.log2m - 1));
                        self.add_raw(hash);
                    }
                }
            },
        }

        Ok(())
    }

    pub fn cardinality(&self) -> f64 {
        match &self.storage {
            Storage::EMPTY => 0.0,
            Storage::EXPLICIT(explicit) => explicit.data.len() as f64,
            Storage::SPARSE(sparse) => self.sparse_cardinality(sparse),
            Storage::FULL(full) => self.full_cardinality(full),
        }
    }

    fn compute_index_and_reg(hash: u64, log2m: u32, pw_max_mask: u64, m_bits_mask: u32) -> (usize, u8) {
        let substream_value = hash >> log2m;
        if substream_value == 0 {
            return (0, 0);
        }
        let p_w = ((substream_value | pw_max_mask).trailing_zeros() + 1) as u8;
        let index = (hash & m_bits_mask as u64) as usize;
        (index, p_w)
    }

    fn sparse_cardinality(&self, sparse: &Sparse) -> f64 {
        let m_f64 = self.m as f64;
        let mut sum = 0.0;
        for (_, &reg) in &sparse.data {
            sum += 1.0 / (1u64 << reg) as f64;
        }
        let number_of_zeroes = self.m - sparse.data.len() as u32;
        sum += number_of_zeroes as f64;

        let estimator = self.alpha_msquared() / sum;
        let small_estimator_cutoff = m_f64 * 5.0 / 2.0;
        if number_of_zeroes != 0 && estimator < small_estimator_cutoff {
            // small_estimator
            m_f64 * (m_f64 / number_of_zeroes as f64).ln()
        } else if estimator <= large_estimator_cutoff(self.log2m, self.regwidth) {
            estimator
        } else {
            large_estimator(self.log2m, self.regwidth, estimator)
        }
    }

    fn full_cardinality(&self, full: &Full) -> f64 {
        let m_f64 = self.m as f64;
        let (sum, number_of_zeroes) = full.indicator(self.m, self.regwidth, self.value_mask);

        let estimator = self.alpha_msquared() / sum;
        let small_estimator_cutoff = m_f64 * 5.0 / 2.0;
        if number_of_zeroes != 0 && estimator < small_estimator_cutoff {
            // small_estimator
            m_f64 * (m_f64 / number_of_zeroes as f64).ln()
        } else if estimator <= large_estimator_cutoff(self.log2m, self.regwidth) {
            estimator
        } else {
            large_estimator(self.log2m, self.regwidth, estimator)
        }
    }

    fn alpha_msquared(&self) -> f64 {
        let m = self.m as f64;
        match self.m {
            16 => 0.673 * m * m,
            32 => 0.697 * m * m,
            64 => 0.709 * m * m,
            _ => (0.7213 / (1.0 + 1.079 / m)) * m * m,
        }
    }

    pub fn clear(&mut self) {
        match &mut self.storage {
            Storage::EMPTY => (),
            Storage::EXPLICIT(explicit) => explicit.data.clear(),
            Storage::SPARSE(sparse) => sparse.data.clear(),
            Storage::FULL(full) => full.data.clear(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match &self.storage {
            Storage::EMPTY => {
                let mut bytes = vec![0; 3];
                self.write_header(&mut bytes, 1);
                bytes
            },
            Storage::EXPLICIT(explicit) => {
                let mut bytes = vec![0; 3 + explicit.data.len() * 8];
                self.write_header(&mut bytes, 2);
                let mut pos = 3;
                for hash in &explicit.data {
                    bytes[pos..pos + 8].copy_from_slice(&hash.to_be_bytes());
                    pos += 8;
                }
                bytes
            },
            Storage::SPARSE(sparse) => {
                let bits = (self.log2m + self.regwidth) as usize * sparse.data.len();
                let mut bytes = vec![0; 3 + (bits + 7) / 8];
                self.write_header(&mut bytes, 3);
                let mut values:Vec<_> = sparse.data.iter().map(|(index, reg)| (*index as u64, *reg as u64)).collect();
                values.sort();
                let mut addr = 3 * 8;
                let bits_per_reg = (self.log2m + self.regwidth) as usize;
                for (index, reg) in values {
                    write_bits(&mut bytes, addr, index << self.regwidth | reg, bits_per_reg);
                    addr += bits_per_reg;
                }
                bytes
            },
            Storage::FULL(full) => {
                let bits = (self.m * self.regwidth) as usize;
                let mut bytes = vec![0; 3 + (bits + 7) / 8];
                self.write_header(&mut bytes, 4);
                full.write_bytes(&mut bytes[3..], self.m, self.regwidth);
                bytes
            },
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 3 {
            return Err(anyhow::anyhow!("InsufficientBytes"));
        }
        let (version, storage_type) = (bytes[0]>>4, bytes[0]&0xf);
        if version != 1 {
            return Err(anyhow::anyhow!("unsupported Hll version:: {}", version));
        }
        let (regwidth, log2m) = (((bytes[1]>>5)+1) as u32, (bytes[1]&0x1f) as u32);
        let mut hll = Hll::new(log2m, regwidth);
        match storage_type {
            1 => {
                hll.storage = Storage::EMPTY;
            },
            2 => {
                let set = HashSet::with_hasher(BuildHasherDefault::<AHasher>::default());
                hll.storage = Storage::EXPLICIT(Explicit {data: set});
                let len = (bytes.len() - 3) / 8;
                let mut pos = 3;
                for i in 0..len {
                    let hash =  u64::from_be_bytes(bytes[pos..pos+8].try_into().unwrap());
                    hll.add_raw(hash);
                    pos += 8;
                }
            },
            3 => {
                let value_mask =  hll.value_mask as u64;
                let word_size = (log2m + regwidth) as usize;
                let len = (bytes.len() - 3) * 8 / word_size;
                let mut addr = 3 * 8;
                if len <= hll.sparse_threshold as usize {
                    let mut sparse = HashMap::with_hasher(BuildHasherDefault::<AHasher>::default());
                    for _ in 0..len {
                        let word = read_bits(&bytes, addr, word_size);
                        addr += word_size;
                        let index = (word >> regwidth) as u32;
                        let reg = (word & value_mask) as u8;
                        if reg != 0 {
                            sparse.insert(index, reg);
                        }
                    }
                    hll.storage = Storage::SPARSE(Sparse {data: sparse});
                } else {
                    let mut full = Full::new(hll.m, hll.regwidth);
                    for _ in 0..len {
                        let word = read_bits(&bytes, addr, word_size);
                        addr += word_size;
                        let index = (word >> regwidth) as usize;
                        let reg = (word & value_mask) as u8;
                        if reg != 0 {
                            full.set_reg(index, reg, regwidth);
                        }
                    }
                    hll.storage = Storage::FULL(full);
                }
            },
            4 => {
                let full = Full::from_bytes(&bytes[3..], hll.m, hll.regwidth)?;
                hll.storage = Storage::FULL(full);
            },
            _ => return Err(anyhow::anyhow!("unsupported storage type:: {}", storage_type))
        }

        Ok(hll)
    }

    fn write_header(&self, bytes: &mut [u8], storage_type: u8) {
        bytes[0] = (1 << 4) | storage_type;
        bytes[1] = (((self.regwidth - 1) << 5)  | self.log2m ) as u8;
        bytes[2] = 127;
    }
}

fn large_estimator(log2m: u32, regwidth: u32, estimator: f64) -> f64 {
    let two_to_l = two_to_l(log2m, regwidth);
    -1.0 * two_to_l * (1.0 - (estimator / two_to_l)).ln()
}

fn large_estimator_cutoff(log2m: u32, regwidth: u32) -> f64 {
    let two_to_l = two_to_l(log2m, regwidth);
    two_to_l / 30.0
}

fn two_to_l(log2m: u32, regwidth: u32) -> f64 {
    let max_register_value = (1 << regwidth) - 1;
    let pw_bits = max_register_value - 1;
    let total_bits = pw_bits + log2m;
    2.0_f64.powf(total_bits as f64)
}

fn calculate_explicit_threshold(log2m: u32, regwidth: u32) -> u32 {
    let m = 1 << log2m;
    let full_representation_size = (regwidth * m + 7) / 8;
    let num_longs = full_representation_size / 8;
    if num_longs > MAXIMUM_EXPLICIT_THRESHOLD {
        MAXIMUM_EXPLICIT_THRESHOLD
    } else {
        num_longs
    }
}

fn calculate_sparse_threshold(log2m: u32, regwidth: u32) -> u32 {
    let m = 1 << log2m;
    let short_word_length = log2m + regwidth;
    let cutoff = (m * regwidth) as f64 / short_word_length as f64;
    let largest_pow2_less_than_cutoff = cutoff.log2() as u32;
    1 << largest_pow2_less_than_cutoff
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Storage {
    EMPTY,
    EXPLICIT(Explicit),
    SPARSE(Sparse),
    FULL(Full),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Explicit {
    data: HashSet<u64, BuildHasherDefault<AHasher>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Sparse {
    data: HashMap<u32, u8, BuildHasherDefault<AHasher>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Full {
    data: Vec<u64>,
}

impl Full {
    fn new(m: u32, regwidth: u32) -> Self {
        // 计算所需字节数
        let bytes = (m * regwidth + 7) / 8;
        // 计算所需 u64 数量
        let words = (bytes + 7) / 8;
        Self {
            data: vec![0u64; words as usize],
        }
    }

    /// 计算reg位置
    /// - regnum：reg索引（0 到 m-1）
    /// - 返回 (idx, pos)：idx 是 Vec<u64> 索引，pos 是 u64 内的位偏移
    fn calc_position(&self, regnum: usize, regwidth: u32) -> (usize, usize) {
        // 计算reg起始位：regnum * regwidth
        let addr = regnum * regwidth as usize;
        // idx = addr / 64（u64 索引）
        let idx = addr >> 6;
        // pos = addr % 64（u64 内的位偏移）
        let pos = addr & 0x3f;
        (idx, pos)
    }

    /// 获取reg值
    /// - 从 Vec<u64> 提取 regwidth 位短字
    /// - 处理单 u64 或跨 u64 边界的情况
    /// - 返回 u8（reg值最大为 2^regwidth - 1）
    fn get_reg(&self, regnum: usize, regwidth: u32) -> u8 {
        let (idx, pos) = self.calc_position(regnum, regwidth);
        let n_bits = regwidth as usize;

        // 情况 1：reg值在单个 u64 内
        if pos + n_bits <= 64 {
            // 计算左移量（将值移到最低位）
            let shift_left = if pos + n_bits < 64 {
                64 - (pos + n_bits)
            } else {
                0
            };
            // 创建掩码：regwidth 位全 1，左移到正确位置
            let mask = ((1u64 << n_bits) - 1) << shift_left;
            // 提取值并右移到最低位
            ((self.data[idx] & mask) >> shift_left) as u8
        } else {
            // 情况 2：reg值跨 u64 边界
            let n_bits_upper = 64 - pos; // 高位 u64 的位数
            let n_bits_lower = n_bits - n_bits_upper; // 低位 u64 的位数
            // 高位掩码：n_bits_upper 位全 1
            let mask_upper = (1u64 << n_bits_upper) - 1;
            // 提取高位并左移，为低位留空间
            let upper = (self.data[idx] & mask_upper) << n_bits_lower;
            // 提取低位并右移到最低位
            let lower = self.data[idx + 1] >> (64 - n_bits_lower);
            // 合并高位和低位
            (upper | lower) as u8
        }
    }

    /// 设置reg值（如果新值更大）
    /// - 仅当 value > 当前值时更新
    /// - 处理单 u64 或跨 u64 边界的情况
    /// - 使用位操作更新 Vec<u64>
    fn set_reg(&mut self, regnum: usize, value: u8, regwidth: u32) {
        let (idx, pos) = self.calc_position(regnum, regwidth);
        let n_bits = regwidth as usize;

        // 情况 1：reg值在单个 u64 内
        if pos + n_bits <= 64 {
            // 计算左移量（将值移到正确位置）
            let shift_left = if pos + n_bits < 64 {
                64 - (pos + n_bits)
            } else {
                0
            };
            // 创建掩码：regwidth 位全 1，左移到正确位置
            let mask = ((1u64 << n_bits) - 1) << shift_left;
            // 提取当前值
            let curr_val = ((self.data[idx] & mask) >> shift_left) as u8;
            // 如果新值更大，更新
            if value > curr_val {
                // 将新值左移到正确位置
                let part_to_write = (value as u64) << shift_left;
                // 清空目标位并写入新值
                self.data[idx] = (!mask & self.data[idx]) | part_to_write;
            }
        } else {
            // 情况 2：reg值跨 u64 边界
            let n_bits_upper = 64 - pos; // 高位 u64 的位数
            let n_bits_lower = n_bits - n_bits_upper; // 低位 u64 的位数
            // 高位掩码：n_bits_upper 位全 1
            let mask_upper = (1u64 << n_bits_upper) - 1;
            // 低位掩码：n_bits_lower 位全 1
            let mask_lower = (1u64 << n_bits_lower) - 1;
            // 提取当前值（高位左移，低位右移，合并）
            let upper = (self.data[idx] & mask_upper) << n_bits_lower;
            let lower = self.data[idx + 1] >> (64 - n_bits_lower);
            let curr_val = (upper | lower) as u8;

            // 如果新值更大，更新两个 u64
            if value > curr_val {
                // 高位部分：value 右移 n_bits_lower 位
                let part_to_write_upper = ((value as u64) >> n_bits_lower) & mask_upper;
                // 低位部分：value 左移到 u64 高位
                let part_to_write_lower = ((value as u64) & mask_lower) << (64 - n_bits_lower);
                // 低位掩码左移到正确位置
                let mask_lower_shifted = mask_lower << (64 - n_bits_lower);
                // 更新高位 u64
                self.data[idx] = (!mask_upper & self.data[idx]) | part_to_write_upper;
                // 更新低位 u64
                self.data[idx + 1] = (!mask_lower_shifted & self.data[idx + 1]) | part_to_write_lower;
            }
        }
    }

    /// 计算基数估计的指标（和与零计数）
    /// - 与 Go 的 denseStorage.indicator 逻辑保持一致
    /// - 遍历所有寄存器，计算 sum = Σ(1 / 2^value) 和零值数量
    /// - 使用位掩码提取 regwidth 位短字，处理单 u64 和跨 u64 边界
    fn indicator(&self, m: u32, regwidth: u32, value_mask: u32) -> (f64, usize) {
        // 寄存器总数：m = 1 << log2m
        let num_reg = m as usize;
        // 当前 u64 索引
        let mut idx = 0;
        // 当前 u64 内的位偏移
        let mut pos = 0;
        // 当前 u64 值
        let mut curr = self.data[idx];
        // 初始掩码：regwidth 位全 1，左移到 u64 高位
        let mut mask = (value_mask as u64) << (64 - regwidth);

        // 指标和：Σ(1 / 2^value)
        let mut sum = 0.0;
        // 零值计数
        let mut number_of_zeros = 0;

        // 遍历所有寄存器
        for _ in 0..num_reg {
            // 当前寄存器值
            let mut value: u64;
            // 当前 u64 剩余位数
            let bits_available = 64 - pos;

            if bits_available >= regwidth as usize {
                // 情况 1：寄存器值在当前 u64 内
                // 提取 regwidth 位值：(curr & mask) >> (64 - pos - regwidth)
                value = (curr & mask) >> (64 - pos - regwidth as usize);
                // 更新位偏移：pos += regwidth
                pos += regwidth as usize;
                // 右移掩码
                mask = mask >> regwidth ;
            } else {
                // 情况 2：寄存器值跨 u64 边界
                // 低位 u64 的位数：regwidth - bits_available
                let n_lower_bits = regwidth as usize - bits_available;
                // 高位值（如果有）
                let upper_bits = if bits_available > 0 {
                    (curr & mask) << n_lower_bits
                } else {
                    0
                };

                // 移动到下一个 u64
                // 等价于 Go 的 idx++; curr = s[idx]
                idx += 1;
                curr = self.data[idx];

                // 提取低位
                let lower_mask = ((1u64 << n_lower_bits) - 1) << (64 - n_lower_bits);
                let lower_bits = (curr & lower_mask) >> (64 - n_lower_bits);
                // 合并高位和低位
                value = upper_bits | lower_bits;

                // 更新 pos 和 mask
                pos = n_lower_bits;
                mask = (value_mask as u64) << (64 - regwidth);
                mask >>= pos as u64;
            }

            // 计算指标：1 / 2^value
            sum += 1.0 / (1u64 << value) as f64;
            // 计数零值
            if value == 0 {
                number_of_zeros += 1;
            }
        }

        // 返回 sum 和 number_of_zeros
        (sum, number_of_zeros)
    }

    /// merge两个 Full 存储
    /// - 比较两个存储的寄存器值，取最大值
    /// - 更新当前存储（self）
    /// - 处理单 u64 和跨 u64 边界的情况
    fn merge(&mut self, other: &Full, m: u32, regwidth: u32, value_mask: u32) {
        let num_reg = m as usize; // 寄存器总数
        let mut idx = 0; // 当前 u64 索引
        let mut pos = 0; // 当前 u64 内的位偏移
        let mut this_word = self.data[idx]; // 当前 u64 值
        let mut other_word = other.data[idx]; // 其他存储的 u64 值
        let mut computed = this_word; // 计算结果
        // 初始掩码：regwidth 位全 1，位于 u64 高位
        let mut mask = (value_mask as u64) << (64 - regwidth);

        // 遍历所有寄存器
        for _ in 0..num_reg {
            let bits_available = 64 - pos;
            if bits_available >= regwidth as usize {
                // 情况 1：寄存器值在当前 u64 内
                // 提取两个存储的值
                let this_value = this_word & mask;
                let other_value = other_word & mask;
                // 如果其他值更大，更新 computed
                // NOTE : no need to shift into position to compare or mix back in.
                if other_value > this_value {
                    computed = (!mask & computed) | other_value;
                }
                // 更新位偏移和掩码
                pos += regwidth as usize;
                mask = mask >> regwidth;
            } else {
                // 情况 2：寄存器值跨 u64 边界
                let mut other_is_greater = false;
                let mut this_is_greater = false;

                // 比较高位（如果有）
                if bits_available > 0 {
                    let this_value = this_word & mask;
                    let other_value = other_word & mask;
                    if other_value > this_value {
                        computed = (!mask & computed) | other_value;
                        other_is_greater = true;
                    } else if other_value < this_value {
                        this_is_greater = true;
                    }
                }

                // 如果高位已更新，写入当前 u64
                if computed != this_word {
                    self.data[idx] = computed;
                }

                // 移动到下一个 u64
                idx += 1;
                this_word = self.data[idx];
                other_word = other.data[idx];
                let n_lower_bits = regwidth as usize - bits_available;
                computed = this_word;

                // 比较低位（如果高位未确定）
                if !this_is_greater {
                    let lower_mask = ((1u64 << n_lower_bits) - 1) << (64 - n_lower_bits);
                    let this_lower_bits = this_word & lower_mask;
                    let other_lower_bits = other_word & lower_mask;
                    // 更新低位（如果其他值更大或高位相等且低位更大）
                    if (other_is_greater && this_lower_bits != other_lower_bits) || other_lower_bits > this_lower_bits {
                        computed = (!lower_mask & computed) | other_lower_bits;
                    }
                }

                // 更新位偏移和掩码
                pos = n_lower_bits;
                mask = ((value_mask as u64) << (64 - regwidth)) >> pos;
            }
        }

        // 写入最后一个 u64（如果有更新）
        if computed != this_word {
            self.data[idx] = computed;
        }
    }

    /// 序列化到字节数组（大端序短字）
    /// - 将寄存器值编码为大端序字节流
    /// - 短字按索引升序存储，从字节高位到低位
    /// - 添加填充位（0）到最后字节低位
    /// - 示例：regwidth=5, m=4, 值 [0, 1, 2, 3] -> [0x00, 0x44, 0x30]
    fn write_bytes(&self, bytes: &mut [u8], m: u32, regwidth: u32) {
        let expected_bytes = ((m * regwidth + 7) / 8) as usize;
        if bytes.len() < expected_bytes  {
            return; // 应抛出错误，当前仅返回
        }

        // 计算完整 u64 数量（每个 u64 转为 8 字节）
        let mut byte_offset = 0;
        let n_words = expected_bytes / 8;
        // 写入完整 u64（大端序）
        for i in 0..n_words {
            // 将 u64 转为 8 字节大端数组
            bytes[byte_offset..byte_offset + 8].copy_from_slice(&self.data[i].to_be_bytes());
            byte_offset += 8;
        }

        // 处理剩余字节（不足 8 字节的部分）
        let remainder = expected_bytes % 8;
        if remainder > 0 {
            let last_word = self.data[n_words];
            // 逐字节提取 last_word 的高位
            for i in 0..remainder {
                // 从高位提取第 i 字节：右移 (64 - 8*(i+1))
                bytes[byte_offset + i] = (last_word >> (64 - 8 * (i + 1))) as u8;
            }
        }
    }

    /// 从字节数组反序列化
    /// - 读取大端序字节流，重建 Vec<u64>
    /// - 处理填充位（忽略最后字节的低位）
    /// - 验证输入长度，抛出错误如果不足
    fn from_bytes(bytes: &[u8], m: u32, regwidth: u32) -> Result<Self> {
        let expected_bytes = ((m * regwidth + 7) / 8) as usize;
        if bytes.len() < expected_bytes {
            return Err(anyhow::anyhow!("Insufficient bytes"));
        }

        // 计算完整 u64 数量
        let n = expected_bytes;
        let n_words = n / 8;
        let mut words = vec![0u64; (expected_bytes + 7) / 8];
        // 读取完整 u64
        for i in 0..n_words {
            let offset = i * 8;
            words[i] = u64::from_be_bytes(bytes[offset..offset + 8].try_into().expect("Invalid slice length"));
        }

        // 处理剩余字节（不足 8 字节）
        let remainder = n % 8;
        if remainder > 0 {
            let mut last_value = 0u64;
            // 从低位到高位合并字节
            for i in 0..remainder {
                let shift_amount = 64 - 8 * (i + 1);
                // 左移字节到正确位置
                last_value |= (bytes[n - (remainder - i)] as u64) << shift_amount;
            }
            words[n_words] = last_value;
        }

        Ok(Self{data: words})
    }
}

/// 从字节数组的指定地址读取 `n_bits` 位，并将其作为 u64 的最低有效位（LSB）返回。
/// 地址是 0 索引的位位置，其中 0 表示第 0 个字节的最高有效位（MSB），
/// 63 表示第 0 个字节的最低有效位（LSB），64 表示第 1 个字节的 MSB，依此类推。
///
/// # 参数
/// * `bytes` - 输入的字节数组。
/// * `addr` - 起始位位置（0 索引）。
/// * `n_bits` - 要读取的位数。
///
/// # 返回
/// 包含读取位的最低有效位的 u64 值。
pub fn read_bits(bytes: &[u8], addr: usize, n_bits: usize) -> u64 {
    let mut idx = addr >> 3; // 除以 8 得到字节索引
    let mut pos = addr & 0x7; // 模 8 得到字节内的位位置
    let mut value: u64 = 0; // 存储读取的结果
    let mut bits_required = n_bits; // 剩余需要读取的位数

    while bits_required > 0 {
        // 计算当前字节中可用的位数
        let bits_available = (8 - pos).min(bits_required);

        // 确保不会读取超过字节数组的范围
        if idx >= bytes.len() {
            return value; // 如果超出范围，返回当前值
        }

        // 移位以腾出空间
        value <<= bits_available as u64;

        // 提取当前字节中的目标位
        let bits = if bits_available == 8 {
            // 如果读取整个字节，直接使用字节值
            bytes[idx]
        } else {
            // 创建掩码并提取位
            let mask = ((1u8 << bits_available) - 1) << (8 - pos - bits_available);
            let mut bits = bytes[idx] & mask;
            bits >> (8 - (pos + bits_available))
        };

        // 将提取的位合并到结果中
        value |= bits as u64;

        // 更新位置和索引
        pos += bits_available;
        if pos == 8 {
            idx += 1;
            pos = 0;
        }

        bits_required -= bits_available;
    }

    value
}

/// 将 `value` 的最低 `n_bits` 位写入字节数组的指定地址。
/// 地址是 0 索引的位位置，其中 0 表示第 0 个字节的最高有效位（MSB），
/// 63 表示第 0 个字节的最低有效位（LSB），64 表示第 1 个字节的 MSB，依此类推。
///
/// # 参数
/// * `bytes` - 可变的字节数组，用于写入。
/// * `addr` - 起始位位置（0 索引）。
/// * `value` - 要写入的值，其最低有效位将被使用。
/// * `n_bits` - 要写入的位数。
pub fn write_bits(bytes: &mut [u8], addr: usize, value: u64, n_bits: usize) {
    let mut idx = addr >> 3; // 除以 8 得到字节索引
    let mut pos = addr & 0x7; // 模 8 得到字节内的位位置
    let mut bits_remaining = n_bits; // 剩余需要写入的位数

    while bits_remaining > 0 {
        // 计算当前字节中可以写入的位数
        let bits_to_write = (8 - pos).min(bits_remaining);

        // 确保不会写入超过字节数组的范围
        if idx >= bytes.len() {
            return; // 如果超出范围，提前返回
        }

        // 提取要写入的位
        let part_to_write = if bits_to_write == 8 {
            // 如果写入整个字节，直接使用值
            (value >> (bits_remaining - bits_to_write)) as u8
        } else {
            // 创建掩码并提取位
            let mask = (1u8 << bits_to_write) - 1;
            let part = mask & ((value >> (bits_remaining - bits_to_write)) as u8);
            part << (8 - (pos + bits_to_write))
        };

        // 使用 OR 操作将位写入字节数组
        bytes[idx] |= part_to_write;

        // 更新位置和索引
        pos += bits_to_write;
        if pos == 8 {
            idx += 1;
            pos = 0;
        }

        bits_remaining -= bits_to_write;
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use super::*;

    #[test]
    fn test_explicit_sparse_threshold() {
        for regwidth in [4, 5, 6] {
            for log2m in [10, 11, 12, 13, 14] {
                let explicit_threshold = calculate_explicit_threshold(log2m, regwidth);
                let sparse_threshold = calculate_sparse_threshold(log2m, regwidth);
                let m = 1 << log2m;
                println!("log2m: {}, regwidth: {}, explicit_threshold: {}, sparse_threshold: {}, m:{}", log2m, regwidth, explicit_threshold, sparse_threshold, m)
            }
        }
    }

    #[test]
    fn test_bytes_regwidth() {
        for regwidth in [4, 5, 6] {
            let log2m = 11;
            let m = 1 << log2m;
            let m_f64 = m as f64;
            let m_bits_mask = m - 1;
            let value_mask = (1 << regwidth) - 1;
            let max_register_value = (1 << regwidth) - 1;
            let pw_max_mask = !((1u64 << (max_register_value - 1)) - 1);
            let mut bytes = vec![0; m as usize];
            let mut full = Full::new(m, regwidth);
            for i in 0..100000u32 {
                let hash =  murmur3_x64_128(&mut Cursor::new(i.to_le_bytes()), 0).unwrap() as u64;
                let (index, reg) = Hll::compute_index_and_reg(hash, log2m, pw_max_mask, m_bits_mask);
                if reg == 0 {
                    continue;
                }
                if reg > bytes[index] {
                    bytes[index] = reg;
                }
                full.set_reg(index, reg, regwidth)
            }
            let mut sum = 0.0;
            let mut number_of_zeroes = 0;
            for i in 0..m as usize {
                let reg1 = bytes[i];
                let reg2 = full.get_reg(i, regwidth);
                //println!("{},{},{}", reg1, reg2, reg1 == reg2);
                assert_eq!(reg1, reg2);
                sum += 1.0 / (1u64 << reg1) as f64;
                if reg1 == 0 {
                    number_of_zeroes += 1;
                }
            }
            let (sum2, number_of_zeroes2) = full.indicator(m, regwidth, value_mask);
            println!("sum:{},number_of_zeroes:{}\nsum2:{},number_of_zeroes2:{}", sum, number_of_zeroes, sum2, number_of_zeroes2);
            println!("{}", "-".repeat(100));
        }
    }

    #[test]
    fn test_full_merge() {
        for regwidth in [4, 5, 6] {
            let log2m = 11;
            let m = 1 << log2m;
            let m_f64 = m as f64;
            let m_bits_mask = m - 1;
            let value_mask = (1 << regwidth) - 1;
            let max_register_value = (1 << regwidth) - 1;
            let pw_max_mask = !((1u64 << (max_register_value - 1)) - 1);
            let mut bytes = vec![0; m as usize];
            let mut full = Full::new(m, regwidth);
            for i in 0..100000u32 {
                let hash =  murmur3_x64_128(&mut Cursor::new(i.to_le_bytes()), 0).unwrap() as u64;
                let (index, reg) = Hll::compute_index_and_reg(hash, log2m, pw_max_mask, m_bits_mask);
                if reg == 0 {
                    continue;
                }
                if reg > bytes[index] {
                    bytes[index] = reg;
                }
                full.set_reg(index, reg, regwidth)
            }

            // sub start
            let mut full1 = Full::new(m, regwidth);
            for i in 100000..200000u32 {
                let hash =  murmur3_x64_128(&mut Cursor::new(i.to_le_bytes()), 0).unwrap() as u64;
                let (index, reg) = Hll::compute_index_and_reg(hash, log2m, pw_max_mask, m_bits_mask);
                if reg == 0 {
                    continue;
                }
                if reg > bytes[index] {
                    bytes[index] = reg;
                }
                full1.set_reg(index, reg, regwidth)
            }
            full.merge(&full1, m, regwidth, value_mask);
            let mut full2 = Full::new(m, regwidth);
            for i in 150000..300000u32 {
                let hash =  murmur3_x64_128(&mut Cursor::new(i.to_le_bytes()), 0).unwrap() as u64;
                let (index, reg) = Hll::compute_index_and_reg(hash, log2m, pw_max_mask, m_bits_mask);
                if reg == 0 {
                    continue;
                }
                if reg > bytes[index] {
                    bytes[index] = reg;
                }
                full2.set_reg(index, reg, regwidth)
            }
            full.merge(&full2, m, regwidth, value_mask);
            // sub end

            let mut sum = 0.0;
            let mut number_of_zeroes = 0;
            for i in 0..m as usize {
                let reg1 = bytes[i];
                let reg2 = full.get_reg(i, regwidth);
                //println!("{},{},{}", reg1, reg2, reg1 == reg2);
                assert_eq!(reg1, reg2);
                sum += 1.0 / (1u64 << reg1) as f64;
                if reg1 == 0 {
                    number_of_zeroes += 1;
                }
            }
            let (sum2, number_of_zeroes2) = full.indicator(m, regwidth, value_mask);
            println!("sum:{},number_of_zeroes:{}\nsum2:{},number_of_zeroes2:{}", sum, number_of_zeroes, sum2, number_of_zeroes2);
            println!("{}", "-".repeat(100));
        }
    }

    #[test]
    fn test_hll_cardinality() {
        let ns = vec![10, 100, 1000, 10000, 100000, 1000000, 10000000];
        let ps = [11, 12];
        for &p in &ps {
            println!("p:{}", p);
            for &n in &ns {
                let mut hll = Hll::new(p, 6);
                for i in 0..n {
                    //hll.add(&i.to_string());
                    //hll.add_str(&i.to_string());
                    hll.add_u32(i);
                }
                let estimate = hll.cardinality();
                let percent_err = (estimate - n as f64).abs() * 100.0 / n as f64;
                println!("n:{},estimate:{},percentErr:{}", n, estimate, percent_err);
            }
        }
    }

    #[test]
    fn test_merge() {
        let mut hll = Hll::new(12, 6);
        let rangs = [(0, 5), (3, 10), (10, 20), (20, 100), (50, 200), (100, 500), (500, 1000), (1000, 5000), (5000, 10000), (10000, 50000), (50000, 100000), (100000, 500000), (500000, 1000000), (1000000, 5000000), (2000000, 10000000)];
        for (start, end) in rangs {
            let mut sub_hll = Hll::new(12, 6);
            for i in start..end {
                sub_hll.add_u32(i);
            }
            hll.merge(&sub_hll).unwrap();
            println!("add [{},{}):{},{}", start, end, sub_hll.cardinality() as u64, hll.cardinality() as u64)
        }
    }

    #[test]
    fn test_to_bytes_from_bytes() {
        let ns = vec![0, 10, 100, 1000, 10000, 100000];
        for &n in &ns {
            let mut hll = Hll::new(12, 6);
            for i in 0..n {
                hll.add_u32(i);
            }
            let bytes = hll.to_bytes();
            let hll_deserialized = Hll::from_bytes(&bytes).unwrap();
            println!("{}, {}", hll.cardinality(), hll_deserialized.cardinality());
            assert_eq!(hll.cardinality(), hll_deserialized.cardinality());
            assert!(hll == hll_deserialized);
        }
    }

    #[test]
    fn test_to_bytes_base64() {
        let ns = vec![0, 10, 100, 1000, 10000, 100000];
        for &n in &ns {
            let mut hll = Hll::new(12, 6);
            for i in 0..n {
                hll.add_u32(i);
            }
            let bytes = hll.to_bytes();
            let base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
            println!("{}", hll.cardinality());
            println!("{}", base64);
        }
    }

    #[test]
    fn write_bits_read_bits() {
        let mut buffer = [0u8; 5];
        let index = 11;
        let reg = 6;
        let value = index << 6 | reg;
        println!("{:b} + {:b} = {:b} (十进制: {})", index, reg, value, value);
        write_bits(&mut buffer, 0, value, 17);
        let index = 1099;
        let reg = 19;
        let value = index << 6 | reg;
        println!("{:b} + {:b} = {:b} (十进制: {})", index, reg, value, value);
        write_bits(&mut buffer, 17,  value, 17);
        println!("{:?}", buffer);

        // 来自 HLL sparse 格式描述的示例字节数组
        let bytes = vec![0x01, 0x63, 0x44, 0xB4, 0xC0];
        println!("{:?}", bytes);
        // 从位位置 0 读取 17 位（应读取第一个短字：index=11, value=6）
        let word = read_bits(&bytes, 0, 17);
        println!("读取的值: {:b} (十进制: {})", word, word); // 应输出 index=11, value=6
        let index = word >> 6;
        let reg = word & ((1 << 6) - 1);
        println!("index:{},reg:{}", index, reg);
        let word = read_bits(&bytes, 17, 17);
        println!("读取的值: {:b} (十进制: {})", word, word); // 应输出 index=1099, value=19
        let index = word >> 6;
        let reg = word & ((1 << 6) - 1);
        println!("index:{},reg:{}", index, reg);
    }




}