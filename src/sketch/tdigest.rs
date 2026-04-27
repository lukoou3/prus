//! An implementation of the [TDigest sketch algorithm] providing approximate
//! quantile calculations.
//!
//! The TDigest code in this module is modified from
//! <https://github.com/MnO2/t-digest>, itself a rust reimplementation of
//! [Facebook's Folly TDigest] implementation.
//!
//! Alterations include reduction of runtime heap allocations, broader type
//! support, (de-)serialization support, reduced type conversions and null value
//! tolerance.
//!
//! [TDigest sketch algorithm]: https://arxiv.org/abs/1902.04023
//! [Facebook's Folly TDigest]: https://github.com/facebook/folly/blob/main/folly/stats/TDigest.h

use std::cmp::Ordering;
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt};

pub const DEFAULT_MAX_SIZE: usize = 100;

/// Centroid implementation to the cluster mentioned in the paper.
#[derive(Debug, PartialEq, Clone)]
pub struct Centroid {
    mean: f64,
    weight: f64,
}

impl PartialOrd for Centroid {
    fn partial_cmp(&self, other: &Centroid) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Centroid {}

impl Ord for Centroid {
    fn cmp(&self, other: &Centroid) -> Ordering {
        self.mean.total_cmp(&other.mean)
    }
}

impl Centroid {
    pub fn new(mean: f64, weight: f64) -> Self {
        Centroid { mean, weight }
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        self.mean
    }

    #[inline]
    pub fn weight(&self) -> f64 {
        self.weight
    }

    pub fn add(&mut self, sum: f64, weight: f64) -> f64 {
        let new_sum = sum + self.weight * self.mean;
        let new_weight = self.weight + weight;
        self.weight = new_weight;
        self.mean = new_sum / new_weight;
        new_sum
    }
}

impl Default for Centroid {
    fn default() -> Self {
        Centroid {
            mean: 0_f64,
            weight: 1_f64,
        }
    }
}

/// T-Digest to be operated on.
#[derive(Debug, PartialEq, Clone)]
pub struct TDigest {
    centroids: Vec<Centroid>,
    max_size: usize,
    sum: f64,
    count: u64,
    max: f64,
    min: f64,
}

impl TDigest {
    pub fn new(max_size: usize) -> Self {
        TDigest {
            centroids: Vec::new(),
            max_size,
            sum: 0_f64,
            count: 0,
            max: f64::NAN,
            min: f64::NAN,
        }
    }

    pub fn new_with_centroid(max_size: usize, centroid: Centroid) -> Self {
        TDigest {
            centroids: vec![centroid.clone()],
            max_size,
            sum: centroid.mean * centroid.weight,
            count: 1,
            max: centroid.mean,
            min: centroid.mean,
        }
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.count
    }

    #[inline]
    pub fn max(&self) -> f64 {
        self.max
    }

    #[inline]
    pub fn min(&self) -> f64 {
        self.min
    }

    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Size in bytes including `Self`.
    pub fn size(&self) -> usize {
        size_of_val(self) + (size_of::<Centroid>() * self.centroids.capacity())
    }
}

impl Default for TDigest {
    fn default() -> Self {
        TDigest {
            centroids: Vec::new(),
            max_size: 100,
            sum: 0_f64,
            count: 0,
            max: f64::NAN,
            min: f64::NAN,
        }
    }
}

impl TDigest {
    fn k_to_q(k: u64, d: usize) -> f64 {
        let k_div_d = k as f64 / d as f64;
        if k_div_d >= 0.5 {
            let base = 1.0 - k_div_d;
            1.0 - 2.0 * base * base
        } else {
            2.0 * k_div_d * k_div_d
        }
    }

    fn clamp(v: f64, lo: f64, hi: f64) -> f64 {
        if lo.is_nan() || hi.is_nan() {
            return v;
        }
        v.clamp(lo, hi)
    }

    // public for testing in other modules
    pub fn merge_unsorted_f64(&self, unsorted_values: Vec<f64>) -> TDigest {
        let mut values = unsorted_values;
        values.sort_by(|a, b| a.total_cmp(b));
        self.merge_sorted_f64(&values)
    }

    pub fn merge_sorted_f64(&self, sorted_values: &[f64]) -> TDigest {
        if sorted_values.is_empty() {
            return self.clone();
        }

        let mut result = TDigest::new(self.max_size());
        result.count = self.count() + sorted_values.len() as u64;

        let maybe_min = *sorted_values.first().unwrap();
        let maybe_max = *sorted_values.last().unwrap();

        if self.count() > 0 {
            result.min = self.min.min(maybe_min);
            result.max = self.max.max(maybe_max);
        } else {
            result.min = maybe_min;
            result.max = maybe_max;
        }

        let mut compressed: Vec<Centroid> = Vec::with_capacity(self.max_size);

        let mut k_limit: u64 = 1;
        let mut q_limit_times_count =
            Self::k_to_q(k_limit, self.max_size) * result.count() as f64;
        k_limit += 1;

        let mut iter_centroids = self.centroids.iter().peekable();
        let mut iter_sorted_values = sorted_values.iter().peekable();

        let mut curr: Centroid = if let Some(c) = iter_centroids.peek() {
            let curr = **iter_sorted_values.peek().unwrap();
            if c.mean() < curr {
                iter_centroids.next().unwrap().clone()
            } else {
                Centroid::new(*iter_sorted_values.next().unwrap(), 1.0)
            }
        } else {
            Centroid::new(*iter_sorted_values.next().unwrap(), 1.0)
        };

        let mut weight_so_far = curr.weight();

        let mut sums_to_merge = 0_f64;
        let mut weights_to_merge = 0_f64;

        while iter_centroids.peek().is_some() || iter_sorted_values.peek().is_some() {
            let next: Centroid = if let Some(c) = iter_centroids.peek() {
                if iter_sorted_values.peek().is_none()
                    || c.mean() < **iter_sorted_values.peek().unwrap()
                {
                    iter_centroids.next().unwrap().clone()
                } else {
                    Centroid::new(*iter_sorted_values.next().unwrap(), 1.0)
                }
            } else {
                Centroid::new(*iter_sorted_values.next().unwrap(), 1.0)
            };

            let next_sum = next.mean() * next.weight();
            weight_so_far += next.weight();

            if weight_so_far <= q_limit_times_count {
                sums_to_merge += next_sum;
                weights_to_merge += next.weight();
            } else {
                result.sum += curr.add(sums_to_merge, weights_to_merge);
                sums_to_merge = 0_f64;
                weights_to_merge = 0_f64;

                compressed.push(curr.clone());
                q_limit_times_count =
                    Self::k_to_q(k_limit, self.max_size) * result.count() as f64;
                k_limit += 1;
                curr = next;
            }
        }

        result.sum += curr.add(sums_to_merge, weights_to_merge);
        compressed.push(curr);
        compressed.shrink_to_fit();
        compressed.sort();

        result.centroids = compressed;
        result
    }

    fn external_merge(
        centroids: &mut [Centroid],
        first: usize,
        middle: usize,
        last: usize,
    ) {
        let mut result: Vec<Centroid> = Vec::with_capacity(centroids.len());

        let mut i = first;
        let mut j = middle;

        while i < middle && j < last {
            match centroids[i].cmp(&centroids[j]) {
                Ordering::Less => {
                    result.push(centroids[i].clone());
                    i += 1;
                }
                Ordering::Greater => {
                    result.push(centroids[j].clone());
                    j += 1;
                }
                Ordering::Equal => {
                    result.push(centroids[i].clone());
                    i += 1;
                }
            }
        }

        while i < middle {
            result.push(centroids[i].clone());
            i += 1;
        }

        while j < last {
            result.push(centroids[j].clone());
            j += 1;
        }

        i = first;
        for centroid in result.into_iter() {
            centroids[i] = centroid;
            i += 1;
        }
    }

    // Merge multiple T-Digests
    pub fn merge_digests<'a>(digests: impl IntoIterator<Item = &'a TDigest>) -> TDigest {
        let digests = digests.into_iter().collect::<Vec<_>>();
        let n_centroids: usize = digests.iter().map(|d| d.centroids.len()).sum();
        if n_centroids == 0 {
            return TDigest::default();
        }

        let max_size = digests.first().unwrap().max_size;
        let mut centroids: Vec<Centroid> = Vec::with_capacity(n_centroids);
        let mut starts: Vec<usize> = Vec::with_capacity(digests.len());

        let mut count = 0;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;

        let mut start: usize = 0;
        for digest in digests.iter() {
            starts.push(start);

            let curr_count = digest.count();
            if curr_count > 0 {
                min = min.min(digest.min);
                max = max.max(digest.max);
                count += curr_count;
                for centroid in &digest.centroids {
                    centroids.push(centroid.clone());
                    start += 1;
                }
            }
        }

        let mut digests_per_block: usize = 1;
        while digests_per_block < starts.len() {
            for i in (0..starts.len()).step_by(digests_per_block * 2) {
                if i + digests_per_block < starts.len() {
                    let first = starts[i];
                    let middle = starts[i + digests_per_block];
                    let last = if i + 2 * digests_per_block < starts.len() {
                        starts[i + 2 * digests_per_block]
                    } else {
                        centroids.len()
                    };

                    debug_assert!(first <= middle && middle <= last);
                    Self::external_merge(&mut centroids, first, middle, last);
                }
            }

            digests_per_block *= 2;
        }

        let mut result = TDigest::new(max_size);
        let mut compressed: Vec<Centroid> = Vec::with_capacity(max_size);

        let mut k_limit = 1;
        let mut q_limit_times_count = Self::k_to_q(k_limit, max_size) * count as f64;

        let mut iter_centroids = centroids.iter_mut();
        let mut curr = iter_centroids.next().unwrap();
        let mut weight_so_far = curr.weight();
        let mut sums_to_merge = 0_f64;
        let mut weights_to_merge = 0_f64;

        for centroid in iter_centroids {
            weight_so_far += centroid.weight();

            if weight_so_far <= q_limit_times_count {
                sums_to_merge += centroid.mean() * centroid.weight();
                weights_to_merge += centroid.weight();
            } else {
                result.sum += curr.add(sums_to_merge, weights_to_merge);
                sums_to_merge = 0_f64;
                weights_to_merge = 0_f64;
                compressed.push(curr.clone());
                q_limit_times_count = Self::k_to_q(k_limit, max_size) * count as f64;
                k_limit += 1;
                curr = centroid;
            }
        }

        result.sum += curr.add(sums_to_merge, weights_to_merge);
        compressed.push(curr.clone());
        compressed.shrink_to_fit();
        compressed.sort();

        result.count = count;
        result.min = min;
        result.max = max;
        result.centroids = compressed;
        result
    }

    /// To estimate the value located at `q` quantile
    pub fn estimate_quantile(&self, q: f64) -> f64 {
        if self.centroids.is_empty() {
            return 0.0;
        }

        let rank = q * self.count as f64;

        let mut pos: usize;
        let mut t;
        if q > 0.5 {
            if q >= 1.0 {
                return self.max();
            }

            pos = 0;
            t = self.count as f64;

            for (k, centroid) in self.centroids.iter().enumerate().rev() {
                t -= centroid.weight();

                if rank >= t {
                    pos = k;
                    break;
                }
            }
        } else {
            if q <= 0.0 {
                return self.min();
            }

            pos = self.centroids.len() - 1;
            t = 0_f64;

            for (k, centroid) in self.centroids.iter().enumerate() {
                if rank < t + centroid.weight() {
                    pos = k;
                    break;
                }

                t += centroid.weight();
            }
        }

        let mut delta = 0_f64;
        let mut min = self.min;
        let mut max = self.max;

        if self.centroids.len() > 1 {
            if pos == 0 {
                delta = self.centroids[pos + 1].mean() - self.centroids[pos].mean();
                max = self.centroids[pos + 1].mean();
            } else if pos == (self.centroids.len() - 1) {
                delta = self.centroids[pos].mean() - self.centroids[pos - 1].mean();
                min = self.centroids[pos - 1].mean();
            } else {
                delta = (self.centroids[pos + 1].mean() - self.centroids[pos - 1].mean())
                    / 2.0;
                min = self.centroids[pos - 1].mean();
                max = self.centroids[pos + 1].mean();
            }
        }

        let value = self.centroids[pos].mean()
            + ((rank - t) / self.centroids[pos].weight() - 0.5) * delta;

        // In `merge_digests()`: `min` is initialized to Inf, `max` is initialized to -Inf
        // and gets updated according to different `TDigest`s
        // However, `min`/`max` won't get updated if there is only one `NaN` within `TDigest`
        // The following two checks is for such edge case
        if !min.is_finite() && min.is_sign_positive() {
            min = f64::NEG_INFINITY;
        }

        if !max.is_finite() && max.is_sign_negative() {
            max = f64::INFINITY;
        }

        Self::clamp(value, min, max)
    }

    /// This method decomposes the [`TDigest`] and its [`Centroid`] instances
    /// into a series of primitive scalar values.
    ///
    /// First the values of the TDigest are packed, followed by the variable
    /// number of centroids packed into a [`ScalarValue::List`] of
    /// [`ScalarValue::Float64`]:
    ///
    /// ```text
    ///
    ///    ┌────────┬────────┬────────┬───────┬────────┬────────┐
    ///    │max_size│  sum   │ count  │  max  │  min   │centroid│
    ///    └────────┴────────┴────────┴───────┴────────┴────────┘
    ///                                                     │
    ///                               ┌─────────────────────┘
    ///                               ▼
    ///                          ┌ List ───┐
    ///                          │┌ ─ ─ ─ ┐│
    ///                          │  mean   │
    ///                          │├ ─ ─ ─ ┼│─ ─ Centroid 1
    ///                          │ weight  │
    ///                          │└ ─ ─ ─ ┘│
    ///                          │         │
    ///                          │┌ ─ ─ ─ ┐│
    ///                          │  mean   │
    ///                          │├ ─ ─ ─ ┼│─ ─ Centroid 2
    ///                          │ weight  │
    ///                          │└ ─ ─ ─ ┘│
    ///                          │         │
    ///                              ...
    ///
    /// ```
    ///
    /// The [`TDigest::from_scalar_state()`] method reverses this processes,
    /// consuming the output of this method and returning an unpacked
    /// [`TDigest`].
    pub fn to_bytes(&self)-> Vec<u8> {
        // format code, totalCount(long), compression(int), centroids-len(int), min(double), max(double),
        // 4 + 8 + 4 + 4 + 8 + 8
        let mut buf = Vec::with_capacity(36 + 8 * self.centroids.len());
        buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
        buf.extend_from_slice(&self.count.to_be_bytes());
        buf.extend_from_slice(&(self.max_size as u32).to_be_bytes());
        buf.extend_from_slice(&(self.centroids.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.min.to_be_bytes());
        buf.extend_from_slice(&self.max.to_be_bytes());
        for centroid in &self.centroids {
            buf.extend_from_slice(&centroid.mean().to_be_bytes());
            buf.extend_from_slice(&(centroid.weight() as u64).to_be_bytes());
        }
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() < 36 {
            return Err(anyhow::anyhow!("Invalid bytes length: {}", bytes.len()));
        }
        let mut cursor = Cursor::new(bytes);
        let v = cursor.read_u32::<BigEndian>().unwrap();
        if v != 1 {
            return Err(anyhow::anyhow!("Invalid version: {}", v));
        }
        let count = cursor.read_u64::<BigEndian>().unwrap();
        let max_size = cursor.read_u32::<BigEndian>().unwrap() as usize;
        let centroids_len = cursor.read_u32::<BigEndian>().unwrap() as usize;
        let min = cursor.read_f64::<BigEndian>().unwrap();
        let max = cursor.read_f64::<BigEndian>().unwrap();
        if bytes.len() < 36 + 8 * centroids_len {
            return Err(anyhow::anyhow!("Invalid bytes length: {}, centroids_len: {}",  bytes.len(), centroids_len));
        }
        let mut centroids = Vec::with_capacity(centroids_len);
        let mut sum = 0_f64;
        for _ in 0..centroids_len {
            let mean = cursor.read_f64::<BigEndian>().unwrap();
            let weight = cursor.read_u64::<BigEndian>().unwrap() as f64;
            sum += mean * weight;
            centroids.push(Centroid::new(mean, weight));
        }
        Ok(Self {
            centroids,
            max_size,
            sum,
            count,
            min,
            max,
        })
    }
}


#[cfg(test)]
mod tests {
    use base64::Engine;
    use super::*;

    // A macro to assert the specified `quantile` estimated by `t` is within the
    // allowable relative error bound.
    macro_rules! assert_error_bounds {
        ($t:ident, quantile = $quantile:literal, want = $want:literal) => {
            assert_error_bounds!(
                $t,
                quantile = $quantile,
                want = $want,
                allowable_error = 0.01
            )
        };
        ($t:ident, quantile = $quantile:literal, want = $want:literal, allowable_error = $re:literal) => {
            let ans = $t.estimate_quantile($quantile);
            let expected: f64 = $want;
            let percentage: f64 = (expected - ans).abs() / expected;
            assert!(
                percentage < $re,
                "relative error {} is more than {}% (got quantile {}, want {})",
                percentage,
                $re,
                ans,
                expected
            );
        };
    }

    #[test]
    fn test_centroid_addition_regression() {
        let vals = vec![1.0, 1.0, 1.0, 2.0, 1.0, 1.0];
        let mut t = TDigest::new(10);

        for v in vals {
            t = t.merge_unsorted_f64(vec![v]);
        }

        assert_error_bounds!(t, quantile = 0.5, want = 1.0);
        assert_error_bounds!(t, quantile = 0.95, want = 2.0);
    }

    #[test]
    fn test_int64_uniform() {
        let values = (1i64..=1000).map(|v| v as f64).collect();

        let t = TDigest::new(100);
        let t = t.merge_unsorted_f64(values);

        assert_error_bounds!(t, quantile = 0.1, want = 100.0);
        assert_error_bounds!(t, quantile = 0.5, want = 500.0);
        assert_error_bounds!(t, quantile = 0.9, want = 900.0);
    }

    #[test]
    fn test_merge_unsorted_against_uniform_distro() {
        let t = TDigest::new(100);
        let values: Vec<_> = (1..=1_000_000).map(f64::from).collect();

        let t = t.merge_unsorted_f64(values);

        assert_error_bounds!(t, quantile = 1.0, want = 1_000_000.0);
        assert_error_bounds!(t, quantile = 0.99, want = 990_000.0);
        assert_error_bounds!(t, quantile = 0.01, want = 10_000.0);
        assert_error_bounds!(t, quantile = 0.0, want = 1.0);
        assert_error_bounds!(t, quantile = 0.5, want = 500_000.0);
    }

    #[test]
    fn test_merge_unsorted_against_skewed_distro() {
        let t = TDigest::new(100);
        let mut values: Vec<_> = (1..=600_000).map(f64::from).collect();
        values.resize(1_000_000, 1_000_000_f64);

        let t = t.merge_unsorted_f64(values);

        assert_error_bounds!(t, quantile = 0.99, want = 1_000_000.0);
        assert_error_bounds!(t, quantile = 0.01, want = 10_000.0);
        assert_error_bounds!(t, quantile = 0.5, want = 500_000.0);
    }

    #[test]
    fn test_merge_digests() {
        let mut digests: Vec<TDigest> = Vec::new();

        for _ in 1..=100 {
            let t = TDigest::new(100);
            let values: Vec<_> = (1..=1_000).map(f64::from).collect();
            let t = t.merge_unsorted_f64(values);
            digests.push(t)
        }

        let t = TDigest::merge_digests(&digests);

        assert_error_bounds!(t, quantile = 1.0, want = 1000.0);
        assert_error_bounds!(t, quantile = 0.99, want = 990.0);
        assert_error_bounds!(t, quantile = 0.01, want = 10.0, allowable_error = 0.2);
        assert_error_bounds!(t, quantile = 0.0, want = 1.0);
        assert_error_bounds!(t, quantile = 0.5, want = 500.0);
    }

    #[test]
    fn test_size() {
        let t = TDigest::new(10);
        let t = t.merge_unsorted_f64(vec![0.0, 1.0]);

        assert_eq!(t.size(), 96);
    }

    #[test]
    fn test_to_bytes() {
        let t = TDigest::new(100);
        let values: Vec<_> = (1..=1_000_000).map(f64::from).collect();

        let t = t.merge_unsorted_f64(values);
        let bytes = t.to_bytes();
        let t2 = TDigest::from_bytes(&bytes).unwrap();
        println!("{:?}", t);
        println!("{:?}", t2);
        println!("{:?}", t.eq(&t2));
        for p in [0.0, 0.1, 0.5, 0.9, 0.99, 1.0] {
            println!("{}:{}", p, t.estimate_quantile(p));
            println!("{}:{}", p, t2.estimate_quantile(p));
        }
    }

    #[test]
    fn test_to_bytes_base64() {
        let t = TDigest::new(100);
        let values: Vec<_> = (1..=1_000_000).map(f64::from).collect();

        let t = t.merge_unsorted_f64(values);
        let bytes = t.to_bytes();
        let base64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        println!("{}", base64);
    }
}
