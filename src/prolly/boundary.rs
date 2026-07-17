//! Boundary detection for Prolly Tree chunking
//!
//! Determines where nodes should split based on content hashing.
//! Uses xxHash64 for fast, deterministic boundary detection.

use std::collections::VecDeque;
use std::hash::Hasher;
use xxhash_rust::xxh64::Xxh64;

use super::config::Config;
use super::error::Error;
use super::format::{BoundaryInput, BoundaryRule, ChunkMeasure, ChunkingSpec};
use super::node::Node;

const LEVEL_SALT: u64 = 0x9e37_79b9_7f4a_7c15;

/// Resettable boundary state for one ordered tree level.
pub struct BoundaryDetector {
    spec: ChunkingSpec,
    seed: u64,
    entries: u64,
    logical_bytes: u64,
    encoded_bytes: u64,
    previous_measure: u64,
    rolling_window: VecDeque<u8>,
    rolling_hash: u64,
}

impl BoundaryDetector {
    /// Create a detector for a persisted policy and tree level.
    pub fn new(mut spec: ChunkingSpec, level: u16) -> Result<Self, Error> {
        spec.validate()?;
        if level > 0 && spec.min < 2 {
            spec.min = 2;
            spec.target = spec.target.max(2);
            spec.max = spec.max.max(2);
        }
        let seed = if spec.level_salt {
            spec.hash_seed ^ u64::from(level).wrapping_mul(LEVEL_SALT)
        } else {
            spec.hash_seed
        };
        Ok(Self {
            spec,
            seed,
            entries: 0,
            logical_bytes: 0,
            encoded_bytes: 0,
            previous_measure: 0,
            rolling_window: VecDeque::new(),
            rolling_hash: 0,
        })
    }

    /// Observe one ordered entry and return whether the chunk ends after it.
    pub fn observe(
        &mut self,
        key: &[u8],
        value: &[u8],
        encoded_entry_bytes: usize,
    ) -> Result<bool, Error> {
        let encoded_entry_bytes = encoded_entry_bytes as u64;
        if self.entries == 0 && encoded_entry_bytes > self.spec.hard_max_node_bytes {
            return Err(Error::EntryTooLarge {
                encoded_bytes: encoded_entry_bytes,
                limit: self.spec.hard_max_node_bytes,
            });
        }

        self.previous_measure = self.measure();
        self.entries = self.entries.saturating_add(1);
        self.logical_bytes = self
            .logical_bytes
            .saturating_add(key.len() as u64)
            .saturating_add(value.len() as u64);
        self.encoded_bytes = self.encoded_bytes.saturating_add(encoded_entry_bytes);

        let input_hash = hash_entry(self.seed, &self.spec.input, key, value);
        if matches!(self.spec.rule, BoundaryRule::RollingBuzHash { .. }) {
            self.observe_rolling(key, value);
        }

        let measure = self.measure();
        let boundary = if self.encoded_bytes >= self.spec.hard_max_node_bytes
            || measure >= self.spec.max
        {
            true
        } else if measure < self.spec.min {
            false
        } else {
            match self.spec.rule {
                BoundaryRule::HashThreshold { factor } => (input_hash as u32) <= u32::MAX / factor,
                BoundaryRule::Weibull { shape } => weibull_boundary(
                    input_hash,
                    self.previous_measure,
                    measure,
                    self.spec.target,
                    shape,
                ),
                BoundaryRule::RollingBuzHash { .. } => {
                    let eligible_previous = self.previous_measure.max(self.spec.min);
                    let eligible_current = measure.max(self.spec.min);
                    let delta = eligible_current.saturating_sub(eligible_previous);
                    let scale = self.spec.target.saturating_sub(self.spec.min).max(1);
                    self.rolling_hash
                        <= deterministic_exponential_threshold(u128::from(delta), u128::from(scale))
                }
            }
        };
        if boundary {
            self.reset();
        }
        Ok(boundary)
    }

    /// Reset state at the beginning of a new chunk.
    pub fn reset(&mut self) {
        self.entries = 0;
        self.logical_bytes = 0;
        self.encoded_bytes = 0;
        self.previous_measure = 0;
        self.rolling_window.clear();
        self.rolling_hash = 0;
    }

    pub(crate) fn supports_independent_hashing(&self) -> bool {
        self.spec.measure == ChunkMeasure::EntryCount
            && matches!(self.spec.rule, BoundaryRule::HashThreshold { .. })
    }

    pub(crate) fn independent_hash_boundary(&self, key: &[u8], value: &[u8]) -> Option<bool> {
        let BoundaryRule::HashThreshold { factor } = self.spec.rule else {
            return None;
        };
        if self.spec.measure != ChunkMeasure::EntryCount {
            return None;
        }
        Some((hash_entry(self.seed, &self.spec.input, key, value) as u32) <= u32::MAX / factor)
    }

    fn measure(&self) -> u64 {
        match self.spec.measure {
            ChunkMeasure::EntryCount => self.entries,
            ChunkMeasure::LogicalBytes => self.logical_bytes,
            ChunkMeasure::EncodedBytes => self.encoded_bytes,
        }
    }

    fn observe_rolling(&mut self, key: &[u8], value: &[u8]) {
        let window = match self.spec.rule {
            BoundaryRule::RollingBuzHash { window } => usize::from(window),
            _ => return,
        };
        rolling_feed_len(self, key.len() as u64, window);
        for byte in key {
            self.roll_byte(*byte, window);
        }
        if self.spec.input == BoundaryInput::KeyValue {
            rolling_feed_len(self, value.len() as u64, window);
            for byte in value {
                self.roll_byte(*byte, window);
            }
        }
    }

    fn roll_byte(&mut self, byte: u8, window: usize) {
        self.rolling_hash = self.rolling_hash.rotate_left(1) ^ byte_hash(self.seed, byte);
        self.rolling_window.push_back(byte);
        if self.rolling_window.len() > window {
            if let Some(old) = self.rolling_window.pop_front() {
                self.rolling_hash ^= byte_hash(self.seed, old).rotate_left((window % 64) as u32);
            }
        }
    }
}

fn rolling_feed_len(detector: &mut BoundaryDetector, len: u64, window: usize) {
    for byte in len.to_be_bytes() {
        detector.roll_byte(byte, window);
    }
}

fn byte_hash(seed: u64, byte: u8) -> u64 {
    let mut hasher = Xxh64::new(seed ^ 0xa076_1d64_78bd_642f);
    hasher.write_u8(byte);
    hasher.finish()
}

fn hash_entry(seed: u64, input: &BoundaryInput, key: &[u8], value: &[u8]) -> u64 {
    let mut hasher = Xxh64::new(seed);
    hasher.write(&(key.len() as u64).to_be_bytes());
    hasher.write(key);
    if *input == BoundaryInput::KeyValue {
        hasher.write(&(value.len() as u64).to_be_bytes());
        hasher.write(value);
    }
    hasher.finish()
}

pub(crate) fn entry_count_boundary(
    spec: &ChunkingSpec,
    level: u16,
    count: usize,
    key: &[u8],
) -> Result<bool, Error> {
    spec.validate()?;
    let BoundaryRule::HashThreshold { factor } = spec.rule else {
        return Err(Error::InvalidFormat(
            "entry-count boundary probe requires a hash-threshold rule".to_string(),
        ));
    };
    if spec.measure != ChunkMeasure::EntryCount || spec.input != BoundaryInput::Key {
        return Err(Error::InvalidFormat(
            "entry-count boundary probe requires key-only entry-count chunking".to_string(),
        ));
    }
    let count = count as u64;
    if count >= spec.max {
        return Ok(true);
    }
    if count < spec.min {
        return Ok(false);
    }
    let seed = if spec.level_salt {
        spec.hash_seed ^ u64::from(level).wrapping_mul(LEVEL_SALT)
    } else {
        spec.hash_seed
    };
    Ok((hash_entry(seed, &spec.input, key, &[]) as u32) <= u32::MAX / factor)
}

const Q62: u128 = 1_u128 << 62;

fn deterministic_exponential_threshold(delta: u128, scale: u128) -> u64 {
    if delta == 0 {
        return 0;
    }
    if scale == 0 || delta / scale >= 64 {
        return u64::MAX;
    }

    let mut exponent = scaled_ratio_q62(delta, scale);
    let mut squarings = 0;
    while exponent > Q62 / 16 {
        exponent = (exponent + 1) / 2;
        squarings += 1;
    }

    let mut survival = exp_neg_series_q62(exponent);
    for _ in 0..squarings {
        survival = survival.saturating_mul(survival) / Q62;
    }

    ((Q62.saturating_sub(survival) * u128::from(u64::MAX)) / Q62) as u64
}

fn scaled_ratio_q62(numerator: u128, denominator: u128) -> u128 {
    debug_assert!(denominator > 0);
    let whole = numerator / denominator;
    let remainder = numerator % denominator;
    let fraction = if remainder <= u128::MAX / Q62 {
        (remainder * Q62) / denominator
    } else {
        fractional_ratio_q62(remainder, denominator)
    };
    whole.saturating_mul(Q62).saturating_add(fraction)
}

fn fractional_ratio_q62(mut numerator: u128, denominator: u128) -> u128 {
    debug_assert!(numerator < denominator);
    let mut quotient = 0_u128;
    for _ in 0..62 {
        quotient <<= 1;
        let complement = denominator - numerator;
        if numerator >= complement {
            numerator -= complement;
            quotient |= 1;
        } else {
            numerator <<= 1;
        }
    }
    quotient
}

fn exp_neg_series_q62(exponent: u128) -> u128 {
    debug_assert!(exponent <= Q62 / 16);
    let mut survival = Q62;
    let mut term = Q62;
    for divisor in 1_u128..=8 {
        term = (term * exponent) / Q62 / divisor;
        if divisor % 2 == 0 {
            survival = survival.saturating_add(term);
        } else {
            survival = survival.saturating_sub(term);
        }
    }
    survival
}

fn weibull_boundary(hash: u64, previous: u64, current: u64, target: u64, shape: u32) -> bool {
    let (hazard_delta, hazard_scale) = match shape {
        1 => (
            u128::from(current.saturating_sub(previous)),
            u128::from(target.max(1)),
        ),
        2 => (
            u128::from(current).pow(2) - u128::from(previous).pow(2),
            u128::from(target.max(1)).pow(2),
        ),
        _ => return false,
    };
    hash <= deterministic_exponential_threshold(hazard_delta, hazard_scale)
}

/// Check if entry at index creates a chunk boundary in a node.
///
/// Boundary detection rules:
/// 1. Below min_chunk_size: never split (returns false)
/// 2. At or above max_chunk_size: always split (returns true)
/// 3. Otherwise: hash-based probabilistic boundary
///
/// The hash-based boundary uses xxHash64 on the key+value pair.
/// A boundary is detected when the lower 32 bits of the hash
/// are less than or equal to `u32::MAX / chunking_factor`.
///
/// # Arguments
/// * `node` - The node containing the entry
/// * `idx` - Index of the entry to check
///
/// # Returns
/// `true` if a boundary should be created after this entry
pub fn is_boundary(node: &Node, idx: usize) -> bool {
    let count = node.keys.len();

    // Below min size: never split
    if count < node.min_chunk_size() {
        return false;
    }

    // At or above max size: always split
    if count >= node.max_chunk_size() {
        return true;
    }

    is_hash_boundary(
        node.hash_seed(),
        node.chunking_factor(),
        &node.keys[idx],
        &node.vals[idx],
    )
}

/// Check if entry creates a chunk boundary using Config.
///
/// Same logic as `is_boundary()` but takes Config and entry data directly
/// instead of a Node reference. Useful for tree-level operations where
/// you don't have a fully constructed node.
///
/// # Arguments
/// * `config` - Tree configuration with chunking parameters
/// * `count` - Current number of entries in the node
/// * `key` - Key bytes of the entry to check
/// * `val` - Value bytes of the entry to check
///
/// # Returns
/// `true` if a boundary should be created after this entry
pub fn is_boundary_config(config: &Config, count: usize, key: &[u8], val: &[u8]) -> bool {
    // Below min size: never split
    if count < config.min_chunk_size() {
        return false;
    }

    // At or above max size: always split
    if count >= config.max_chunk_size() {
        return true;
    }

    is_hash_boundary_config(config, key, val)
}

/// Check only the hash predicate for a boundary, without applying min/max size rules.
///
/// Bulk builders can precompute this part in parallel, then apply the min/max
/// checks using the current chunk-local entry count.
pub(crate) fn is_hash_boundary_config(config: &Config, key: &[u8], val: &[u8]) -> bool {
    is_hash_boundary(config.hash_seed(), config.chunking_factor(), key, val)
}

fn is_hash_boundary(hash_seed: u64, chunking_factor: u32, key: &[u8], val: &[u8]) -> bool {
    let mut hasher = Xxh64::new(hash_seed);
    hasher.write(key);
    hasher.write(val);
    let hash = hasher.finish();

    // Use lower 32 bits for threshold comparison
    let hash_val = (hash & 0xFFFF_FFFF) as u32;

    // Threshold: lower = more boundaries = smaller nodes
    let threshold = u32::MAX / chunking_factor;
    hash_val <= threshold
}

#[cfg(test)]
mod tests {
    use super::super::encoding::Encoding;
    use super::*;

    #[test]
    fn deterministic_threshold_golden_vectors() {
        assert_eq!(deterministic_exponential_threshold(0, 12_288), 0);
        assert_eq!(
            deterministic_exponential_threshold(44, 12_288),
            65_934_676_975_190_507
        );
        assert_eq!(
            deterministic_exponential_threshold(4_096, 12_288),
            5_229_074_366_755_166_475
        );
        assert_eq!(
            deterministic_exponential_threshold(12_288, 12_288),
            11_660_566_172_440_661_763
        );
        assert_eq!(
            deterministic_exponential_threshold(u128::MAX - 1, u128::MAX),
            11_660_566_172_440_661_763
        );
        assert_eq!(deterministic_exponential_threshold(u128::MAX, 1), u64::MAX);
    }

    #[test]
    fn test_is_boundary_below_min_chunk_size() {
        // Node with fewer entries than min_chunk_size should never trigger boundary
        let node = Node::builder()
            .keys(vec![b"a".to_vec(), b"b".to_vec()])
            .vals(vec![b"1".to_vec(), b"2".to_vec()])
            .min_chunk_size(4)
            .max_chunk_size(100)
            .chunking_factor(128)
            .build();

        // 2 entries < min_chunk_size of 4, so no boundary
        assert!(!is_boundary(&node, 0));
        assert!(!is_boundary(&node, 1));
    }

    #[test]
    fn test_is_boundary_at_max_chunk_size() {
        // Node at max_chunk_size should always trigger boundary
        let keys: Vec<Vec<u8>> = (0..10).map(|i| vec![i]).collect();
        let vals: Vec<Vec<u8>> = (0..10).map(|i| vec![i]).collect();

        let node = Node::builder()
            .keys(keys)
            .vals(vals)
            .min_chunk_size(2)
            .max_chunk_size(10) // exactly at max
            .chunking_factor(128)
            .build();

        // At max_chunk_size, should always return true
        assert!(is_boundary(&node, 0));
    }

    #[test]
    fn test_is_boundary_deterministic() {
        // Same node should always produce same boundary result
        let node = Node::builder()
            .keys(vec![
                b"key1".to_vec(),
                b"key2".to_vec(),
                b"key3".to_vec(),
                b"key4".to_vec(),
                b"key5".to_vec(),
            ])
            .vals(vec![
                b"val1".to_vec(),
                b"val2".to_vec(),
                b"val3".to_vec(),
                b"val4".to_vec(),
                b"val5".to_vec(),
            ])
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(128)
            .hash_seed(42)
            .build();

        let result1 = is_boundary(&node, 2);
        let result2 = is_boundary(&node, 2);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_is_boundary_config_below_min() {
        let config = Config::builder()
            .min_chunk_size(4)
            .max_chunk_size(100)
            .chunking_factor(128)
            .build();

        // count=2 < min_chunk_size=4, so no boundary
        assert!(!is_boundary_config(&config, 2, b"key", b"val"));
    }

    #[test]
    fn test_is_boundary_config_at_max() {
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(10)
            .chunking_factor(128)
            .build();

        // count=10 >= max_chunk_size=10, so always boundary
        assert!(is_boundary_config(&config, 10, b"key", b"val"));
    }

    #[test]
    fn test_is_boundary_config_deterministic() {
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(128)
            .hash_seed(42)
            .build();

        let result1 = is_boundary_config(&config, 5, b"test_key", b"test_val");
        let result2 = is_boundary_config(&config, 5, b"test_key", b"test_val");
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_is_boundary_matches_is_boundary_config() {
        // Both functions should produce the same result for equivalent inputs
        let node = Node::builder()
            .keys(vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
            ])
            .vals(vec![
                b"1".to_vec(),
                b"2".to_vec(),
                b"3".to_vec(),
                b"4".to_vec(),
                b"5".to_vec(),
            ])
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(128)
            .hash_seed(42)
            .encoding(Encoding::Raw)
            .build();

        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(128)
            .hash_seed(42)
            .encoding(Encoding::Raw)
            .build();

        for idx in 0..node.keys.len() {
            let node_result = is_boundary(&node, idx);
            let config_result =
                is_boundary_config(&config, node.keys.len(), &node.keys[idx], &node.vals[idx]);
            assert_eq!(
                node_result, config_result,
                "Mismatch at index {}: is_boundary={}, is_boundary_config={}",
                idx, node_result, config_result
            );
        }
    }

    #[test]
    fn entry_count_threshold_exposes_parallel_hash_predicate() {
        let spec = ChunkingSpec {
            min: 1,
            max: 128,
            ..ChunkingSpec::default()
        };
        let mut detector = BoundaryDetector::new(spec, 0).unwrap();

        let independent = detector
            .independent_hash_boundary(b"parallel-key", b"ignored-value")
            .expect("entry-count threshold hashing is independent");

        assert_eq!(
            detector
                .observe(b"parallel-key", b"ignored-value", 32)
                .unwrap(),
            independent
        );
    }

    #[test]
    fn test_different_seeds_produce_different_results() {
        let config1 = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(128)
            .hash_seed(1)
            .build();

        let config2 = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(128)
            .hash_seed(999999)
            .build();

        // Test with multiple keys to find at least one difference
        let mut found_difference = false;
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let val = format!("val{}", i).into_bytes();
            let r1 = is_boundary_config(&config1, 5, &key, &val);
            let r2 = is_boundary_config(&config2, 5, &key, &val);
            if r1 != r2 {
                found_difference = true;
                break;
            }
        }
        assert!(
            found_difference,
            "Different seeds should produce different boundary patterns"
        );
    }

    #[test]
    fn test_higher_chunking_factor_fewer_boundaries() {
        // Higher chunking factor = higher threshold = fewer boundaries
        let config_low = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(1000)
            .chunking_factor(4) // Low factor = more boundaries
            .hash_seed(0)
            .build();

        let config_high = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(1000)
            .chunking_factor(1024) // High factor = fewer boundaries
            .hash_seed(0)
            .build();

        let mut low_boundaries = 0;
        let mut high_boundaries = 0;

        for i in 0..1000 {
            let key = format!("key{:04}", i).into_bytes();
            let val = format!("val{:04}", i).into_bytes();
            if is_boundary_config(&config_low, 100, &key, &val) {
                low_boundaries += 1;
            }
            if is_boundary_config(&config_high, 100, &key, &val) {
                high_boundaries += 1;
            }
        }

        assert!(
            low_boundaries > high_boundaries,
            "Lower chunking factor should produce more boundaries: low={}, high={}",
            low_boundaries,
            high_boundaries
        );
    }
}
