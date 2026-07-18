//! Boundary detection for Prolly Tree chunking
//!
//! Determines where nodes should split based on content hashing.
//! Uses xxHash64 for fast, deterministic boundary detection.

use std::hash::Hasher;
use xxhash_rust::xxh64::Xxh64;

use super::error::Error;
use super::format::{BoundaryInput, BoundaryRule, ChunkMeasure, ChunkingSpec};

const LEVEL_SALT: u64 = 0x9e37_79b9_7f4a_7c15;

struct ByteHashCache {
    seed: u64,
    values: [u64; 256],
    initialized: [u64; 4],
}

impl ByteHashCache {
    fn new(seed: u64) -> Self {
        Self {
            seed,
            values: [0; 256],
            initialized: [0; 4],
        }
    }

    #[inline]
    fn get(&mut self, byte: u8) -> u64 {
        let index = usize::from(byte);
        let word = index / 64;
        let mask = 1_u64 << (index % 64);
        if self.initialized[word] & mask == 0 {
            self.values[index] = byte_hash(self.seed, byte);
            self.initialized[word] |= mask;
        }
        self.values[index]
    }
}

struct RollingWindow {
    bytes: Vec<u8>,
    capacity: usize,
    next: usize,
}

impl RollingWindow {
    fn new(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
            capacity,
            next: 0,
        }
    }

    #[inline]
    fn push(&mut self, byte: u8) -> Option<u8> {
        if self.bytes.len() < self.capacity {
            self.bytes.push(byte);
            return None;
        }
        let old = std::mem::replace(&mut self.bytes[self.next], byte);
        self.next += 1;
        if self.next == self.capacity {
            self.next = 0;
        }
        Some(old)
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.next = 0;
    }
}

/// Resettable boundary state for one ordered tree level.
pub struct BoundaryDetector {
    spec: ChunkingSpec,
    seed: u64,
    entries: u64,
    logical_bytes: u64,
    encoded_bytes: u64,
    previous_measure: u64,
    rolling_window: RollingWindow,
    byte_hash_cache: Option<Box<ByteHashCache>>,
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
        let rolling_window = match spec.rule {
            BoundaryRule::RollingBuzHash { window } => RollingWindow::new(usize::from(window)),
            _ => RollingWindow::new(0),
        };
        let byte_hash_cache = matches!(spec.rule, BoundaryRule::RollingBuzHash { .. })
            .then(|| Box::new(ByteHashCache::new(seed)));
        Ok(Self {
            spec,
            seed,
            entries: 0,
            logical_bytes: 0,
            encoded_bytes: 0,
            previous_measure: 0,
            rolling_window,
            byte_hash_cache,
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
        if !matches!(self.spec.rule, BoundaryRule::RollingBuzHash { .. }) {
            return;
        }
        rolling_feed_len(self, key.len() as u64);
        for byte in key {
            self.roll_byte(*byte);
        }
        if self.spec.input == BoundaryInput::KeyValue {
            rolling_feed_len(self, value.len() as u64);
            for byte in value {
                self.roll_byte(*byte);
            }
        }
    }

    #[inline]
    fn roll_byte(&mut self, byte: u8) {
        let incoming = self
            .byte_hash_cache
            .as_mut()
            .expect("rolling detector has byte hash cache")
            .get(byte);
        self.rolling_hash = self.rolling_hash.rotate_left(1) ^ incoming;
        if let Some(old) = self.rolling_window.push(byte) {
            let outgoing = self
                .byte_hash_cache
                .as_mut()
                .expect("rolling detector has byte hash cache")
                .get(old);
            self.rolling_hash ^= outgoing.rotate_left((self.rolling_window.capacity % 64) as u32);
        }
    }
}

fn rolling_feed_len(detector: &mut BoundaryDetector, len: u64) {
    for byte in len.to_be_bytes() {
        detector.roll_byte(byte);
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
        exponent = exponent.div_ceil(2);
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    struct ReferenceRollingDetector {
        spec: ChunkingSpec,
        seed: u64,
        entries: u64,
        logical_bytes: u64,
        encoded_bytes: u64,
        previous_measure: u64,
        rolling_window: VecDeque<u8>,
        rolling_hash: u64,
    }

    impl ReferenceRollingDetector {
        fn new(spec: ChunkingSpec, level: u16) -> Self {
            let seed = if spec.level_salt {
                spec.hash_seed ^ u64::from(level).wrapping_mul(LEVEL_SALT)
            } else {
                spec.hash_seed
            };
            Self {
                spec,
                seed,
                entries: 0,
                logical_bytes: 0,
                encoded_bytes: 0,
                previous_measure: 0,
                rolling_window: VecDeque::new(),
                rolling_hash: 0,
            }
        }

        fn measure(&self) -> u64 {
            match self.spec.measure {
                ChunkMeasure::EntryCount => self.entries,
                ChunkMeasure::LogicalBytes => self.logical_bytes,
                ChunkMeasure::EncodedBytes => self.encoded_bytes,
            }
        }

        fn roll_byte(&mut self, byte: u8, window: usize) {
            self.rolling_hash = self.rolling_hash.rotate_left(1) ^ byte_hash(self.seed, byte);
            self.rolling_window.push_back(byte);
            if self.rolling_window.len() > window {
                let old = self.rolling_window.pop_front().unwrap();
                self.rolling_hash ^= byte_hash(self.seed, old).rotate_left((window % 64) as u32);
            }
        }

        fn feed_len(&mut self, len: u64, window: usize) {
            for byte in len.to_be_bytes() {
                self.roll_byte(byte, window);
            }
        }

        fn observe(&mut self, key: &[u8], value: &[u8], encoded_entry_bytes: usize) -> bool {
            self.previous_measure = self.measure();
            self.entries = self.entries.saturating_add(1);
            self.logical_bytes = self
                .logical_bytes
                .saturating_add(key.len() as u64)
                .saturating_add(value.len() as u64);
            self.encoded_bytes = self
                .encoded_bytes
                .saturating_add(encoded_entry_bytes as u64);

            let BoundaryRule::RollingBuzHash { window } = self.spec.rule else {
                unreachable!();
            };
            let window = usize::from(window);
            self.feed_len(key.len() as u64, window);
            for &byte in key {
                self.roll_byte(byte, window);
            }
            if self.spec.input == BoundaryInput::KeyValue {
                self.feed_len(value.len() as u64, window);
                for &byte in value {
                    self.roll_byte(byte, window);
                }
            }

            let measure = self.measure();
            let boundary = if self.encoded_bytes >= self.spec.hard_max_node_bytes
                || measure >= self.spec.max
            {
                true
            } else if measure < self.spec.min {
                false
            } else {
                let eligible_previous = self.previous_measure.max(self.spec.min);
                let eligible_current = measure.max(self.spec.min);
                let delta = eligible_current.saturating_sub(eligible_previous);
                let scale = self.spec.target.saturating_sub(self.spec.min).max(1);
                self.rolling_hash
                    <= deterministic_exponential_threshold(u128::from(delta), u128::from(scale))
            };
            if boundary {
                self.reset();
            }
            boundary
        }

        fn reset(&mut self) {
            self.entries = 0;
            self.logical_bytes = 0;
            self.encoded_bytes = 0;
            self.previous_measure = 0;
            self.rolling_window.clear();
            self.rolling_hash = 0;
        }
    }

    #[test]
    fn byte_hash_cache_matches_direct_hash_for_every_byte() {
        for seed in [0, 1, u64::MAX] {
            let mut cache = ByteHashCache::new(seed);
            for byte in u8::MIN..=u8::MAX {
                assert_eq!(cache.get(byte), byte_hash(seed, byte));
                assert_eq!(cache.get(byte), byte_hash(seed, byte));
            }
        }
    }

    #[test]
    fn rolling_window_matches_vec_deque_across_wraps_and_reset() {
        let mut window = RollingWindow::new(7);
        let mut reference = VecDeque::new();
        for byte in 0_u8..100 {
            let expected = if reference.len() == 7 {
                reference.pop_front()
            } else {
                None
            };
            reference.push_back(byte);
            assert_eq!(window.push(byte), expected);
        }

        window.clear();
        reference.clear();
        for byte in (100_u8..180).rev() {
            let expected = if reference.len() == 7 {
                reference.pop_front()
            } else {
                None
            };
            reference.push_back(byte);
            assert_eq!(window.push(byte), expected);
        }
    }

    #[test]
    fn optimized_rolling_detector_matches_vec_deque_reference() {
        let spec = ChunkingSpec {
            measure: ChunkMeasure::LogicalBytes,
            input: BoundaryInput::KeyValue,
            rule: BoundaryRule::RollingBuzHash { window: 17 },
            min: 32,
            target: 96,
            max: 256,
            hash_seed: 0xfeed_cafe_dead_beef,
            hard_max_node_bytes: 512,
            ..ChunkingSpec::default()
        };
        let mut optimized = BoundaryDetector::new(spec.clone(), 3).unwrap();
        let mut reference = ReferenceRollingDetector::new(spec, 3);
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;

        for index in 0..2_000 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let key_len = (state as usize % 31) + 1;
            let value_len = ((state >> 8) as usize % 67) + 1;
            let mut key = vec![0; key_len];
            let mut value = vec![0; value_len];
            for byte in key.iter_mut().chain(value.iter_mut()) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            let encoded = key_len + value_len + 9;

            assert_eq!(
                optimized.observe(&key, &value, encoded).unwrap(),
                reference.observe(&key, &value, encoded),
                "boundary mismatch at entry {index}"
            );
            assert_eq!(optimized.rolling_hash, reference.rolling_hash);
            assert_eq!(optimized.entries, reference.entries);
            assert_eq!(optimized.logical_bytes, reference.logical_bytes);
            assert_eq!(optimized.encoded_bytes, reference.encoded_bytes);

            if index % 53 == 0 {
                optimized.reset();
                reference.reset();
            }
        }
    }

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
}
