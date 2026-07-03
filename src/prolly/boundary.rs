//! Boundary detection for Prolly Tree chunking
//!
//! Determines where nodes should split based on content hashing.
//! Uses xxHash64 for fast, deterministic boundary detection.

use std::hash::Hasher;
use xxhash_rust::xxh64::Xxh64;

use super::config::Config;
use super::node::Node;

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
    if count < node.min_chunk_size {
        return false;
    }

    // At or above max size: always split
    if count >= node.max_chunk_size {
        return true;
    }

    is_hash_boundary(
        node.hash_seed,
        node.chunking_factor,
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
    if count < config.min_chunk_size {
        return false;
    }

    // At or above max size: always split
    if count >= config.max_chunk_size {
        return true;
    }

    is_hash_boundary_config(config, key, val)
}

/// Check only the hash predicate for a boundary, without applying min/max size rules.
///
/// Bulk builders can precompute this part in parallel, then apply the min/max
/// checks using the current chunk-local entry count.
pub(crate) fn is_hash_boundary_config(config: &Config, key: &[u8], val: &[u8]) -> bool {
    is_hash_boundary(config.hash_seed, config.chunking_factor, key, val)
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
