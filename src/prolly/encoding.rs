//! Encoding types and default constants for Prolly Trees

use serde::{Deserialize, Serialize};

/// Initial (leaf) level from which the prolly tree is built
pub const INIT_LEVEL: u8 = 0;

/// Default seed for the hash function
pub const DEFAULT_HASH_SEED: u64 = 0;

/// Default minimum entries before considering chunk boundary
pub const DEFAULT_MIN_CHUNK_SIZE: usize = 4;

/// Default maximum number of key-value pairs in a node
pub const DEFAULT_MAX_CHUNK_SIZE: usize = 1024 * 1024;

/// Default chunking factor: 128 = ~0.78% boundary probability
pub const DEFAULT_CHUNKING_FACTOR: u32 = 128;

/// Encoding type for values
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Encoding {
    #[default]
    Raw,
    Cbor,
    Json,
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_default() {
        let encoding = Encoding::default();
        assert_eq!(encoding, Encoding::Raw);
    }

    #[test]
    fn test_encoding_custom() {
        let encoding = Encoding::Custom("protobuf".to_string());
        assert_eq!(encoding, Encoding::Custom("protobuf".to_string()));
    }

    #[test]
    fn test_constants() {
        assert_eq!(INIT_LEVEL, 0);
        assert_eq!(DEFAULT_HASH_SEED, 0);
        assert_eq!(DEFAULT_MIN_CHUNK_SIZE, 4);
        assert_eq!(DEFAULT_MAX_CHUNK_SIZE, 1024 * 1024);
        assert_eq!(DEFAULT_CHUNKING_FACTOR, 128);
    }
}
