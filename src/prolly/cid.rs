//! Content Identifier (CID) - 32-byte SHA-256 hash of node content

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Content identifier - a 32-byte SHA-256 hash
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Cid(pub [u8; 32]);

impl Cid {
    /// Compute CID from bytes using SHA-256
    pub fn from_bytes(data: &[u8]) -> Self {
        let hash = Sha256::digest(data);
        Cid(hash.into())
    }

    /// Get raw bytes of the CID
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cid_from_bytes() {
        let data = b"hello world";
        let cid = Cid::from_bytes(data);
        assert_eq!(cid.as_bytes().len(), 32);
    }

    #[test]
    fn test_cid_deterministic() {
        let data = b"test data";
        let cid1 = Cid::from_bytes(data);
        let cid2 = Cid::from_bytes(data);
        assert_eq!(cid1, cid2);
    }

    #[test]
    fn test_cid_different_data() {
        let cid1 = Cid::from_bytes(b"data1");
        let cid2 = Cid::from_bytes(b"data2");
        assert_ne!(cid1, cid2);
    }
}
