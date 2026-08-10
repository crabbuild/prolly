use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, ErrorCode, Result};

/// Encode a struct-only schema using serde CBOR's packed deterministic form.
pub fn encode_canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_cbor::ser::to_vec_packed(value)
        .map_err(|error| Error::serialization(format!("canonical encode failed: {error}")))
}

/// Decode and reject alternate encodings of the same value.
pub fn decode_canonical<T>(bytes: &[u8]) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    let value: T = serde_cbor::from_slice(bytes).map_err(|error| {
        Error::new(
            ErrorCode::CorruptCommit,
            format!("canonical decode failed: {error}"),
        )
    })?;
    let encoded = encode_canonical(&value)?;
    if encoded != bytes {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "noncanonical CBOR encoding",
        ));
    }
    Ok(value)
}

pub(crate) fn domain_hash(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u32).to_be_bytes());
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
