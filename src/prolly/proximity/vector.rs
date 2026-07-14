use super::super::error::Error;
use xxhash_rust::xxh64::xxh64;

pub(crate) fn encode_components(vector: &[f32], out: &mut Vec<u8>) {
    for component in vector {
        out.extend_from_slice(&component.to_bits().to_le_bytes());
    }
}

pub(crate) fn decode_components(bytes: &[u8], dimensions: u32) -> Result<Vec<f32>, Error> {
    let count = usize::try_from(dimensions).map_err(|_| Error::InvalidProximityVector {
        reason: "dimensions exceed usize".to_owned(),
    })?;
    let expected = count
        .checked_mul(4)
        .ok_or_else(|| Error::InvalidProximityVector {
            reason: "vector byte length overflow".to_owned(),
        })?;
    if bytes.len() != expected {
        return Err(Error::InvalidProximityVector {
            reason: format!("expected {expected} vector bytes, received {}", bytes.len()),
        });
    }
    let mut vector = Vec::with_capacity(count);
    for (index, chunk) in bytes.chunks_exact(4).enumerate() {
        let bits = u32::from_le_bytes(chunk.try_into().expect("four-byte chunk"));
        let component = f32::from_bits(bits);
        if !component.is_finite() || bits == 0x8000_0000 {
            return Err(Error::InvalidProximityVector {
                reason: format!("component {index} is non-canonical"),
            });
        }
        vector.push(component);
    }
    Ok(vector)
}

pub(crate) fn promotion_level(key: &[u8], log_chunk_size: u8, seed: u64) -> u8 {
    let zeros = xxh64(key, seed).leading_zeros() as u8;
    zeros / log_chunk_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn promotion_is_deterministic() {
        assert_eq!(
            promotion_level(b"stable", 8, 42),
            promotion_level(b"stable", 8, 42)
        );
    }
}
