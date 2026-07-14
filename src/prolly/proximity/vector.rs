use super::super::error::Error;
use xxhash_rust::xxh64::xxh64;

pub(crate) fn canonicalize(vector: &[f32], dimensions: u32) -> Result<Vec<f32>, Error> {
    let expected = usize::try_from(dimensions).map_err(|_| Error::InvalidProximityVector {
        reason: "dimensions exceed usize".to_owned(),
    })?;
    if vector.len() != expected {
        return Err(Error::InvalidProximityVector {
            reason: format!("expected {expected} dimensions, received {}", vector.len()),
        });
    }
    let mut result = Vec::with_capacity(vector.len());
    for (index, &component) in vector.iter().enumerate() {
        if !component.is_finite() {
            return Err(Error::InvalidProximityVector {
                reason: format!("component {index} is not finite"),
            });
        }
        result.push(if component == 0.0 { 0.0 } else { component });
    }
    Ok(result)
}

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

pub(crate) fn l2_squared(left: &[f32], right: &[f32]) -> f64 {
    left.iter().zip(right).fold(0.0f64, |sum, (&a, &b)| {
        let delta = f64::from(a) - f64::from(b);
        sum + delta * delta
    })
}

pub(crate) fn promotion_level(key: &[u8], log_chunk_size: u8, seed: u64) -> u8 {
    let zeros = xxh64(key, seed).leading_zeros() as u8;
    zeros / log_chunk_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_vectors_normalize_negative_zero_and_reject_non_finite_values() {
        let vector = canonicalize(&[-0.0, 1.5], 2).unwrap();
        assert_eq!(vector[0].to_bits(), 0);
        assert!(canonicalize(&[f32::INFINITY], 1).is_err());
        assert!(canonicalize(&[f32::NAN], 1).is_err());
    }

    #[test]
    fn squared_l2_and_promotion_are_deterministic() {
        assert_eq!(l2_squared(&[1.0, 2.0], &[4.0, 6.0]), 25.0);
        assert_eq!(
            promotion_level(b"stable", 8, 42),
            promotion_level(b"stable", 8, 42)
        );
    }
}
