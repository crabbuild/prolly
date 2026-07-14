use super::canonical::reciprocal_sqrt;
use crate::prolly::error::Error;
use crate::prolly::proximity::DistanceMetric;

pub(crate) fn prepare_vector(
    metric: DistanceMetric,
    vector: &[f32],
    dimensions: u32,
) -> Result<Vec<f32>, Error> {
    let expected = usize::try_from(dimensions).map_err(|_| Error::InvalidProximityVector {
        reason: "dimensions exceed usize".to_owned(),
    })?;
    if vector.len() != expected {
        return Err(Error::InvalidProximityVector {
            reason: format!("expected {expected} dimensions, received {}", vector.len()),
        });
    }

    let mut prepared = Vec::with_capacity(vector.len());
    for (index, &component) in vector.iter().enumerate() {
        if !component.is_finite() {
            return Err(Error::InvalidProximityVector {
                reason: format!("component {index} is not finite"),
            });
        }
        prepared.push(if component == 0.0 { 0.0 } else { component });
    }

    if metric == DistanceMetric::Cosine {
        prepared = normalize_cosine_to_fixed_point(prepared)?;
    }
    Ok(prepared)
}

fn normalize_cosine_to_fixed_point(mut vector: Vec<f32>) -> Result<Vec<f32>, Error> {
    // Persisted cosine vectors must be safe to pass through ingestion again.
    // Iterating the f64-to-f32 projection to a fixed point gives that property
    // without tagging vectors or depending on their provenance.
    for _ in 0..16 {
        let norm_squared = dot(&vector, &vector);
        if norm_squared == 0.0 {
            return Err(Error::ZeroCosineVector);
        }
        let inverse_norm = reciprocal_sqrt(norm_squared);
        let next: Vec<f32> = vector
            .iter()
            .map(|component| {
                let normalized = (f64::from(*component) * inverse_norm) as f32;
                if normalized == 0.0 {
                    0.0
                } else {
                    normalized
                }
            })
            .collect();
        if next
            .iter()
            .zip(&vector)
            .all(|(left, right)| left.to_bits() == right.to_bits())
        {
            return Ok(next);
        }
        vector = next;
    }
    Err(Error::InvalidProximityVector {
        reason: "cosine normalization did not reach a canonical fixed point".to_owned(),
    })
}

pub(crate) fn score(metric: DistanceMetric, left: &[f32], right: &[f32]) -> f64 {
    debug_assert_eq!(left.len(), right.len());
    let result = match metric {
        DistanceMetric::L2Squared => left.iter().zip(right).fold(0.0, |sum, (&a, &b)| {
            let delta = f64::from(a) - f64::from(b);
            sum + delta * delta
        }),
        DistanceMetric::Cosine => 1.0 - dot(left, right).clamp(-1.0, 1.0),
        DistanceMetric::InnerProduct => -dot(left, right),
    };
    if result == 0.0 {
        0.0
    } else {
        result
    }
}

fn dot(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .fold(0.0, |sum, (&a, &b)| sum + f64::from(a) * f64::from(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_scores_have_stable_bits() {
        assert_eq!(
            score(DistanceMetric::L2Squared, &[1.0, 2.0], &[4.0, 6.0]),
            25.0
        );
        assert_eq!(
            score(DistanceMetric::InnerProduct, &[1.0, 2.0], &[2.0, 1.0]),
            -4.0
        );
        assert_eq!(
            score(DistanceMetric::InnerProduct, &[0.0], &[0.0]).to_bits(),
            0
        );
        let unit = prepare_vector(DistanceMetric::Cosine, &[3.0, 4.0], 2).unwrap();
        assert_eq!(score(DistanceMetric::Cosine, &unit, &unit).to_bits(), 0);
    }

    #[test]
    fn cosine_preparation_is_idempotent_for_adversarial_inputs() {
        let mut state = 0xd1b5_4a32_d192_ed03u64;
        for _ in 0..10_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let first_bits = state as u32;
            let first =
                f32::from_bits((first_bits & 0x807f_ffff) | (((first_bits >> 23) % 255) << 23));
            state = state.rotate_left(29).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            let second_bits = state as u32;
            let second =
                f32::from_bits((second_bits & 0x807f_ffff) | (((second_bits >> 23) % 255) << 23));
            let once = prepare_vector(DistanceMetric::Cosine, &[first, second], 2).unwrap();
            let twice = prepare_vector(DistanceMetric::Cosine, &once, 2).unwrap();
            assert_eq!(once, twice, "input=({first:?}, {second:?})");
        }
    }
}
