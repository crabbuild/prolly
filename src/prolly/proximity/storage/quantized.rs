use super::codec::{put_f32, put_varint, Reader, FORMAT_VERSION, MAX_OBJECT_ENTRIES};
use crate::prolly::error::Error;
use crate::prolly::proximity::DistanceMetric;

const MAGIC: &[u8; 4] = b"PQS8";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScalarQuantized {
    pub(crate) dimensions: u32,
    pub(crate) group_size: u32,
    pub(crate) entry_count: u64,
    pub(crate) scales: Vec<f32>,
    pub(crate) max_error: f32,
    pub(crate) values: Vec<i8>,
}

impl ScalarQuantized {
    pub(crate) fn build(
        vectors: &[&[f32]],
        dimensions: u32,
        group_size: u32,
    ) -> Result<Self, Error> {
        if dimensions == 0 || group_size == 0 {
            return Err(invalid("dimensions and group size must be non-zero"));
        }
        let dimensions_usize = dimensions as usize;
        if vectors
            .iter()
            .any(|vector| vector.len() != dimensions_usize)
        {
            return Err(invalid("source vector dimension mismatch"));
        }
        let groups = dimensions.div_ceil(group_size) as usize;
        let mut scales = vec![0.0f32; groups];
        for vector in vectors {
            for (index, &component) in vector.iter().enumerate() {
                if !component.is_finite() {
                    return Err(invalid("source vector contains a non-finite component"));
                }
                let group = index / group_size as usize;
                scales[group] = scales[group].max(component.abs());
            }
        }
        for scale in &mut scales {
            if *scale != 0.0 {
                *scale /= 127.0;
            }
        }

        let value_count = vectors
            .len()
            .checked_mul(dimensions_usize)
            .ok_or_else(|| invalid("quantized value length overflow"))?;
        let mut values = Vec::with_capacity(value_count);
        let mut max_error = 0.0f32;
        for vector in vectors {
            for (index, &component) in vector.iter().enumerate() {
                let scale = scales[index / group_size as usize];
                let quantized = quantize_component(component, scale);
                let reconstructed = f32::from(quantized) * scale;
                max_error = max_error.max((component - reconstructed).abs());
                values.push(quantized);
            }
        }
        let object = Self {
            dimensions,
            group_size,
            entry_count: vectors.len() as u64,
            scales,
            max_error,
            values,
        };
        object.validate()?;
        Ok(object)
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(FORMAT_VERSION);
        bytes.push(0);
        put_varint(u64::from(self.dimensions), &mut bytes);
        put_varint(u64::from(self.group_size), &mut bytes);
        put_varint(self.entry_count, &mut bytes);
        put_varint(self.scales.len() as u64, &mut bytes);
        for &scale in &self.scales {
            put_f32(scale, &mut bytes)?;
        }
        put_f32(self.max_error, &mut bytes)?;
        bytes.extend(self.values.iter().map(|value| *value as u8));
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "quantizer");
        reader.exact(MAGIC)?;
        reader.version()?;
        if reader.u8()? != 0 {
            return Err(reader.invalid("unknown flags"));
        }
        let dimensions =
            u32::try_from(reader.varint()?).map_err(|_| reader.invalid("dimensions exceed u32"))?;
        let group_size = u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("group size exceeds u32"))?;
        let entry_count = reader.varint()?;
        let scale_count = reader.bounded_usize(MAX_OBJECT_ENTRIES)?;
        if scale_count
            .checked_mul(4)
            .map_or(true, |len| len > reader.remaining())
        {
            return Err(reader.invalid("impossible scale length"));
        }
        let mut scales = Vec::with_capacity(scale_count);
        for _ in 0..scale_count {
            scales.push(reader.f32()?);
        }
        let max_error = reader.f32()?;
        let value_count = usize::try_from(entry_count)
            .ok()
            .and_then(|count| count.checked_mul(dimensions as usize))
            .ok_or_else(|| reader.invalid("quantized value length overflow"))?;
        let values = reader
            .take(value_count)?
            .iter()
            .map(|byte| *byte as i8)
            .collect();
        reader.finish()?;
        let object = Self {
            dimensions,
            group_size,
            entry_count,
            scales,
            max_error,
            values,
        };
        object.validate()?;
        Ok(object)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.dimensions == 0 || self.group_size == 0 {
            return Err(Error::InvalidProximityObject {
                kind: "quantizer",
                reason: "dimensions and group size must be non-zero".to_owned(),
            });
        }
        let groups = self.dimensions.div_ceil(self.group_size) as usize;
        if self.scales.len() != groups
            || self
                .scales
                .iter()
                .any(|scale| !scale.is_finite() || *scale < 0.0)
            || !self.max_error.is_finite()
            || self.max_error < 0.0
        {
            return Err(Error::InvalidProximityObject {
                kind: "quantizer",
                reason: "invalid group scales".to_owned(),
            });
        }
        let expected = usize::try_from(self.entry_count)
            .ok()
            .and_then(|count| count.checked_mul(self.dimensions as usize));
        if expected != Some(self.values.len()) {
            return Err(Error::InvalidProximityObject {
                kind: "quantizer",
                reason: "quantized value count mismatch".to_owned(),
            });
        }
        if self.values.contains(&i8::MIN) {
            return Err(Error::InvalidProximityObject {
                kind: "quantizer",
                reason: "quantized values must be in -127..=127".to_owned(),
            });
        }
        Ok(())
    }

    pub(crate) fn verify(&self, vectors: &[&[f32]]) -> Result<(), Error> {
        let expected = Self::build(vectors, self.dimensions, self.group_size)?;
        if &expected != self {
            return Err(invalid(
                "quantizer parameters, values, or error bound disagree with node vectors",
            ));
        }
        Ok(())
    }

    pub(crate) fn approximate_score(
        &self,
        metric: DistanceMetric,
        query: &[f32],
        entry: usize,
    ) -> Result<f64, Error> {
        if query.len() != self.dimensions as usize || entry >= self.entry_count as usize {
            return Err(invalid("quantized score index or dimensions are invalid"));
        }
        let start = entry * self.dimensions as usize;
        let values = &self.values[start..start + self.dimensions as usize];
        let mut reduced = 0.0f64;
        for (index, (&query, &value)) in query.iter().zip(values).enumerate() {
            let reconstructed =
                f64::from(value) * f64::from(self.scales[index / self.group_size as usize]);
            if metric == DistanceMetric::L2Squared {
                let delta = f64::from(query) - reconstructed;
                reduced += delta * delta;
            } else {
                reduced += f64::from(query) * reconstructed;
            }
        }
        let score = match metric {
            DistanceMetric::L2Squared => reduced,
            DistanceMetric::Cosine => 1.0 - reduced.clamp(-1.0, 1.0),
            DistanceMetric::InnerProduct => -reduced,
        };
        Ok(if score == 0.0 { 0.0 } else { score })
    }
}

fn quantize_component(component: f32, scale: f32) -> i8 {
    if scale == 0.0 {
        return 0;
    }
    let rounded = (f64::from(component) / f64::from(scale)).round_ties_even();
    rounded.clamp(-127.0, 127.0) as i8
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "quantizer",
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_quantization_handles_zero_groups_ties_clamping_and_error() {
        let vectors: Vec<&[f32]> = vec![&[0.0, 0.0, 1.0, -1.0], &[0.0, 0.0, 0.5, -0.5]];
        let quantized = ScalarQuantized::build(&vectors, 4, 2).unwrap();
        assert_eq!(quantized.scales[0].to_bits(), 0);
        assert_eq!(quantized.values[0..2], [0, 0]);
        assert!(quantized.values.iter().all(|value| *value != i8::MIN));
        assert!(quantized.max_error >= 0.0);
        quantized.verify(&vectors).unwrap();

        assert_eq!(quantize_component(2.5, 1.0), 2);
        assert_eq!(quantize_component(3.5, 1.0), 4);
        assert_eq!(quantize_component(f32::MAX, f32::MIN_POSITIVE), 127);
        assert_eq!(quantize_component(-f32::MAX, f32::MIN_POSITIVE), -127);

        let symmetric = ScalarQuantized::build(&[&[-2.0, 1.0]], 2, 2).unwrap();
        assert_eq!(symmetric.scales, vec![2.0 / 127.0]);
        assert_eq!(symmetric.values, vec![-127, 64]);
        assert_eq!(
            symmetric.max_error.to_bits(),
            (1.0 - 64.0 * (2.0 / 127.0_f32)).abs().to_bits()
        );
    }
}
