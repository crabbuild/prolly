use super::codec::{put_f32, put_varint, Reader, FORMAT_VERSION, MAX_OBJECT_ENTRIES};
use crate::prolly::error::Error;

const MAGIC: &[u8; 4] = b"PQS8";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ScalarQuantized {
    pub(crate) dimensions: u32,
    pub(crate) group_size: u32,
    pub(crate) entry_count: u64,
    pub(crate) scales: Vec<f32>,
    pub(crate) values: Vec<i8>,
}

impl ScalarQuantized {
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
        if self.scales.len() != groups || self.scales.iter().any(|scale| *scale < 0.0) {
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
}
