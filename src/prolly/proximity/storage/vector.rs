use super::codec::{put_f32, put_f64, put_varint, Reader, FORMAT_VERSION, VECTOR_ENCODING_F32_LE};
use crate::prolly::error::Error;

const MAGIC: &[u8; 4] = b"PRXV";
const HAS_NORM: u8 = 1;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExternalVector {
    pub(crate) vector: Vec<f32>,
    pub(crate) norm: Option<f64>,
}

impl ExternalVector {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(FORMAT_VERSION);
        bytes.push(VECTOR_ENCODING_F32_LE);
        bytes.push(if self.norm.is_some() { HAS_NORM } else { 0 });
        put_varint(self.vector.len() as u64, &mut bytes);
        for &component in &self.vector {
            put_f32(component, &mut bytes)?;
        }
        if let Some(norm) = self.norm {
            if norm < 0.0 {
                return Err(Error::InvalidProximityObject {
                    kind: "vector",
                    reason: "norm must be non-negative".to_owned(),
                });
            }
            put_f64(norm, &mut bytes)?;
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "vector");
        reader.exact(MAGIC)?;
        reader.version()?;
        if reader.u8()? != VECTOR_ENCODING_F32_LE {
            return Err(reader.invalid("unsupported vector encoding"));
        }
        let flags = reader.u8()?;
        if flags & !HAS_NORM != 0 {
            return Err(reader.invalid("unknown flags"));
        }
        let dimensions = reader.bounded_usize(super::codec::MAX_OBJECT_ENTRIES)?;
        if dimensions
            .checked_mul(4)
            .map_or(true, |len| len > reader.remaining())
        {
            return Err(reader.invalid("impossible vector length"));
        }
        let mut vector = Vec::with_capacity(dimensions);
        for _ in 0..dimensions {
            vector.push(reader.f32()?);
        }
        let norm = if flags & HAS_NORM != 0 {
            let norm = reader.f64()?;
            if norm < 0.0 {
                return Err(reader.invalid("norm must be non-negative"));
            }
            Some(norm)
        } else {
            None
        };
        reader.finish()?;
        Ok(Self { vector, norm })
    }
}
