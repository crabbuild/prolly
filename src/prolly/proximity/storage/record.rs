use super::codec::{put_bytes, put_varint, Reader, FORMAT_VERSION, VECTOR_ENCODING_F32_LE};
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::prepare_vector;
use crate::prolly::proximity::vector::{decode_components, encode_components};
use crate::prolly::proximity::DistanceMetric;

const MAGIC: &[u8; 4] = b"PRVR";

#[derive(Clone, Copy, Debug)]
pub(crate) struct EncodedVectorRef<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) dimensions: u32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StoredRecordRef<'a> {
    pub(crate) vector: EncodedVectorRef<'a>,
    pub(crate) value: &'a [u8],
}

impl<'a> StoredRecordRef<'a> {
    pub(crate) fn decode(bytes: &'a [u8], dimensions: u32) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "record");
        reader.exact(MAGIC)?;
        reader.version()?;
        if reader.u8()? != VECTOR_ENCODING_F32_LE {
            return Err(reader.invalid("unsupported vector encoding"));
        }
        if reader.varint()? != u64::from(dimensions) {
            return Err(reader.invalid("dimension mismatch"));
        }
        let vector_bytes = usize::try_from(dimensions)
            .ok()
            .and_then(|value| value.checked_mul(4))
            .ok_or_else(|| reader.invalid("vector length overflow"))?;
        let vector = reader.take(vector_bytes)?;
        for component in vector.chunks_exact(4) {
            let value = f32::from_bits(u32::from_le_bytes(
                component.try_into().expect("four-byte vector component"),
            ));
            if !value.is_finite() || value.to_bits() == 0x8000_0000 {
                return Err(reader.invalid("non-canonical f32"));
            }
        }
        let value_len = reader.bounded_usize(reader.remaining())?;
        let value = reader.take(value_len)?;
        reader.finish()?;
        Ok(Self {
            vector: EncodedVectorRef {
                bytes: vector,
                dimensions,
            },
            value,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredRecord {
    pub(crate) vector: Vec<f32>,
    pub(crate) value: Vec<u8>,
}

impl StoredRecord {
    pub(crate) fn new(
        vector: &[f32],
        value: Vec<u8>,
        metric: DistanceMetric,
        dimensions: u32,
    ) -> Result<Self, Error> {
        Ok(Self {
            vector: prepare_vector(metric, vector, dimensions)?,
            value,
        })
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(9 + self.vector.len() * 4 + self.value.len());
        bytes.extend_from_slice(MAGIC);
        bytes.push(FORMAT_VERSION);
        bytes.push(VECTOR_ENCODING_F32_LE);
        put_varint(self.vector.len() as u64, &mut bytes);
        encode_components(&self.vector, &mut bytes);
        put_bytes(&self.value, &mut bytes);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], dimensions: u32) -> Result<Self, Error> {
        let record = StoredRecordRef::decode(bytes, dimensions)?;
        Ok(Self {
            vector: decode_components(record.vector.bytes, dimensions)?,
            value: record.value.to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_is_current_format_and_strict() {
        let record = StoredRecord::new(
            &[-0.0, 2.5],
            b"value".to_vec(),
            DistanceMetric::L2Squared,
            2,
        )
        .unwrap();
        let bytes = record.encode();
        assert_eq!(&bytes[..6], b"PRVR\x02\x01");
        assert_eq!(StoredRecord::decode(&bytes, 2).unwrap(), record);
        let mut trailing = bytes;
        trailing.push(0);
        assert!(StoredRecord::decode(&trailing, 2).is_err());
    }
}
