use crate::prolly::cid::Cid;
use crate::prolly::error::Error;

pub(crate) const FORMAT_VERSION: u8 = 2;
pub(crate) const VECTOR_ENCODING_F32_LE: u8 = 1;
pub(crate) const MAX_OBJECT_ENTRIES: usize = 16_777_216;
pub(crate) const MAX_KEY_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn put_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

pub(crate) fn put_bytes(value: &[u8], out: &mut Vec<u8>) {
    put_varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

pub(crate) fn put_cid(cid: &Cid, out: &mut Vec<u8>) {
    out.extend_from_slice(cid.as_bytes());
}

pub(crate) fn put_f32(value: f32, out: &mut Vec<u8>) -> Result<(), Error> {
    if !value.is_finite() || value.to_bits() == 0x8000_0000 {
        return Err(Error::InvalidProximityObject {
            kind: "float",
            reason: "non-canonical f32".to_owned(),
        });
    }
    out.extend_from_slice(&value.to_bits().to_le_bytes());
    Ok(())
}

pub(crate) fn put_f64(value: f64, out: &mut Vec<u8>) -> Result<(), Error> {
    if !value.is_finite() || value.to_bits() == 0x8000_0000_0000_0000 {
        return Err(Error::InvalidProximityObject {
            kind: "float",
            reason: "non-canonical f64".to_owned(),
        });
    }
    out.extend_from_slice(&value.to_bits().to_le_bytes());
    Ok(())
}

pub(crate) fn require_version(found: u8) -> Result<(), Error> {
    if found == FORMAT_VERSION {
        Ok(())
    } else {
        Err(Error::UnsupportedProximityVersion {
            found,
            required: FORMAT_VERSION,
        })
    }
}

pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
    kind: &'static str,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8], kind: &'static str) -> Self {
        Self {
            bytes,
            offset: 0,
            kind,
        }
    }

    pub(crate) fn invalid(&self, reason: impl Into<String>) -> Error {
        Error::InvalidProximityObject {
            kind: self.kind,
            reason: reason.into(),
        }
    }

    pub(crate) fn exact(&mut self, expected: &[u8]) -> Result<(), Error> {
        if self.take(expected.len())? != expected {
            return Err(self.invalid("invalid magic"));
        }
        Ok(())
    }

    pub(crate) fn version(&mut self) -> Result<(), Error> {
        require_version(self.u8()?)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u64_le(&mut self) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight-byte slice"),
        ))
    }

    pub(crate) fn f32(&mut self) -> Result<f32, Error> {
        let value = f32::from_bits(u32::from_le_bytes(
            self.take(4)?.try_into().expect("four-byte slice"),
        ));
        if !value.is_finite() || value.to_bits() == 0x8000_0000 {
            return Err(self.invalid("non-canonical f32"));
        }
        Ok(value)
    }

    pub(crate) fn f64(&mut self) -> Result<f64, Error> {
        let value = f64::from_bits(u64::from_le_bytes(
            self.take(8)?.try_into().expect("eight-byte slice"),
        ));
        if !value.is_finite() || value.to_bits() == 0x8000_0000_0000_0000 {
            return Err(self.invalid("non-canonical f64"));
        }
        Ok(value)
    }

    pub(crate) fn cid(&mut self) -> Result<Cid, Error> {
        Ok(Cid(self
            .take(32)?
            .try_into()
            .expect("thirty-two-byte slice")))
    }

    pub(crate) fn varint(&mut self) -> Result<u64, Error> {
        let start = self.offset;
        let mut value = 0u64;
        for shift in (0..=63).step_by(7) {
            let byte = self.u8()?;
            if shift == 63 && byte > 1 {
                return Err(self.invalid("varint overflow"));
            }
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                let canonical_len = if value == 0 {
                    1
                } else {
                    ((64 - value.leading_zeros()) as usize).div_ceil(7)
                };
                if self.offset - start != canonical_len {
                    return Err(self.invalid("non-canonical varint"));
                }
                return Ok(value);
            }
        }
        Err(self.invalid("varint overflow"))
    }

    pub(crate) fn usize(&mut self) -> Result<usize, Error> {
        usize::try_from(self.varint()?).map_err(|_| self.invalid("length exceeds usize"))
    }

    pub(crate) fn bounded_usize(&mut self, maximum: usize) -> Result<usize, Error> {
        let value = self.usize()?;
        if value > maximum {
            return Err(self.invalid(format!("length {value} exceeds limit {maximum}")));
        }
        Ok(value)
    }

    pub(crate) fn bytes(&mut self, maximum: usize) -> Result<Vec<u8>, Error> {
        let len = self.bounded_usize(maximum)?;
        Ok(self.take(len)?.to_vec())
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| self.invalid("length overflow"))?;
        if end > self.bytes.len() {
            return Err(self.invalid("truncated object"));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    pub(crate) fn finish(self) -> Result<(), Error> {
        if self.offset != self.bytes.len() {
            return Err(self.invalid("trailing bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_varints_round_trip_and_reject_bad_encodings() {
        for value in [0, 1, 127, 128, 16_383, 16_384, u64::MAX] {
            let mut bytes = Vec::new();
            put_varint(value, &mut bytes);
            let mut reader = Reader::new(&bytes, "test");
            assert_eq!(reader.varint().unwrap(), value);
            reader.finish().unwrap();
        }
        for bytes in [
            vec![0x80],
            vec![0x80, 0],
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2],
        ] {
            assert!(Reader::new(&bytes, "test").varint().is_err());
        }
    }
}
