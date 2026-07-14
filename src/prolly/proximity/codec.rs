use super::super::error::Error;

pub(crate) fn put_varint(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
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

    fn invalid(&self, reason: impl Into<String>) -> Error {
        Error::InvalidProximityObject {
            kind: self.kind,
            reason: reason.into(),
        }
    }

    pub(crate) fn exact(&mut self, expected: &[u8]) -> Result<(), Error> {
        let bytes = self.take(expected.len())?;
        if bytes != expected {
            return Err(self.invalid("invalid magic"));
        }
        Ok(())
    }

    pub(crate) fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u64_le(&mut self) -> Result<u64, Error> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| self.invalid("truncated u64"))?;
        Ok(u64::from_le_bytes(bytes))
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
                let mut canonical = Vec::new();
                put_varint(value, &mut canonical);
                if canonical.as_slice() != &self.bytes[start..self.offset] {
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

    pub(crate) fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| self.invalid("length overflow"))?;
        if end > self.bytes.len() {
            return Err(self.invalid("truncated object"));
        }
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
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
    fn varint_decoder_rejects_truncation_overflow_and_non_canonical_bytes() {
        for bytes in [
            vec![0x80],
            vec![0x80, 0x00],
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02],
        ] {
            assert!(Reader::new(&bytes, "test").varint().is_err());
        }
    }

    #[test]
    fn canonical_varints_round_trip_boundary_values() {
        for value in [0, 1, 127, 128, 16_383, 16_384, u64::MAX] {
            let mut bytes = Vec::new();
            put_varint(value, &mut bytes);
            let mut reader = Reader::new(&bytes, "test");
            assert_eq!(reader.varint().unwrap(), value);
            reader.finish().unwrap();
        }
    }
}
