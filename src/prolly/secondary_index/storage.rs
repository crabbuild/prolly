use super::super::error::Error;
use super::super::key::{decode_segments, encode_segment, encode_segment_prefix, prefix_end};

pub const INDEX_PHYSICAL_LAYOUT_VERSION: u32 = 1;
const INDEX_VALUE_FORMAT: u32 = 1;
const INDEX_VALUE_MAGIC: &[u8; 4] = b"PSIV";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexValue {
    KeysOnly,
    Included(Vec<u8>),
    FullSource(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexValueRef<'a> {
    KeysOnly,
    Included(&'a [u8]),
    FullSource(&'a [u8]),
}

impl<'a> IndexValueRef<'a> {
    pub fn decode(bytes: &'a [u8], max_payload_bytes: usize) -> Result<Self, Error> {
        if bytes.is_empty() {
            return Ok(Self::KeysOnly);
        }
        if bytes.len() < 13 || &bytes[..4] != INDEX_VALUE_MAGIC {
            return Err(Error::Deserialize(
                "invalid secondary-index value envelope".to_string(),
            ));
        }
        let version = u32::from_be_bytes(bytes[4..8].try_into().expect("fixed header"));
        if version != INDEX_VALUE_FORMAT {
            return Err(Error::Deserialize(
                "unsupported secondary-index value format".to_string(),
            ));
        }
        let payload_len =
            u32::from_be_bytes(bytes[9..13].try_into().expect("fixed header")) as usize;
        if payload_len > max_payload_bytes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "projection_bytes",
                limit: max_payload_bytes,
                actual: payload_len,
            });
        }
        if bytes.len() != 13usize.saturating_add(payload_len) {
            return Err(Error::Deserialize(
                "secondary-index value length mismatch or trailing bytes".to_string(),
            ));
        }
        match bytes[8] {
            1 => Ok(Self::Included(&bytes[13..])),
            2 => Ok(Self::FullSource(&bytes[13..])),
            _ => Err(Error::Deserialize(
                "unknown secondary-index value projection".to_string(),
            )),
        }
    }

    pub fn to_owned(self) -> IndexValue {
        match self {
            Self::KeysOnly => IndexValue::KeysOnly,
            Self::Included(value) => IndexValue::Included(value.to_vec()),
            Self::FullSource(value) => IndexValue::FullSource(value.to_vec()),
        }
    }
}

impl IndexValue {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let (tag, payload) = match self {
            Self::KeysOnly => return Ok(Vec::new()),
            Self::Included(payload) => (1, payload),
            Self::FullSource(payload) => (2, payload),
        };
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| Error::IndexResourceLimitExceeded {
                resource: "projection_bytes",
                limit: u32::MAX as usize,
                actual: payload.len(),
            })?;
        let mut bytes = Vec::with_capacity(13usize.saturating_add(payload.len()));
        bytes.extend_from_slice(INDEX_VALUE_MAGIC);
        bytes.extend_from_slice(&INDEX_VALUE_FORMAT.to_be_bytes());
        bytes.push(tag);
        bytes.extend_from_slice(&payload_len.to_be_bytes());
        bytes.extend_from_slice(payload);
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8], max_payload_bytes: usize) -> Result<Self, Error> {
        Ok(IndexValueRef::decode(bytes, max_payload_bytes)?.to_owned())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedPhysicalIndexKey {
    pub term: Vec<u8>,
    pub primary_key: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodedPhysicalIndexKeyRef<'a> {
    pub term: &'a [u8],
    pub primary_key: &'a [u8],
}

pub fn decode_physical_index_key_ref<'a>(
    key: &'a [u8],
    term_scratch: &'a mut Vec<u8>,
    primary_key_scratch: &'a mut Vec<u8>,
) -> Result<DecodedPhysicalIndexKeyRef<'a>, Error> {
    let mut offset = 0usize;
    let term = decode_physical_segment_ref(key, &mut offset, term_scratch)?;
    let primary_key = decode_physical_segment_ref(key, &mut offset, primary_key_scratch)?;
    if offset != key.len() {
        return Err(Error::Deserialize(
            "physical secondary-index key must contain exactly two segments".to_string(),
        ));
    }
    Ok(DecodedPhysicalIndexKeyRef { term, primary_key })
}

fn decode_physical_segment_ref<'a>(
    key: &'a [u8],
    offset: &mut usize,
    scratch: &'a mut Vec<u8>,
) -> Result<&'a [u8], Error> {
    let start = *offset;
    let mut cursor = start;
    let mut escaped = false;
    loop {
        let byte = *key.get(cursor).ok_or_else(|| {
            Error::Deserialize("unterminated physical secondary-index segment".to_string())
        })?;
        if byte != 0 {
            cursor += 1;
            continue;
        }
        let marker = *key.get(cursor + 1).ok_or_else(|| {
            Error::Deserialize("truncated physical secondary-index escape".to_string())
        })?;
        match marker {
            0 => {
                *offset = cursor + 2;
                if !escaped {
                    return Ok(&key[start..cursor]);
                }
                scratch.clear();
                let mut decode = start;
                while decode < cursor {
                    if key[decode] == 0 {
                        scratch.push(0);
                        decode += 2;
                    } else {
                        scratch.push(key[decode]);
                        decode += 1;
                    }
                }
                return Ok(scratch);
            }
            0xff => {
                escaped = true;
                cursor += 2;
            }
            _ => {
                return Err(Error::Deserialize(
                    "invalid physical secondary-index escape".to_string(),
                ))
            }
        }
    }
}

pub fn physical_index_key(term: &[u8], primary_key: &[u8]) -> Result<Vec<u8>, Error> {
    let capacity = term
        .len()
        .checked_mul(2)
        .and_then(|size| size.checked_add(primary_key.len().saturating_mul(2)))
        .and_then(|size| size.checked_add(4))
        .ok_or(Error::IndexResourceLimitExceeded {
            resource: "physical_key_bytes",
            limit: usize::MAX,
            actual: usize::MAX,
        })?;
    let mut key = Vec::with_capacity(capacity);
    key.extend_from_slice(&encode_segment(term));
    key.extend_from_slice(&encode_segment(primary_key));
    Ok(key)
}

pub fn decode_physical_index_key(key: &[u8]) -> Result<DecodedPhysicalIndexKey, Error> {
    let segments = decode_segments(key).map_err(|error| Error::Deserialize(error.to_string()))?;
    let [term, primary_key]: [Vec<u8>; 2] =
        segments.try_into().map_err(|segments: Vec<Vec<u8>>| {
            Error::Deserialize(format!(
                "physical secondary-index key must contain two segments, got {}",
                segments.len()
            ))
        })?;
    Ok(DecodedPhysicalIndexKey { term, primary_key })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TermBounds {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

pub fn term_bounds_exact(term: &[u8]) -> TermBounds {
    let start = encode_segment(term);
    let end = prefix_end(&start);
    TermBounds { start, end }
}

pub fn term_bounds_prefix(prefix: &[u8]) -> TermBounds {
    let start = encode_segment_prefix(prefix);
    let end = prefix_end(&start);
    TermBounds { start, end }
}

pub fn term_bounds_range(start_term: &[u8], end_term: Option<&[u8]>) -> Result<TermBounds, Error> {
    if end_term.is_some_and(|end| start_term > end) {
        return Err(Error::InvalidIndexDefinition {
            reason: "secondary-index term range start exceeds end".to_string(),
        });
    }
    Ok(TermBounds {
        start: encode_segment(start_term),
        end: end_term.map(encode_segment),
    })
}
