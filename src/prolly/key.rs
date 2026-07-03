//! Helpers for building lexicographic byte keys.
//!
//! Prolly trees order raw byte keys lexicographically. This module provides
//! small helpers for common application key layouts: prefix scans, ordered
//! numeric encodings, segment-safe composite keys, and readable debug output.

/// Return the smallest exclusive upper bound for all keys with `prefix`.
///
/// Returns `None` when the prefix covers the rest of the keyspace, which is
/// true for the empty prefix and for prefixes made entirely of `0xff` bytes.
///
/// # Example
/// ```
/// use prolly::prefix_end;
///
/// assert_eq!(prefix_end(b"user/42/"), Some(b"user/420".to_vec()));
/// assert_eq!(prefix_end(b""), None);
/// ```
pub fn prefix_end(prefix: impl AsRef<[u8]>) -> Option<Vec<u8>> {
    let prefix = prefix.as_ref();
    if prefix.is_empty() {
        return None;
    }

    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last == u8::MAX {
            end.pop();
        } else {
            *last += 1;
            return Some(end);
        }
    }
    None
}

/// Return the half-open byte range covering all keys with `prefix`.
///
/// Use the returned `(start, end)` with [`crate::Prolly::range`] or
/// [`crate::Prolly::range_diff`].
pub fn prefix_range(prefix: impl AsRef<[u8]>) -> (Vec<u8>, Option<Vec<u8>>) {
    let start = prefix.as_ref().to_vec();
    let end = prefix_end(&start);
    (start, end)
}

/// Encode a `u64` so lexicographic byte ordering matches numeric ordering.
pub fn u64_key(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Encode a `u128` so lexicographic byte ordering matches numeric ordering.
pub fn u128_key(value: u128) -> [u8; 16] {
    value.to_be_bytes()
}

/// Encode an `i64` so lexicographic byte ordering matches numeric ordering.
pub fn i64_key(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1u64 << 63)).to_be_bytes()
}

/// Encode an `i128` so lexicographic byte ordering matches numeric ordering.
pub fn i128_key(value: i128) -> [u8; 16] {
    ((value as u128) ^ (1u128 << 127)).to_be_bytes()
}

/// Encode a Unix timestamp in milliseconds using unsigned lexicographic order.
pub fn timestamp_millis_key(value: u64) -> [u8; 8] {
    u64_key(value)
}

/// Builder for segment-safe composite keys.
///
/// Segments are escaped and terminated so component boundaries remain
/// unambiguous while preserving byte ordering within each segment.
///
/// # Example
/// ```
/// use prolly::{prefix_range, KeyBuilder};
///
/// let prefix = KeyBuilder::new()
///     .push_str("conversation")
///     .push_str("c42")
///     .finish();
/// let (start, end) = prefix_range(&prefix);
///
/// let message_key = KeyBuilder::from_prefix(prefix)
///     .push_u64(7)
///     .finish();
/// assert!(message_key >= start);
/// assert!(end.as_ref().map_or(true, |end| message_key < *end));
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyBuilder {
    bytes: Vec<u8>,
}

impl KeyBuilder {
    /// Create an empty key builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an empty key builder with reserved capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    /// Continue building from an existing prefix.
    pub fn from_prefix(prefix: impl Into<Vec<u8>>) -> Self {
        Self {
            bytes: prefix.into(),
        }
    }

    /// Append raw bytes without segment escaping.
    ///
    /// Prefer [`KeyBuilder::push_segment`] for tuple-like components.
    pub fn push_raw(mut self, bytes: impl AsRef<[u8]>) -> Self {
        self.bytes.extend_from_slice(bytes.as_ref());
        self
    }

    /// Append one escaped byte segment.
    pub fn push_segment(mut self, segment: impl AsRef<[u8]>) -> Self {
        encode_segment_into(segment.as_ref(), &mut self.bytes);
        self
    }

    /// Append one UTF-8 string segment.
    pub fn push_str(self, segment: &str) -> Self {
        self.push_segment(segment.as_bytes())
    }

    /// Append one lexicographic `u64` segment.
    pub fn push_u64(self, value: u64) -> Self {
        self.push_segment(u64_key(value))
    }

    /// Append one lexicographic `u128` segment.
    pub fn push_u128(self, value: u128) -> Self {
        self.push_segment(u128_key(value))
    }

    /// Append one lexicographic `i64` segment.
    pub fn push_i64(self, value: i64) -> Self {
        self.push_segment(i64_key(value))
    }

    /// Append one lexicographic `i128` segment.
    pub fn push_i128(self, value: i128) -> Self {
        self.push_segment(i128_key(value))
    }

    /// Append one Unix-millisecond timestamp segment.
    pub fn push_timestamp_millis(self, value: u64) -> Self {
        self.push_segment(timestamp_millis_key(value))
    }

    /// Borrow the bytes built so far.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    /// Finish and return the key bytes.
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Encode one segment for use in a composite key.
pub fn encode_segment(segment: impl AsRef<[u8]>) -> Vec<u8> {
    let mut out = Vec::with_capacity(segment.as_ref().len() + 2);
    encode_segment_into(segment.as_ref(), &mut out);
    out
}

/// Decode a composite key built from escaped segments.
pub fn decode_segments(key: &[u8]) -> Result<Vec<Vec<u8>>, KeyDecodeError> {
    let mut segments = Vec::new();
    let mut current = Vec::new();
    let mut offset = 0usize;

    while offset < key.len() {
        let byte = key[offset];
        if byte != 0 {
            current.push(byte);
            offset += 1;
            continue;
        }

        let Some(marker) = key.get(offset + 1).copied() else {
            return Err(KeyDecodeError::UnexpectedEnd { offset });
        };

        match marker {
            0x00 => {
                segments.push(std::mem::take(&mut current));
                offset += 2;
            }
            0xff => {
                current.push(0);
                offset += 2;
            }
            marker => {
                return Err(KeyDecodeError::InvalidEscape { offset, marker });
            }
        }
    }

    if current.is_empty() {
        Ok(segments)
    } else {
        Err(KeyDecodeError::UnexpectedEnd { offset: key.len() })
    }
}

/// Error returned by [`decode_segments`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeyDecodeError {
    /// The key ended inside a segment or escape sequence.
    UnexpectedEnd { offset: usize },
    /// A zero byte was followed by an unsupported escape marker.
    InvalidEscape { offset: usize, marker: u8 },
}

impl std::fmt::Display for KeyDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEnd { offset } => {
                write!(f, "encoded key ended unexpectedly at byte offset {offset}")
            }
            Self::InvalidEscape { offset, marker } => write!(
                f,
                "invalid encoded key escape at byte offset {offset}: 0x{marker:02x}"
            ),
        }
    }
}

impl std::error::Error for KeyDecodeError {}

/// Format arbitrary key bytes with printable ASCII and `\xNN` escapes.
pub fn debug_key(key: &[u8]) -> String {
    let mut out = String::with_capacity(key.len().saturating_add(2));
    out.push('"');
    for byte in key {
        match *byte {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(*byte as char),
            byte => out.push_str(&format!("\\x{byte:02x}")),
        }
    }
    out.push('"');
    out
}

fn encode_segment_into(segment: &[u8], out: &mut Vec<u8>) {
    for byte in segment {
        if *byte == 0 {
            out.extend_from_slice(&[0x00, 0xff]);
        } else {
            out.push(*byte);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_end_handles_empty_carry_and_unbounded_cases() {
        assert_eq!(prefix_end(b""), None);
        assert_eq!(prefix_end(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_end([0x12, 0xff]), Some(vec![0x13]));
        assert_eq!(prefix_end([0xff, 0xff]), None);
    }

    #[test]
    fn signed_and_unsigned_numeric_keys_sort_by_value() {
        let mut signed = vec![i64::MAX, 0, -1, i64::MIN, 42, -42];
        signed.sort_by_key(|value| i64_key(*value));
        assert_eq!(signed, vec![i64::MIN, -42, -1, 0, 42, i64::MAX]);

        let mut unsigned = vec![u64::MAX, 0, 9, 1, 1_000_000];
        unsigned.sort_by_key(|value| u64_key(*value));
        assert_eq!(unsigned, vec![0, 1, 9, 1_000_000, u64::MAX]);
    }

    #[test]
    fn escaped_segments_round_trip_and_preserve_byte_order() {
        let segments = vec![b"tenant".to_vec(), vec![0, 1, 0xff], Vec::new()];
        let mut key = Vec::new();
        for segment in &segments {
            key.extend(encode_segment(segment));
        }
        assert_eq!(decode_segments(&key).unwrap(), segments);

        let mut ordered = [
            KeyBuilder::new().push_segment(b"a\0").finish(),
            KeyBuilder::new().push_segment(b"").finish(),
            KeyBuilder::new().push_segment(b"a").finish(),
            KeyBuilder::new().push_segment(b"aa").finish(),
        ];
        ordered.sort();
        let decoded = ordered
            .iter()
            .map(|key| decode_segments(key).unwrap().remove(0))
            .collect::<Vec<_>>();
        assert_eq!(
            decoded,
            vec![b"".to_vec(), b"a".to_vec(), b"a\0".to_vec(), b"aa".to_vec()]
        );
    }

    #[test]
    fn decode_segments_rejects_partial_segments_and_bad_escapes() {
        assert_eq!(
            decode_segments(b"abc"),
            Err(KeyDecodeError::UnexpectedEnd { offset: 3 })
        );
        assert_eq!(
            decode_segments(&[0, 7]),
            Err(KeyDecodeError::InvalidEscape {
                offset: 0,
                marker: 7
            })
        );
    }

    #[test]
    fn debug_key_escapes_non_printable_bytes() {
        assert_eq!(debug_key(b"a\n\0\\\""), "\"a\\n\\x00\\\\\\\"\"");
    }
}
