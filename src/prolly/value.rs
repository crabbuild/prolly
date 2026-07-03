//! Typed value encoding helpers.
//!
//! The core tree stores raw byte values. This module provides small helpers for
//! serializing typed application values into those bytes, plus a deterministic
//! versioned envelope for values that need schema and migration metadata.

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::encoding::Encoding;
use super::error::Error;

const VERSIONED_VALUE_MAGIC: &[u8; 4] = b"PLVV";
const VERSIONED_VALUE_WIRE_VERSION: u8 = 1;
const ENCODING_RAW: u8 = 0;
const ENCODING_CBOR: u8 = 1;
const ENCODING_JSON: u8 = 2;
const ENCODING_CUSTOM: u8 = 3;
const HEADER_LEN: usize = 4 + 1 + 1 + 8 + 4 + 4 + 8;

/// Codec for converting typed application values to and from stored bytes.
///
/// The core tree stores `Vec<u8>` leaf values. A `ValueCodec` is a small
/// reusable adapter that keeps application encode/decode policy next to the
/// schema that owns it.
pub trait ValueCodec {
    /// Encode a typed value into bytes suitable for storing in a tree.
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Error>;

    /// Decode a typed value from bytes read from a tree.
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Error>;
}

/// JSON codec for serde-backed application values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JsonCodec;

/// CBOR codec for serde-backed application values.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CborCodec;

/// Versioned JSON codec with schema/version validation on decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedJsonCodec {
    schema: String,
    version: u64,
}

/// Versioned CBOR codec with schema/version validation on decode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedCborCodec {
    schema: String,
    version: u64,
}

/// Serialize a typed value as compact JSON bytes.
pub fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(value).map_err(|err| Error::Serialize(err.to_string()))
}

/// Deserialize typed JSON value bytes.
pub fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(|err| Error::Deserialize(err.to_string()))
}

/// Serialize a typed value as compact CBOR bytes.
pub fn encode_cbor<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    serde_cbor::ser::to_vec_packed(value).map_err(|err| Error::Serialize(err.to_string()))
}

/// Deserialize typed CBOR value bytes.
pub fn decode_cbor<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_cbor::from_slice(bytes).map_err(|err| Error::Deserialize(err.to_string()))
}

impl ValueCodec for JsonCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Error> {
        encode_json(value)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Error> {
        decode_json(bytes)
    }
}

impl ValueCodec for CborCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Error> {
        encode_cbor(value)
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Error> {
        decode_cbor(bytes)
    }
}

impl VersionedJsonCodec {
    /// Create a JSON codec that wraps values in a [`VersionedValue`] envelope.
    pub fn new(schema: impl Into<String>, version: u64) -> Self {
        Self {
            schema: schema.into(),
            version,
        }
    }

    /// Schema expected by this codec.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Schema version expected by this codec.
    pub fn version(&self) -> u64 {
        self.version
    }
}

impl ValueCodec for VersionedJsonCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Error> {
        VersionedValue::json(&self.schema, self.version, value)?.to_bytes()
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Error> {
        let envelope = VersionedValue::from_bytes(bytes)?;
        envelope.require_schema(&self.schema, self.version)?;
        envelope.decode_json()
    }
}

impl VersionedCborCodec {
    /// Create a CBOR codec that wraps values in a [`VersionedValue`] envelope.
    pub fn new(schema: impl Into<String>, version: u64) -> Self {
        Self {
            schema: schema.into(),
            version,
        }
    }

    /// Schema expected by this codec.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Schema version expected by this codec.
    pub fn version(&self) -> u64 {
        self.version
    }
}

impl ValueCodec for VersionedCborCodec {
    fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, Error> {
        VersionedValue::cbor(&self.schema, self.version, value)?.to_bytes()
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, Error> {
        let envelope = VersionedValue::from_bytes(bytes)?;
        envelope.require_schema(&self.schema, self.version)?;
        envelope.decode_cbor()
    }
}

/// Deterministic schema/version envelope for an application value.
///
/// The envelope is useful when a tree contains values that may evolve over
/// time. It keeps the core map byte-oriented while giving applications a
/// stable place to record a schema name, schema version, value encoding, and
/// payload bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedValue {
    /// Application schema or type name.
    pub schema: String,
    /// Application-controlled schema version.
    pub version: u64,
    /// Encoding used for `payload`.
    pub encoding: Encoding,
    /// Encoded application payload.
    pub payload: Vec<u8>,
}

impl VersionedValue {
    /// Create a versioned raw-byte value.
    pub fn raw(schema: impl Into<String>, version: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            schema: schema.into(),
            version,
            encoding: Encoding::Raw,
            payload: payload.into(),
        }
    }

    /// Create a versioned JSON value.
    pub fn json<T: Serialize>(
        schema: impl Into<String>,
        version: u64,
        value: &T,
    ) -> Result<Self, Error> {
        Ok(Self {
            schema: schema.into(),
            version,
            encoding: Encoding::Json,
            payload: encode_json(value)?,
        })
    }

    /// Create a versioned CBOR value.
    pub fn cbor<T: Serialize>(
        schema: impl Into<String>,
        version: u64,
        value: &T,
    ) -> Result<Self, Error> {
        Ok(Self {
            schema: schema.into(),
            version,
            encoding: Encoding::Cbor,
            payload: encode_cbor(value)?,
        })
    }

    /// Create a versioned value with an explicit encoding marker.
    pub fn with_encoding(
        schema: impl Into<String>,
        version: u64,
        encoding: Encoding,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            schema: schema.into(),
            version,
            encoding,
            payload: payload.into(),
        }
    }

    /// Encode the envelope as deterministic bytes suitable for a leaf value.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let schema = self.schema.as_bytes();
        let custom = custom_encoding_name(&self.encoding);
        let schema_len = checked_u32_len(schema.len(), "schema")?;
        let custom_len = checked_u32_len(custom.len(), "custom encoding")?;

        let mut out =
            Vec::with_capacity(HEADER_LEN + schema.len() + custom.len() + self.payload.len());
        out.extend_from_slice(VERSIONED_VALUE_MAGIC);
        out.push(VERSIONED_VALUE_WIRE_VERSION);
        out.push(encoding_tag(&self.encoding));
        out.extend_from_slice(&self.version.to_be_bytes());
        out.extend_from_slice(&schema_len.to_be_bytes());
        out.extend_from_slice(&custom_len.to_be_bytes());
        out.extend_from_slice(&(self.payload.len() as u64).to_be_bytes());
        out.extend_from_slice(schema);
        out.extend_from_slice(custom);
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    /// Decode a deterministic versioned value envelope.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        decode_versioned_value(bytes)
    }

    /// Decode the payload as JSON, failing if the envelope is not JSON encoded.
    pub fn decode_json<T: DeserializeOwned>(&self) -> Result<T, Error> {
        if self.encoding != Encoding::Json {
            return Err(invalid_value(format!(
                "expected JSON payload, got {:?}",
                self.encoding
            )));
        }
        decode_json(&self.payload)
    }

    /// Decode the payload as CBOR, failing if the envelope is not CBOR encoded.
    pub fn decode_cbor<T: DeserializeOwned>(&self) -> Result<T, Error> {
        if self.encoding != Encoding::Cbor {
            return Err(invalid_value(format!(
                "expected CBOR payload, got {:?}",
                self.encoding
            )));
        }
        decode_cbor(&self.payload)
    }

    /// Verify the envelope schema and version before decoding a payload.
    pub fn require_schema(&self, schema: &str, version: u64) -> Result<(), Error> {
        if self.schema == schema && self.version == version {
            return Ok(());
        }

        Err(invalid_value(format!(
            "schema/version mismatch: expected {schema}@{version}, got {}@{}",
            self.schema, self.version
        )))
    }

    /// Return true when this value has the requested schema and version.
    pub fn matches_schema(&self, schema: &str, version: u64) -> bool {
        self.schema == schema && self.version == version
    }
}

fn decode_versioned_value(bytes: &[u8]) -> Result<VersionedValue, Error> {
    if bytes.len() < HEADER_LEN {
        return Err(invalid_value("versioned value envelope is too short"));
    }
    if !bytes.starts_with(VERSIONED_VALUE_MAGIC) {
        return Err(invalid_value("versioned value missing PLVV magic"));
    }

    let wire_version = bytes[4];
    if wire_version != VERSIONED_VALUE_WIRE_VERSION {
        return Err(invalid_value(format!(
            "unsupported versioned value wire version {wire_version}"
        )));
    }

    let encoding = bytes[5];
    let version = read_u64(bytes, 6)?;
    let schema_len = read_u32(bytes, 14)? as usize;
    let custom_len = read_u32(bytes, 18)? as usize;
    let payload_len = usize::try_from(read_u64(bytes, 22)?)
        .map_err(|_| invalid_value("payload length does not fit in usize"))?;

    let schema_start = HEADER_LEN;
    let custom_start = checked_add(schema_start, schema_len, "schema")?;
    let payload_start = checked_add(custom_start, custom_len, "custom encoding")?;
    let expected_len = checked_add(payload_start, payload_len, "payload")?;
    if expected_len != bytes.len() {
        return Err(invalid_value(format!(
            "versioned value length mismatch: header expects {expected_len} bytes, got {}",
            bytes.len()
        )));
    }

    let schema = decode_utf8(&bytes[schema_start..custom_start], "schema")?;
    let custom = decode_utf8(&bytes[custom_start..payload_start], "custom encoding")?;
    let encoding = decode_encoding(encoding, custom)?;
    let payload = bytes[payload_start..expected_len].to_vec();

    Ok(VersionedValue {
        schema,
        version,
        encoding,
        payload,
    })
}

fn checked_u32_len(len: usize, field: &str) -> Result<u32, Error> {
    u32::try_from(len).map_err(|_| invalid_value(format!("{field} is too large")))
}

fn checked_add(base: usize, len: usize, field: &str) -> Result<usize, Error> {
    base.checked_add(len)
        .ok_or_else(|| invalid_value(format!("{field} length overflows usize")))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, Error> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_value("versioned value header is truncated"))?;
    Ok(u32::from_be_bytes(
        value.try_into().expect("fixed slice length"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, Error> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid_value("versioned value header is truncated"))?;
    Ok(u64::from_be_bytes(
        value.try_into().expect("fixed slice length"),
    ))
}

fn decode_utf8(bytes: &[u8], field: &str) -> Result<String, Error> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|err| invalid_value(format!("{field} is not valid UTF-8: {err}")))
}

fn custom_encoding_name(encoding: &Encoding) -> &[u8] {
    match encoding {
        Encoding::Custom(name) => name.as_bytes(),
        _ => &[],
    }
}

fn encoding_tag(encoding: &Encoding) -> u8 {
    match encoding {
        Encoding::Raw => ENCODING_RAW,
        Encoding::Cbor => ENCODING_CBOR,
        Encoding::Json => ENCODING_JSON,
        Encoding::Custom(_) => ENCODING_CUSTOM,
    }
}

fn decode_encoding(tag: u8, custom: String) -> Result<Encoding, Error> {
    match tag {
        ENCODING_RAW => require_no_custom(custom, Encoding::Raw),
        ENCODING_CBOR => require_no_custom(custom, Encoding::Cbor),
        ENCODING_JSON => require_no_custom(custom, Encoding::Json),
        ENCODING_CUSTOM => Ok(Encoding::Custom(custom)),
        tag => Err(invalid_value(format!("unknown value encoding tag {tag}"))),
    }
}

fn require_no_custom(custom: String, encoding: Encoding) -> Result<Encoding, Error> {
    if custom.is_empty() {
        Ok(encoding)
    } else {
        Err(invalid_value(format!(
            "non-custom encoding {:?} included custom encoding name",
            encoding
        )))
    }
}

fn invalid_value(message: impl Into<String>) -> Error {
    Error::Deserialize(format!("invalid versioned value: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Example {
        name: String,
        score: u64,
    }

    #[test]
    fn json_and_cbor_helpers_round_trip_typed_values() {
        let value = Example {
            name: "memory".to_string(),
            score: 42,
        };

        let json = encode_json(&value).unwrap();
        assert_eq!(decode_json::<Example>(&json).unwrap(), value);

        let cbor = encode_cbor(&value).unwrap();
        assert_eq!(decode_cbor::<Example>(&cbor).unwrap(), value);
    }

    #[test]
    fn versioned_value_round_trips_schema_encoding_and_payload() {
        let value = Example {
            name: "chunk".to_string(),
            score: 9,
        };
        let envelope = VersionedValue::json("memory.chunk", 2, &value).unwrap();

        let bytes = envelope.to_bytes().unwrap();
        let decoded = VersionedValue::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.schema, "memory.chunk");
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.encoding, Encoding::Json);
        decoded.require_schema("memory.chunk", 2).unwrap();
        assert_eq!(decoded.decode_json::<Example>().unwrap(), value);
    }

    #[test]
    fn versioned_value_rejects_bad_magic_and_schema_mismatch() {
        assert!(matches!(
            VersionedValue::from_bytes(b"raw"),
            Err(Error::Deserialize(_))
        ));

        let value = VersionedValue::raw("memory", 1, b"bytes");
        let err = value.require_schema("memory", 2).unwrap_err();
        assert!(matches!(err, Error::Deserialize(_)));
    }
}
