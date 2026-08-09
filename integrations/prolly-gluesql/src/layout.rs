use {
    crate::{Error, Result},
    serde::{de::DeserializeOwned, Serialize},
};

const DATABASE_PREFIX: &[u8] = b"\0prolly-gluesql\x01";
const RECORD_MAGIC: &[u8; 4] = b"PGSQ";
const RECORD_VERSION: u16 = 1;

pub(crate) const KIND_SCHEMA: u8 = 1;
pub(crate) const KIND_ROW: u8 = 2;
pub(crate) const KIND_SEQUENCE: u8 = 3;
pub(crate) const KIND_METADATA: u8 = 4;
pub(crate) const KIND_FUNCTION: u8 = 5;
pub(crate) const KIND_INDEX: u8 = 6;

pub(crate) fn encode_record<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = bincode::serialize(value)?;
    let mut encoded = Vec::with_capacity(RECORD_MAGIC.len() + 2 + payload.len());
    encoded.extend_from_slice(RECORD_MAGIC);
    encoded.extend_from_slice(&RECORD_VERSION.to_be_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub(crate) fn decode_record<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    if bytes.len() < 6 || &bytes[..4] != RECORD_MAGIC {
        return Err(Error::Corrupt("missing ProllySQL record header".to_owned()));
    }
    let version = u16::from_be_bytes([bytes[4], bytes[5]]);
    if version != RECORD_VERSION {
        return Err(Error::UnsupportedFormat(format!(
            "record version {version}; expected {RECORD_VERSION}"
        )));
    }
    Ok(bincode::deserialize(&bytes[6..])?)
}

fn kind_prefix(kind: u8) -> Vec<u8> {
    let mut key = Vec::with_capacity(DATABASE_PREFIX.len() + 1);
    key.extend_from_slice(DATABASE_PREFIX);
    key.push(kind);
    key
}

fn push_segment(key: &mut Vec<u8>, segment: &[u8]) {
    let length = segment.len() as u64;
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(segment);
}

pub(crate) fn all_schemas_prefix() -> Vec<u8> {
    kind_prefix(KIND_SCHEMA)
}

pub(crate) fn all_rows_prefix() -> Vec<u8> {
    kind_prefix(KIND_ROW)
}

pub(crate) fn all_sequences_prefix() -> Vec<u8> {
    kind_prefix(KIND_SEQUENCE)
}

pub(crate) fn schema_key(table_name: &str) -> Vec<u8> {
    let mut key = all_schemas_prefix();
    push_segment(&mut key, table_name.as_bytes());
    key
}

pub(crate) fn row_prefix(table_name: &str) -> Vec<u8> {
    let mut key = kind_prefix(KIND_ROW);
    push_segment(&mut key, table_name.as_bytes());
    key
}

pub(crate) fn row_key(table_name: &str, encoded_key: &[u8]) -> Vec<u8> {
    let mut key = row_prefix(table_name);
    key.extend_from_slice(encoded_key);
    key
}

pub(crate) fn row_key_payload<'a>(table_name: &str, physical_key: &'a [u8]) -> Result<&'a [u8]> {
    let prefix = row_prefix(table_name);
    physical_key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| Error::Corrupt("row key escaped its table prefix".to_owned()))
}

pub(crate) fn row_key_parts(physical_key: &[u8]) -> Result<(String, &[u8])> {
    let prefix = kind_prefix(KIND_ROW);
    let tail = physical_key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| Error::Corrupt("row key is outside the row namespace".to_owned()))?;
    let (table_name, encoded_key) = split_segment(tail)?;
    let table_name = String::from_utf8(table_name.to_vec())
        .map_err(|_| Error::Corrupt("row key contains a non-UTF-8 table name".to_owned()))?;
    Ok((table_name, encoded_key))
}

pub(crate) fn key_kind(key: &[u8]) -> Option<u8> {
    key.strip_prefix(DATABASE_PREFIX)?.first().copied()
}

fn split_segment(bytes: &[u8]) -> Result<(&[u8], &[u8])> {
    let length_bytes = bytes
        .get(..8)
        .ok_or_else(|| Error::Corrupt("truncated key segment length".to_owned()))?;
    let length = usize::try_from(u64::from_be_bytes(
        length_bytes
            .try_into()
            .map_err(|_| Error::Corrupt("invalid key segment length".to_owned()))?,
    ))
    .map_err(|_| Error::Corrupt("key segment length exceeds this platform".to_owned()))?;
    let end = 8_usize
        .checked_add(length)
        .ok_or_else(|| Error::Corrupt("key segment length overflow".to_owned()))?;
    let segment = bytes
        .get(8..end)
        .ok_or_else(|| Error::Corrupt("truncated key segment".to_owned()))?;
    Ok((segment, &bytes[end..]))
}

pub(crate) fn sequence_key(table_name: &str) -> Vec<u8> {
    let mut key = kind_prefix(KIND_SEQUENCE);
    push_segment(&mut key, table_name.as_bytes());
    key
}

pub(crate) fn metadata_prefix() -> Vec<u8> {
    kind_prefix(KIND_METADATA)
}

pub(crate) fn metadata_key(table_name: &str) -> Vec<u8> {
    let mut key = metadata_prefix();
    push_segment(&mut key, table_name.as_bytes());
    key
}

pub(crate) fn functions_prefix() -> Vec<u8> {
    kind_prefix(KIND_FUNCTION)
}

pub(crate) fn all_indexes_prefix() -> Vec<u8> {
    kind_prefix(KIND_INDEX)
}

pub(crate) fn function_key(function_name: &str) -> Vec<u8> {
    let mut key = functions_prefix();
    push_segment(&mut key, function_name.to_uppercase().as_bytes());
    key
}

pub(crate) fn index_prefix(table_name: &str, index_name: &str) -> Vec<u8> {
    let mut key = kind_prefix(KIND_INDEX);
    push_segment(&mut key, table_name.as_bytes());
    push_segment(&mut key, index_name.as_bytes());
    key
}

pub(crate) fn index_key(
    table_name: &str,
    index_name: &str,
    comparison_bytes: &[u8],
    identity_bytes: &[u8],
    encoded_primary_key: &[u8],
) -> Vec<u8> {
    let mut key = index_value_prefix(table_name, index_name, comparison_bytes);
    push_memcomparable(&mut key, identity_bytes);
    key.extend_from_slice(encoded_primary_key);
    key
}

pub(crate) fn index_value_prefix(
    table_name: &str,
    index_name: &str,
    comparison_bytes: &[u8],
) -> Vec<u8> {
    let mut key = index_prefix(table_name, index_name);
    push_memcomparable(&mut key, comparison_bytes);
    key
}

fn push_memcomparable(target: &mut Vec<u8>, bytes: &[u8]) {
    for byte in bytes {
        if *byte == 0 {
            target.extend_from_slice(&[0, 0xff]);
        } else {
            target.push(*byte);
        }
    }
    target.extend_from_slice(&[0, 0]);
}

pub(crate) fn branch_root_name(branch: &str) -> Result<Vec<u8>> {
    validate_ref_component(branch)?;
    let mut name = b"\0prolly-gluesql/v1/refs/heads/".to_vec();
    name.extend_from_slice(branch.as_bytes());
    Ok(name)
}

pub(crate) fn validate_ref_component(value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 255
        && !value.starts_with('/')
        && !value.ends_with('/')
        && !value.contains("//")
        && !value.contains("..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'/' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(Error::Branch(format!("invalid branch name {value:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::data::Value;

    #[test]
    fn records_reject_unknown_versions() {
        let mut record = encode_record(&42_u64).unwrap();
        record[5] = 2;
        assert!(matches!(
            decode_record::<u64>(&record),
            Err(Error::UnsupportedFormat(_))
        ));
    }

    #[test]
    fn table_segments_do_not_collide() {
        assert_ne!(row_prefix("a"), row_prefix("a\0"));
        assert_ne!(row_prefix("a/b"), row_prefix("a"));
    }

    #[test]
    fn index_keys_preserve_value_identity_after_comparison_encoding() {
        let signed = Value::I8(1);
        let unsigned = Value::U8(1);
        let signed_comparison = signed.to_cmp_be_bytes().unwrap();
        let unsigned_comparison = unsigned.to_cmp_be_bytes().unwrap();
        assert_eq!(signed_comparison, unsigned_comparison);

        let signed_key = index_key(
            "items",
            "value_idx",
            &signed_comparison,
            &bincode::serialize(&signed).unwrap(),
            b"pk",
        );
        let unsigned_key = index_key(
            "items",
            "value_idx",
            &unsigned_comparison,
            &bincode::serialize(&unsigned).unwrap(),
            b"pk",
        );
        assert_ne!(signed_key, unsigned_key);
    }

    #[test]
    fn validates_branch_names() {
        assert!(branch_root_name("feature/sql-1").is_ok());
        assert!(branch_root_name("").is_err());
        assert!(branch_root_name("../main").is_err());
    }
}
