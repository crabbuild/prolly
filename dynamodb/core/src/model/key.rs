use super::{AttributeValue, Item, KeyAttribute, KeyKind, TableDescription};
use crate::{Error, Result};

const ITEM_MAGIC: &[u8; 4] = b"DDBI";
const ITEM_VERSION: u8 = 1;
pub const MAX_ITEM_BYTES: usize = 400 * 1024;
const MAX_PARTITION_KEY_BYTES: usize = 2048;
const MAX_SORT_KEY_BYTES: usize = 1024;

pub fn encode_item(item: &Item) -> Result<Vec<u8>> {
    let canonical = canonical_item(item, 0)?;
    let logical_size = item_size_canonical(&canonical)?;
    if logical_size > MAX_ITEM_BYTES {
        return Err(Error::Validation(format!(
            "item uses {logical_size} logical bytes, exceeding {MAX_ITEM_BYTES}"
        )));
    }
    let payload = serde_cbor::ser::to_vec_packed(&canonical)
        .map_err(|error| Error::Serialization(error.to_string()))?;
    let mut bytes = Vec::with_capacity(ITEM_MAGIC.len() + 1 + payload.len());
    bytes.extend_from_slice(ITEM_MAGIC);
    bytes.push(ITEM_VERSION);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// Calculate the logical DynamoDB item size using the documented attribute,
/// collection-overhead, and number-size rules.
pub fn item_size(item: &Item) -> Result<usize> {
    item_size_canonical(&canonical_item(item, 0)?)
}

/// Validate and canonicalize one standalone attribute value.
pub fn canonicalize_attribute_value(value: &AttributeValue) -> Result<AttributeValue> {
    canonical_value(value, 0)
}

pub fn decode_item(bytes: &[u8]) -> Result<Item> {
    if bytes.len() < 5 || &bytes[..4] != ITEM_MAGIC || bytes[4] != ITEM_VERSION {
        return Err(Error::CorruptData(
            "unsupported or malformed DDBI item envelope".into(),
        ));
    }
    let item: Item = serde_cbor::from_slice(&bytes[5..])
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    let canonical =
        canonical_item(&item, 0).map_err(|error| Error::CorruptData(error.to_string()))?;
    if encode_item(&canonical).map_err(|error| Error::CorruptData(error.to_string()))? != bytes {
        return Err(Error::CorruptData("item encoding is not canonical".into()));
    }
    Ok(canonical)
}

fn canonical_item(item: &Item, depth: usize) -> Result<Item> {
    let mut canonical = Item::new();
    for (name, value) in item {
        if name.is_empty() || name.len() > 64 * 1024 {
            return Err(Error::Validation(
                "attribute name length must be 1..=65536 UTF-8 bytes".into(),
            ));
        }
        canonical.insert(name.clone(), canonical_value(value, depth)?);
    }
    Ok(canonical)
}

fn canonical_value(value: &AttributeValue, depth: usize) -> Result<AttributeValue> {
    Ok(match value {
        AttributeValue::B(value) => AttributeValue::B(value.clone()),
        AttributeValue::Bool(value) => AttributeValue::Bool(*value),
        AttributeValue::Bs(values) => {
            let mut values = values.clone();
            values.sort();
            reject_empty_or_duplicate_set(&values)?;
            AttributeValue::Bs(values)
        }
        AttributeValue::L(values) => {
            ensure_document_depth(depth)?;
            AttributeValue::L(
                values
                    .iter()
                    .map(|value| canonical_value(value, depth + 1))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        AttributeValue::M(values) => {
            ensure_document_depth(depth)?;
            AttributeValue::M(canonical_item(values, depth + 1)?)
        }
        AttributeValue::N(value) => AttributeValue::N(value.clone()),
        AttributeValue::Ns(values) => {
            let mut values = values.clone();
            values.sort();
            reject_empty_or_duplicate_set(&values)?;
            AttributeValue::Ns(values)
        }
        AttributeValue::Null(true) => AttributeValue::Null(true),
        AttributeValue::Null(false) => {
            return Err(Error::Validation(
                "a DynamoDB NULL attribute must contain true".into(),
            ))
        }
        AttributeValue::S(value) => AttributeValue::S(value.clone()),
        AttributeValue::Ss(values) => {
            let mut values = values.clone();
            values.sort();
            reject_empty_or_duplicate_set(&values)?;
            AttributeValue::Ss(values)
        }
    })
}

fn ensure_document_depth(depth: usize) -> Result<()> {
    if depth >= 32 {
        return Err(Error::Validation(
            "document nesting exceeds 32 levels".into(),
        ));
    }
    Ok(())
}

fn reject_empty_or_duplicate_set<T: PartialEq>(values: &[T]) -> Result<()> {
    if values.is_empty() {
        return Err(Error::Validation("sets must not be empty".into()));
    }
    if values.windows(2).any(|window| window[0] == window[1]) {
        return Err(Error::Validation("sets must not contain duplicates".into()));
    }
    Ok(())
}

fn item_size_canonical(item: &Item) -> Result<usize> {
    item.iter().try_fold(0_usize, |size, (name, value)| {
        size.checked_add(name.len())
            .and_then(|size| logical_value_size(value).and_then(|value| size.checked_add(value)))
            .ok_or_else(|| Error::Validation("item size overflow".into()))
    })
}

fn logical_value_size(value: &AttributeValue) -> Option<usize> {
    match value {
        AttributeValue::B(value) => Some(value.len()),
        AttributeValue::Bool(_) | AttributeValue::Null(_) => Some(1),
        AttributeValue::Bs(values) => checked_sum(values.iter().map(Vec::len)),
        AttributeValue::L(values) => checked_sum(
            values
                .iter()
                .map(logical_value_size)
                .map(|size| size.and_then(|size| size.checked_add(1))),
        )?
        .checked_add(3),
        AttributeValue::M(values) => checked_sum(values.iter().map(|(name, value)| {
            logical_value_size(value)
                .and_then(|size| size.checked_add(name.len()))
                .and_then(|size| size.checked_add(1))
        }))?
        .checked_add(3),
        AttributeValue::N(value) => Some(value.storage_size()),
        AttributeValue::Ns(values) => checked_sum(values.iter().map(|value| value.storage_size())),
        AttributeValue::S(value) => Some(value.len()),
        AttributeValue::Ss(values) => checked_sum(values.iter().map(String::len)),
    }
}

fn checked_sum<I, T>(values: I) -> Option<usize>
where
    I: IntoIterator<Item = T>,
    T: Into<Option<usize>>,
{
    values
        .into_iter()
        .try_fold(0_usize, |total, value| total.checked_add(value.into()?))
}

pub fn encode_primary_key(schema: &TableDescription, key: &Item) -> Result<Vec<u8>> {
    encode_key_schema(&schema.partition_key, schema.sort_key.as_ref(), key)
}

pub fn encode_key_schema(
    partition_key: &KeyAttribute,
    sort_key: Option<&KeyAttribute>,
    key: &Item,
) -> Result<Vec<u8>> {
    let expected = 1 + usize::from(sort_key.is_some());
    if key.len() != expected {
        return Err(Error::Validation(format!(
            "key contains {} attributes; schema requires {expected}",
            key.len()
        )));
    }
    let mut encoded = Vec::new();
    encode_component(
        &mut encoded,
        partition_key,
        key.get(&partition_key.name),
        MAX_PARTITION_KEY_BYTES,
    )?;
    if let Some(sort_key) = sort_key {
        encode_component(
            &mut encoded,
            sort_key,
            key.get(&sort_key.name),
            MAX_SORT_KEY_BYTES,
        )?;
    }
    Ok(encoded)
}

pub fn encode_partition_prefix(schema: &TableDescription, key: &Item) -> Result<Vec<u8>> {
    if key.len() != 1 {
        return Err(Error::Validation(
            "partition key must contain exactly one attribute".into(),
        ));
    }
    let mut encoded = Vec::new();
    encode_component(
        &mut encoded,
        &schema.partition_key,
        key.get(&schema.partition_key.name),
        MAX_PARTITION_KEY_BYTES,
    )?;
    Ok(encoded)
}

fn encode_component(
    output: &mut Vec<u8>,
    declaration: &KeyAttribute,
    value: Option<&AttributeValue>,
    maximum: usize,
) -> Result<()> {
    let value = value.ok_or_else(|| {
        Error::Validation(format!("missing key attribute {:?}", declaration.name))
    })?;
    let (tag, bytes) = match (declaration.kind, value) {
        (KeyKind::String, AttributeValue::S(value)) if !value.is_empty() => {
            (b's', value.as_bytes().to_vec())
        }
        (KeyKind::Binary, AttributeValue::B(value)) if !value.is_empty() => (b'b', value.clone()),
        (KeyKind::Number, AttributeValue::N(value)) => (b'n', value.ordered_bytes()),
        _ => {
            return Err(Error::Validation(format!(
                "key attribute {:?} has the wrong or an empty scalar type",
                declaration.name
            )))
        }
    };
    if bytes.len() > maximum {
        return Err(Error::Validation(format!(
            "key attribute {:?} exceeds {maximum} bytes",
            declaration.name
        )));
    }
    output.push(tag);
    for byte in bytes {
        if byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
    Ok(())
}
