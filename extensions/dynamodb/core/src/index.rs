use std::collections::{BTreeMap, BTreeSet};

use prolly::{
    IndexProjection as ProllyIndexProjection, SecondaryIndex, SecondaryIndexEntry,
    SecondaryIndexError, SecondaryIndexRegistry,
};
use serde::{Deserialize, Serialize};

use crate::blob::BlobLayer;
use crate::{
    encode_item, encode_key_schema, Error, Item, Result, SecondaryIndexDescription,
    SecondaryIndexId, SecondaryIndexProjection, TableDescription,
};

const INDEX_SOURCE_MAGIC: &[u8; 5] = b"DDBX\x01";
const INDEX_PROJECTION_INLINE_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IndexSourceEntry {
    term: Vec<u8>,
    /// Blob-aware `ValueRef` storage bytes for INCLUDE/ALL; absent for KEYS_ONLY.
    projection: Option<Vec<u8>>,
}

/// Compact deterministic mirror record consumed by synchronous Prolly index
/// extractors. Terms and projections are precomputed by the async DynamoDB
/// core, so extraction never performs provider I/O.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IndexSourceRecord {
    /// Blob-aware canonical complete item used for exact source fetches.
    item: Vec<u8>,
    entries: BTreeMap<SecondaryIndexId, IndexSourceEntry>,
}

/// Construct runtime definitions for one durable DynamoDB table descriptor.
pub(crate) fn index_registry(table: &TableDescription) -> Result<SecondaryIndexRegistry> {
    let mut registry = SecondaryIndexRegistry::new();
    for index in &table.secondary_indexes {
        // DynamoDB ALL is represented as an explicit blob-aware projection,
        // not Prolly's raw-source `All`, because the mirror source is compact.
        let projection = match index.projection {
            SecondaryIndexProjection::KeysOnly => ProllyIndexProjection::KeysOnly,
            SecondaryIndexProjection::Include(_) | SecondaryIndexProjection::All => {
                ProllyIndexProjection::Include
            }
        };
        let index_id = index.id.clone();
        let extractor_id = format!("prolly-dynamodb/index-source/v1/{}", hex(&index_id.0));
        let definition =
            SecondaryIndex::builder(index.name.as_bytes(), index.generation, extractor_id)
                .projection(projection)
                .extract(move |_primary_key, source_value| {
                    extract_persisted_entry(&index_id, source_value)
                })
                .map_err(Error::Storage)?;
        registry = registry.register(definition).map_err(Error::Storage)?;
    }
    Ok(registry)
}

pub(crate) async fn prepare_index_source_record(
    table: &TableDescription,
    item: &Item,
    stored_item: Vec<u8>,
    blobs: &BlobLayer,
) -> Result<Vec<u8>> {
    let mut entries = BTreeMap::new();
    for index in &table.secondary_indexes {
        let Some(term) = index_term(index, item)? else {
            continue;
        };
        let projection = match &index.projection {
            SecondaryIndexProjection::KeysOnly => None,
            SecondaryIndexProjection::Include(non_keys) => {
                let projected = projected_names(table, index, non_keys)
                    .into_iter()
                    .filter_map(|name| {
                        item.get(name)
                            .cloned()
                            .map(|value| (name.to_string(), value))
                    })
                    .collect::<Item>();
                Some(
                    blobs
                        .prepare_with_inline_threshold(
                            encode_item(&projected)?,
                            INDEX_PROJECTION_INLINE_BYTES,
                        )
                        .await?,
                )
            }
            SecondaryIndexProjection::All => Some(stored_item.clone()),
        };
        entries.insert(index.id.clone(), IndexSourceEntry { term, projection });
    }
    encode_index_source_record(&IndexSourceRecord {
        item: stored_item,
        entries,
    })
}

pub(crate) fn stored_item_from_index_source(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(decode_index_source_record(bytes)?.item)
}

fn extract_persisted_entry(
    index_id: &SecondaryIndexId,
    source_value: &[u8],
) -> std::result::Result<Vec<SecondaryIndexEntry>, SecondaryIndexError> {
    let record = decode_index_source_record(source_value).map_err(|_| {
        SecondaryIndexError::new("invalid canonical DynamoDB secondary-index source record")
    })?;
    Ok(record
        .entries
        .get(index_id)
        .map(|entry| {
            vec![SecondaryIndexEntry {
                term: entry.term.clone(),
                projection: entry.projection.clone(),
            }]
        })
        .unwrap_or_default())
}

pub(crate) fn index_term(
    index: &SecondaryIndexDescription,
    item: &Item,
) -> Result<Option<Vec<u8>>> {
    if !item.contains_key(&index.partition_key.name)
        || index
            .sort_key
            .as_ref()
            .is_some_and(|sort| !item.contains_key(&sort.name))
    {
        return Ok(None);
    }
    let mut key = Item::from([(
        index.partition_key.name.clone(),
        item[&index.partition_key.name].clone(),
    )]);
    if let Some(sort) = &index.sort_key {
        key.insert(sort.name.clone(), item[&sort.name].clone());
    }
    encode_key_schema(&index.partition_key, index.sort_key.as_ref(), &key).map(Some)
}

fn projected_names<'a>(
    table: &'a TableDescription,
    index: &'a SecondaryIndexDescription,
    non_keys: &'a BTreeSet<String>,
) -> BTreeSet<&'a str> {
    let mut names = non_keys.iter().map(String::as_str).collect::<BTreeSet<_>>();
    names.insert(table.partition_key.name.as_str());
    names.insert(index.partition_key.name.as_str());
    if let Some(sort) = &table.sort_key {
        names.insert(sort.name.as_str());
    }
    if let Some(sort) = &index.sort_key {
        names.insert(sort.name.as_str());
    }
    names
}

fn encode_index_source_record(record: &IndexSourceRecord) -> Result<Vec<u8>> {
    let mut bytes = INDEX_SOURCE_MAGIC.to_vec();
    bytes.extend(
        serde_cbor::ser::to_vec_packed(record)
            .map_err(|error| Error::Serialization(error.to_string()))?,
    );
    Ok(bytes)
}

fn decode_index_source_record(bytes: &[u8]) -> Result<IndexSourceRecord> {
    if !bytes.starts_with(INDEX_SOURCE_MAGIC) {
        return Err(Error::CorruptData(
            "secondary-index source record has an invalid envelope".into(),
        ));
    }
    let record: IndexSourceRecord = serde_cbor::from_slice(&bytes[INDEX_SOURCE_MAGIC.len()..])
        .map_err(|error| Error::CorruptData(error.to_string()))?;
    if encode_index_source_record(&record).map_err(|error| Error::CorruptData(error.to_string()))?
        != bytes
    {
        return Err(Error::CorruptData(
            "secondary-index source record is not canonical".into(),
        ));
    }
    Ok(record)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use prolly::IndexProjection;

    use super::*;
    use crate::{
        decode_item, AttributeValue, DynamoNumber, KeyAttribute, KeyKind, SecondaryIndexKind,
        SecondaryIndexStatus, TableId, TableStatus,
    };

    fn schema(projection: SecondaryIndexProjection) -> TableDescription {
        TableDescription {
            name: "Orders".into(),
            id: TableId([1; 32]),
            partition_key: KeyAttribute {
                name: "account".into(),
                kind: KeyKind::String,
            },
            sort_key: Some(KeyAttribute {
                name: "sequence".into(),
                kind: KeyKind::Number,
            }),
            attribute_definitions: BTreeMap::from([
                ("account".into(), KeyKind::String),
                ("sequence".into(), KeyKind::Number),
                ("status".into(), KeyKind::String),
                ("opened_at".into(), KeyKind::Number),
            ]),
            secondary_indexes: vec![SecondaryIndexDescription {
                name: "ByStatus".into(),
                id: SecondaryIndexId([2; 32]),
                generation: 1,
                kind: SecondaryIndexKind::Global,
                partition_key: KeyAttribute {
                    name: "status".into(),
                    kind: KeyKind::String,
                },
                sort_key: Some(KeyAttribute {
                    name: "opened_at".into(),
                    kind: KeyKind::Number,
                }),
                projection,
                status: SecondaryIndexStatus::Active,
            }],
            status: TableStatus::Active,
            created_at_millis: 42,
        }
    }

    fn item(include_index_keys: bool) -> Item {
        let mut item = Item::from([
            ("account".into(), AttributeValue::S("a-1".into())),
            (
                "sequence".into(),
                AttributeValue::N(DynamoNumber::parse("1").unwrap()),
            ),
            ("owner".into(), AttributeValue::S("legal".into())),
            ("secret".into(), AttributeValue::S("omit".into())),
        ]);
        if include_index_keys {
            item.insert("status".into(), AttributeValue::S("OPEN".into()));
            item.insert(
                "opened_at".into(),
                AttributeValue::N(DynamoNumber::parse("10").unwrap()),
            );
        }
        item
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut context = Context::from_waker(Waker::noop());
        let mut future = pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn persisted_extractor_is_sparse_and_projection_exact() {
        block_on(async {
            let table = schema(SecondaryIndexProjection::Include(BTreeSet::from([
                "owner".into()
            ])));
            table.validate().unwrap();
            let registry = index_registry(&table).unwrap();
            let definition = registry.get(b"ByStatus").unwrap();
            assert_eq!(definition.projection(), IndexProjection::Include);
            let blobs = BlobLayer::inline_only();

            let sparse = item(false);
            let sparse_stored = encode_item(&sparse).unwrap();
            let source =
                prepare_index_source_record(&table, &sparse, sparse_stored.clone(), &blobs)
                    .await
                    .unwrap();
            assert!(definition.extract(b"pk", &source).unwrap().is_empty());
            assert_eq!(
                stored_item_from_index_source(&source).unwrap(),
                sparse_stored
            );

            let item = item(true);
            let stored = encode_item(&item).unwrap();
            let source = prepare_index_source_record(&table, &item, stored, &blobs)
                .await
                .unwrap();
            let entries = definition.extract(b"pk", &source).unwrap();
            assert_eq!(entries.len(), 1);
            let projection = decode_item(
                &blobs
                    .resolve(entries[0].projection.as_ref().unwrap())
                    .await
                    .unwrap(),
            )
            .unwrap();
            for name in ["account", "sequence", "status", "opened_at", "owner"] {
                assert!(projection.contains_key(name));
            }
            assert!(!projection.contains_key("secret"));
            assert!(decode_index_source_record(&[source, vec![0]].concat()).is_err());
        });
    }
}
