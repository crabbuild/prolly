use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Stable random identity for one logical table incarnation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableId(pub [u8; 32]);

/// DynamoDB key scalar type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum KeyKind {
    String,
    Number,
    Binary,
}

/// One partition or sort key declaration.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KeyAttribute {
    pub name: String,
    pub kind: KeyKind,
}

/// Stable identity for one secondary-index generation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SecondaryIndexId(pub [u8; 32]);

/// DynamoDB secondary-index family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryIndexKind {
    Local,
    Global,
}

/// Attributes materialized in an index entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryIndexProjection {
    KeysOnly,
    Include(BTreeSet<String>),
    All,
}

/// Durable lifecycle of one index generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryIndexStatus {
    Building,
    Active,
    Retiring,
}

/// Durable logical description of one LSI or GSI generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexDescription {
    pub name: String,
    pub id: SecondaryIndexId,
    pub generation: u64,
    pub kind: SecondaryIndexKind,
    pub partition_key: KeyAttribute,
    pub sort_key: Option<KeyAttribute>,
    pub projection: SecondaryIndexProjection,
    pub status: SecondaryIndexStatus,
}

/// Requested secondary-index schema before a table incarnation assigns IDs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndexDefinition {
    pub name: String,
    pub kind: SecondaryIndexKind,
    pub partition_key: KeyAttribute,
    pub sort_key: Option<KeyAttribute>,
    pub projection: SecondaryIndexProjection,
}

/// Logical table lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TableStatus {
    Active,
    Deleting,
}

/// Durable logical table descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDescription {
    pub name: String,
    pub id: TableId,
    pub partition_key: KeyAttribute,
    pub sort_key: Option<KeyAttribute>,
    /// Every scalar attribute used by the table or an index key.
    pub attribute_definitions: BTreeMap<String, KeyKind>,
    pub secondary_indexes: Vec<SecondaryIndexDescription>,
    pub status: TableStatus,
    pub created_at_millis: u64,
}

impl TableDescription {
    pub fn validate(&self) -> Result<()> {
        if !(3..=255).contains(&self.name.len())
            || !self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(Error::Validation(
                "table name must contain 3..=255 ASCII letters, digits, '_', '-', or '.'".into(),
            ));
        }
        if self.partition_key.name.is_empty() || self.partition_key.name.len() > 255 {
            return Err(Error::Validation(
                "partition key name length must be 1..=255 bytes".into(),
            ));
        }
        if self.sort_key.as_ref().is_some_and(|sort| {
            sort.name.is_empty() || sort.name.len() > 255 || sort.name == self.partition_key.name
        }) {
            return Err(Error::Validation(
                "sort key name must be 1..=255 bytes and distinct from partition key".into(),
            ));
        }
        let mut required =
            BTreeMap::from([(self.partition_key.name.clone(), self.partition_key.kind)]);
        if let Some(sort) = &self.sort_key {
            required.insert(sort.name.clone(), sort.kind);
        }
        let mut names = BTreeSet::new();
        let mut ids = BTreeSet::new();
        let mut local_count = 0usize;
        let mut global_count = 0usize;
        let mut included_attributes = 0usize;
        for index in &self.secondary_indexes {
            if !(3..=255).contains(&index.name.len())
                || !index
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
            {
                return Err(Error::Validation(
                    "index name must contain 3..=255 ASCII letters, digits, '_', '-', or '.'"
                        .into(),
                ));
            }
            if !names.insert(index.name.as_str()) || !ids.insert(&index.id) {
                return Err(Error::Validation(
                    "secondary index names and IDs must be unique within a table".into(),
                ));
            }
            if index.generation == 0 {
                return Err(Error::Validation(
                    "secondary index generation must be greater than zero".into(),
                ));
            }
            validate_key_schema(&index.partition_key, index.sort_key.as_ref())?;
            insert_required_definition(&mut required, &index.partition_key)?;
            if let Some(sort) = &index.sort_key {
                insert_required_definition(&mut required, sort)?;
            }
            match index.kind {
                SecondaryIndexKind::Local => {
                    local_count += 1;
                    if self.sort_key.is_none()
                        || index.partition_key != self.partition_key
                        || index.sort_key.is_none()
                    {
                        return Err(Error::Validation(
                            "an LSI requires the table partition key and an alternate sort key"
                                .into(),
                        ));
                    }
                }
                SecondaryIndexKind::Global => global_count += 1,
            }
            if let SecondaryIndexProjection::Include(attributes) = &index.projection {
                if attributes.is_empty() || attributes.len() > 20 {
                    return Err(Error::Validation(
                        "INCLUDE projection must contain 1..=20 unique non-key attributes".into(),
                    ));
                }
                let key_names = required_key_names(self, index);
                if attributes.iter().any(|name| {
                    name.is_empty() || name.len() > 255 || key_names.contains(name.as_str())
                }) {
                    return Err(Error::Validation(
                        "INCLUDE projection attributes must be valid non-key attribute names"
                            .into(),
                    ));
                }
                included_attributes = included_attributes.saturating_add(attributes.len());
            }
        }
        if local_count > 5 || global_count > 20 || included_attributes > 100 {
            return Err(Error::Validation(
                "table exceeds DynamoDB's LSI, GSI, or projected-attribute limits".into(),
            ));
        }
        if self.attribute_definitions != required {
            return Err(Error::Validation(
                "attribute definitions must exactly match all table and index key attributes"
                    .into(),
            ));
        }
        Ok(())
    }
}

fn insert_required_definition(
    definitions: &mut BTreeMap<String, KeyKind>,
    key: &KeyAttribute,
) -> Result<()> {
    if definitions
        .get(&key.name)
        .is_some_and(|kind| *kind != key.kind)
    {
        return Err(Error::Validation(format!(
            "attribute definition for {:?} has inconsistent scalar types",
            key.name
        )));
    }
    definitions.insert(key.name.clone(), key.kind);
    Ok(())
}

fn validate_key_schema(partition: &KeyAttribute, sort: Option<&KeyAttribute>) -> Result<()> {
    if partition.name.is_empty() || partition.name.len() > 255 {
        return Err(Error::Validation(
            "partition key name length must be 1..=255 bytes".into(),
        ));
    }
    if sort.is_some_and(|sort| {
        sort.name.is_empty() || sort.name.len() > 255 || sort.name == partition.name
    }) {
        return Err(Error::Validation(
            "sort key name must be 1..=255 bytes and distinct from partition key".into(),
        ));
    }
    Ok(())
}

fn required_key_names<'a>(
    table: &'a TableDescription,
    index: &'a SecondaryIndexDescription,
) -> BTreeSet<&'a str> {
    let mut names = BTreeSet::from([
        table.partition_key.name.as_str(),
        index.partition_key.name.as_str(),
    ]);
    if let Some(sort) = &table.sort_key {
        names.insert(sort.name.as_str());
    }
    if let Some(sort) = &index.sort_key {
        names.insert(sort.name.as_str());
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    fn indexed_table() -> TableDescription {
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
            secondary_indexes: vec![
                SecondaryIndexDescription {
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
                    projection: SecondaryIndexProjection::Include(BTreeSet::from(["owner".into()])),
                    status: SecondaryIndexStatus::Active,
                },
                SecondaryIndexDescription {
                    name: "ByOpenedAt".into(),
                    id: SecondaryIndexId([3; 32]),
                    generation: 1,
                    kind: SecondaryIndexKind::Local,
                    partition_key: KeyAttribute {
                        name: "account".into(),
                        kind: KeyKind::String,
                    },
                    sort_key: Some(KeyAttribute {
                        name: "opened_at".into(),
                        kind: KeyKind::Number,
                    }),
                    projection: SecondaryIndexProjection::KeysOnly,
                    status: SecondaryIndexStatus::Active,
                },
            ],
            status: TableStatus::Active,
            created_at_millis: 42,
        }
    }

    #[test]
    fn secondary_index_schema_enforces_dynamodb_limits_and_key_types() {
        let table = indexed_table();
        table.validate().unwrap();

        let mut wrong_lsi = table.clone();
        wrong_lsi.secondary_indexes[1].partition_key.name = "status".into();
        assert!(wrong_lsi.validate().is_err());

        let mut key_projection = table.clone();
        key_projection.secondary_indexes[0].projection =
            SecondaryIndexProjection::Include(BTreeSet::from(["account".into()]));
        assert!(key_projection.validate().is_err());

        let mut conflicting_type = table;
        conflicting_type.secondary_indexes[0].sort_key = Some(KeyAttribute {
            name: "sequence".into(),
            kind: KeyKind::String,
        });
        assert!(conflicting_type.validate().is_err());
    }
}
