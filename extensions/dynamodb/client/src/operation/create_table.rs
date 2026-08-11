use std::collections::{BTreeMap, BTreeSet};

use aws_sdk_dynamodb::operation::create_table::CreateTableOutput;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, LocalSecondaryIndex,
    Projection, ProjectionType, ScalarAttributeType,
};
use prolly_dynamodb_core::{
    KeyAttribute, KeyKind, SecondaryIndexDefinition, SecondaryIndexKind, SecondaryIndexProjection,
};

use crate::conversion::table_to_aws;
use crate::{Client, Error, Result, TableTransitionMetadata, WithMetadata};

#[derive(Clone)]
pub struct CreateTable {
    client: Client,
    table_name: Option<String>,
    attribute_definitions: Vec<AttributeDefinition>,
    key_schema: Vec<KeySchemaElement>,
    local_secondary_indexes: Vec<LocalSecondaryIndex>,
    global_secondary_indexes: Vec<GlobalSecondaryIndex>,
    request_token: Option<String>,
}

impl CreateTable {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            table_name: None,
            attribute_definitions: Vec::new(),
            key_schema: Vec::new(),
            local_secondary_indexes: Vec::new(),
            global_secondary_indexes: Vec::new(),
            request_token: None,
        }
    }

    pub fn table_name(mut self, value: impl Into<String>) -> Self {
        self.table_name = Some(value.into());
        self
    }

    pub fn attribute_definitions(mut self, value: AttributeDefinition) -> Self {
        self.attribute_definitions.push(value);
        self
    }

    pub fn set_attribute_definitions(mut self, values: Option<Vec<AttributeDefinition>>) -> Self {
        self.attribute_definitions = values.unwrap_or_default();
        self
    }

    pub fn key_schema(mut self, value: KeySchemaElement) -> Self {
        self.key_schema.push(value);
        self
    }

    pub fn set_key_schema(mut self, values: Option<Vec<KeySchemaElement>>) -> Self {
        self.key_schema = values.unwrap_or_default();
        self
    }

    pub fn local_secondary_indexes(mut self, value: LocalSecondaryIndex) -> Self {
        self.local_secondary_indexes.push(value);
        self
    }

    pub fn set_local_secondary_indexes(mut self, values: Option<Vec<LocalSecondaryIndex>>) -> Self {
        self.local_secondary_indexes = values.unwrap_or_default();
        self
    }

    pub fn global_secondary_indexes(mut self, value: GlobalSecondaryIndex) -> Self {
        self.global_secondary_indexes.push(value);
        self
    }

    pub fn set_global_secondary_indexes(
        mut self,
        values: Option<Vec<GlobalSecondaryIndex>>,
    ) -> Self {
        self.global_secondary_indexes = values.unwrap_or_default();
        self
    }

    /// Add durable ten-minute replay protection for logical table creation.
    pub fn request_token(mut self, token: impl Into<String>) -> Self {
        self.request_token = Some(token.into());
        self
    }

    pub fn set_request_token(mut self, token: Option<String>) -> Self {
        self.request_token = token;
        self
    }

    pub async fn send(self) -> Result<CreateTableOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.CreateTable",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "CreateTable"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<CreateTableOutput>> {
        let name = self
            .table_name
            .ok_or_else(|| Error::InvalidRequest("CreateTable.table_name is required".into()))?;
        let definition_count = self.attribute_definitions.len();
        let definitions = self
            .attribute_definitions
            .into_iter()
            .map(|definition| {
                let kind = match definition.attribute_type() {
                    value if value == &ScalarAttributeType::S => KeyKind::String,
                    value if value == &ScalarAttributeType::N => KeyKind::Number,
                    value if value == &ScalarAttributeType::B => KeyKind::Binary,
                    _ => return Err(Error::Unsupported("unknown ScalarAttributeType".into())),
                };
                Ok((definition.attribute_name().to_string(), kind))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        if definitions.len() != definition_count {
            return Err(Error::InvalidRequest(
                "attribute definitions must have unique names".into(),
            ));
        }
        let (partition_key, sort_key) = parse_key_schema(&self.key_schema, &definitions)?;
        let mut indexes = Vec::with_capacity(
            self.local_secondary_indexes.len() + self.global_secondary_indexes.len(),
        );
        for index in self.local_secondary_indexes {
            let (index_partition, index_sort) = parse_key_schema(index.key_schema(), &definitions)?;
            indexes.push(SecondaryIndexDefinition {
                name: index.index_name().to_string(),
                kind: SecondaryIndexKind::Local,
                partition_key: index_partition,
                sort_key: index_sort,
                projection: parse_projection(index.projection())?,
            });
        }
        for index in self.global_secondary_indexes {
            if index.provisioned_throughput.is_some()
                || index.on_demand_throughput.is_some()
                || index.warm_throughput.is_some()
            {
                return Err(Error::Unsupported(
                    "logical global indexes do not accept throughput configuration".into(),
                ));
            }
            let (index_partition, index_sort) = parse_key_schema(index.key_schema(), &definitions)?;
            indexes.push(SecondaryIndexDefinition {
                name: index.index_name().to_string(),
                kind: SecondaryIndexKind::Global,
                partition_key: index_partition,
                sort_key: index_sort,
                projection: parse_projection(index.projection())?,
            });
        }
        let lifecycle = self
            .client
            .core()
            .create_table_with_indexes_result(
                name.clone(),
                partition_key,
                sort_key,
                definitions,
                indexes,
                self.request_token.as_deref(),
            )
            .await?;
        let version_id = lifecycle.transition.after.clone();
        let transition = TableTransitionMetadata {
            commit_id: Some(lifecycle.commit_id.clone()),
            table_name: name.clone(),
            table_id: Some(lifecycle.transition.table_id),
            before: lifecycle.transition.before,
            after: lifecycle.transition.after,
            applied: lifecycle.transition.applied,
        };
        Ok(WithMetadata::single_write(
            CreateTableOutput::builder()
                .table_description(table_to_aws(&lifecycle.description))
                .build(),
            name,
            version_id,
            Some(lifecycle.commit_id),
            transition,
        ))
    }
}

fn parse_key_schema(
    elements: &[KeySchemaElement],
    definitions: &BTreeMap<String, KeyKind>,
) -> Result<(KeyAttribute, Option<KeyAttribute>)> {
    let mut partition_key = None;
    let mut sort_key = None;
    for element in elements {
        let kind = definitions.get(element.attribute_name()).ok_or_else(|| {
            Error::InvalidRequest(format!(
                "missing attribute definition for {:?}",
                element.attribute_name()
            ))
        })?;
        let key = KeyAttribute {
            name: element.attribute_name().to_string(),
            kind: *kind,
        };
        match element.key_type() {
            value if value == &KeyType::Hash && partition_key.is_none() => {
                partition_key = Some(key)
            }
            value if value == &KeyType::Range && sort_key.is_none() => sort_key = Some(key),
            _ => {
                return Err(Error::InvalidRequest(
                    "invalid or duplicate key role".into(),
                ))
            }
        }
    }
    if elements.len() != usize::from(sort_key.is_some()) + 1 {
        return Err(Error::InvalidRequest(
            "key schema must contain exactly one HASH and at most one RANGE element".into(),
        ));
    }
    Ok((
        partition_key
            .ok_or_else(|| Error::InvalidRequest("HASH key schema element is required".into()))?,
        sort_key,
    ))
}

fn parse_projection(projection: Option<&Projection>) -> Result<SecondaryIndexProjection> {
    let projection = projection
        .ok_or_else(|| Error::InvalidRequest("secondary index projection is required".into()))?;
    let non_keys = projection.non_key_attributes();
    match projection.projection_type().map(ProjectionType::as_str) {
        Some("KEYS_ONLY") if non_keys.is_empty() => Ok(SecondaryIndexProjection::KeysOnly),
        Some("ALL") if non_keys.is_empty() => Ok(SecondaryIndexProjection::All),
        Some("INCLUDE") => {
            let included = non_keys.iter().cloned().collect::<BTreeSet<_>>();
            if included.len() != non_keys.len() {
                return Err(Error::InvalidRequest(
                    "secondary index projection attributes must be unique".into(),
                ));
            }
            Ok(SecondaryIndexProjection::Include(included))
        }
        Some("KEYS_ONLY" | "ALL") => Err(Error::InvalidRequest(
            "non-key attributes are valid only for INCLUDE projections".into(),
        )),
        Some(value) => Err(Error::Unsupported(format!(
            "unsupported secondary index projection type {value:?}"
        ))),
        None => Err(Error::InvalidRequest(
            "secondary index projection type is required".into(),
        )),
    }
}
