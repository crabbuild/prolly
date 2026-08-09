use std::collections::HashMap;

use aws_sdk_dynamodb::operation::get_item::GetItemOutput;
use aws_sdk_dynamodb::types::AttributeValue;
use prolly::MapVersionId;

use crate::conversion::{item_from_aws, item_to_aws, projection_from_aws};
use crate::{Client, Error, Result, WithMetadata};

#[derive(Clone)]
pub struct GetItem {
    client: Client,
    table_name: Option<String>,
    key: HashMap<String, AttributeValue>,
    projection_expression: Option<String>,
    expression_attribute_names: HashMap<String, String>,
    version: Option<MapVersionId>,
}

impl GetItem {
    pub(crate) fn new(client: Client, version: Option<MapVersionId>) -> Self {
        Self {
            client,
            table_name: None,
            key: HashMap::new(),
            projection_expression: None,
            expression_attribute_names: HashMap::new(),
            version,
        }
    }

    pub fn table_name(mut self, value: impl Into<String>) -> Self {
        self.table_name = Some(value.into());
        self
    }

    pub fn key(mut self, name: impl Into<String>, value: AttributeValue) -> Self {
        self.key.insert(name.into(), value);
        self
    }

    pub fn set_key(mut self, key: Option<HashMap<String, AttributeValue>>) -> Self {
        self.key = key.unwrap_or_default();
        self
    }

    pub fn projection_expression(mut self, value: impl Into<String>) -> Self {
        self.projection_expression = Some(value.into());
        self
    }

    pub fn set_projection_expression(mut self, value: Option<String>) -> Self {
        self.projection_expression = value;
        self
    }

    pub fn expression_attribute_names(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.expression_attribute_names
            .insert(name.into(), value.into());
        self
    }

    pub fn set_expression_attribute_names(
        mut self,
        values: Option<HashMap<String, String>>,
    ) -> Self {
        self.expression_attribute_names = values.unwrap_or_default();
        self
    }

    pub async fn send(self) -> Result<GetItemOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.GetItem",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "GetItem"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<GetItemOutput>> {
        let table = self
            .table_name
            .ok_or_else(|| Error::InvalidRequest("GetItem.table_name is required".into()))?;
        if self.key.is_empty() {
            return Err(Error::InvalidRequest("GetItem.key is required".into()));
        }
        let projection = match self.projection_expression {
            Some(expression) => Some(projection_from_aws(
                &expression,
                &self.expression_attribute_names,
            )?),
            None if self.expression_attribute_names.is_empty() => None,
            None => {
                return Err(Error::InvalidRequest(
                    "expression_attribute_names requires projection_expression".into(),
                ))
            }
        };
        let key = item_from_aws(self.key)?;
        let (mut item, version_id) = match &self.version {
            Some(version) => (
                self.client
                    .core()
                    .get_item_at(&table, version, &key)
                    .await?,
                version.clone(),
            ),
            None => {
                let read = self
                    .client
                    .core()
                    .get_item_with_version(&table, &key)
                    .await?;
                (read.item, read.version_id)
            }
        };
        if let (Some(item), Some(projection)) = (&mut item, projection) {
            *item = projection.apply(item);
        }
        Ok(WithMetadata::single(
            GetItemOutput::builder()
                .set_item(item.map(item_to_aws))
                .build(),
            table,
            Some(version_id),
        ))
    }
}
