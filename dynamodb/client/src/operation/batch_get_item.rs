use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemOutput;
use aws_sdk_dynamodb::types::{KeysAndAttributes, ReturnConsumedCapacity};
use prolly::MapVersionId;
use prolly_dynamodb_core::BatchGetTableRequest;

use crate::conversion::{item_from_aws, item_to_aws, projection_from_aws};
use crate::{Client, Error, Result, WithMetadata};

/// AWS SDK-shaped multi-table point-read builder.
#[derive(Clone)]
pub struct BatchGetItem {
    client: Client,
    request_items: HashMap<String, KeysAndAttributes>,
    return_consumed_capacity: Option<ReturnConsumedCapacity>,
    versions: BTreeMap<String, MapVersionId>,
}

impl BatchGetItem {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            request_items: HashMap::new(),
            return_consumed_capacity: None,
            versions: BTreeMap::new(),
        }
    }

    pub fn request_items(mut self, table: impl Into<String>, request: KeysAndAttributes) -> Self {
        self.request_items.insert(table.into(), request);
        self
    }

    pub fn set_request_items(
        mut self,
        requests: Option<HashMap<String, KeysAndAttributes>>,
    ) -> Self {
        self.request_items = requests.unwrap_or_default();
        self
    }

    pub fn return_consumed_capacity(mut self, value: ReturnConsumedCapacity) -> Self {
        self.return_consumed_capacity = Some(value);
        self
    }

    pub fn set_return_consumed_capacity(mut self, value: Option<ReturnConsumedCapacity>) -> Self {
        self.return_consumed_capacity = value;
        self
    }

    /// Pin selected request tables to exact immutable versions.
    pub fn at<I>(mut self, versions: I) -> Self
    where
        I: IntoIterator<Item = (String, MapVersionId)>,
    {
        self.versions = versions.into_iter().collect();
        self
    }

    pub async fn send(self) -> Result<BatchGetItemOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.BatchGetItem",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "BatchGetItem"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<BatchGetItemOutput>> {
        if self
            .return_consumed_capacity
            .as_ref()
            .is_some_and(|value| value != &ReturnConsumedCapacity::None)
        {
            return Err(Error::Unsupported(
                "BatchGetItem consumed-capacity reporting is not implemented".into(),
            ));
        }
        if let Some(table) = self
            .versions
            .keys()
            .find(|table| !self.request_items.contains_key(*table))
        {
            return Err(Error::InvalidRequest(format!(
                "BatchGetItem version pin refers to unrequested table {table:?}"
            )));
        }

        let mut requests = BTreeMap::new();
        let mut settings = BTreeMap::new();
        for (table, request) in self.request_items {
            if request.attributes_to_get.is_some() {
                return Err(Error::Unsupported(format!(
                    "BatchGetItem legacy AttributesToGet is unsupported for table {table:?}"
                )));
            }
            let names = request
                .expression_attribute_names
                .clone()
                .unwrap_or_default();
            let projection = match request.projection_expression.as_deref() {
                Some(expression) => Some(projection_from_aws(expression, &names)?),
                None if names.is_empty() => None,
                None => {
                    return Err(Error::InvalidRequest(format!(
                        "BatchGetItem ExpressionAttributeNames requires ProjectionExpression for table {table:?}"
                    )))
                }
            };
            let keys = request
                .keys
                .iter()
                .cloned()
                .map(item_from_aws)
                .collect::<Result<Vec<_>>>()?;
            settings.insert(
                table.clone(),
                (
                    request.projection_expression,
                    request.expression_attribute_names,
                    request.consistent_read,
                ),
            );
            requests.insert(
                table.clone(),
                BatchGetTableRequest {
                    keys,
                    projection,
                    version: self.versions.get(&table).cloned(),
                },
            );
        }

        let result = self.client.core().batch_get(requests).await?;
        let mut responses = HashMap::with_capacity(result.tables.len());
        let mut unprocessed = HashMap::new();
        let mut versions = BTreeMap::new();
        for (table, table_result) in result.tables {
            versions.insert(table.clone(), table_result.version_id);
            responses.insert(
                table.clone(),
                table_result.items.into_iter().map(item_to_aws).collect(),
            );
            if !table_result.unprocessed_keys.is_empty() {
                let (projection, names, consistent_read) =
                    settings.remove(&table).ok_or_else(|| {
                        Error::InvalidRequest(format!(
                            "internal BatchGetItem settings missing for table {table:?}"
                        ))
                    })?;
                let request = KeysAndAttributes::builder()
                    .set_keys(Some(
                        table_result
                            .unprocessed_keys
                            .into_iter()
                            .map(item_to_aws)
                            .collect(),
                    ))
                    .set_projection_expression(projection)
                    .set_expression_attribute_names(names)
                    .set_consistent_read(consistent_read)
                    .build()
                    .map_err(|error| Error::InvalidRequest(error.to_string()))?;
                unprocessed.insert(table, request);
            }
        }
        Ok(WithMetadata::multiple(
            BatchGetItemOutput::builder()
                .set_responses(Some(responses))
                .set_unprocessed_keys(Some(unprocessed))
                .build(),
            versions,
        ))
    }
}
