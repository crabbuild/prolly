use aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsOutput;
use aws_sdk_dynamodb::types::{ItemResponse, ReturnConsumedCapacity, TransactGetItem};
use prolly_dynamodb_core::TransactGetRequest;

use crate::conversion::{item_from_aws, item_to_aws, projection_from_aws};
use crate::{Client, Error, Result, WithMetadata};

/// AWS SDK-shaped atomic multi-table read builder.
#[derive(Clone)]
pub struct TransactGetItems {
    client: Client,
    transact_items: Vec<TransactGetItem>,
    return_consumed_capacity: Option<ReturnConsumedCapacity>,
}

impl TransactGetItems {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            transact_items: Vec::new(),
            return_consumed_capacity: None,
        }
    }

    pub fn transact_items(mut self, item: TransactGetItem) -> Self {
        self.transact_items.push(item);
        self
    }

    pub fn set_transact_items(mut self, items: Option<Vec<TransactGetItem>>) -> Self {
        self.transact_items = items.unwrap_or_default();
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

    pub async fn send(self) -> Result<TransactGetItemsOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.TransactGetItems",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "TransactGetItems"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<TransactGetItemsOutput>> {
        if self
            .return_consumed_capacity
            .as_ref()
            .is_some_and(|value| value != &ReturnConsumedCapacity::None)
        {
            return Err(Error::Unsupported(
                "TransactGetItems consumed-capacity reporting is not implemented".into(),
            ));
        }
        let mut requests = Vec::with_capacity(self.transact_items.len());
        for (index, item) in self.transact_items.into_iter().enumerate() {
            let get = item.get.ok_or_else(|| {
                Error::InvalidRequest(format!(
                    "TransactGetItems item {index} does not contain Get"
                ))
            })?;
            let names = get.expression_attribute_names.unwrap_or_default();
            let projection = match get.projection_expression.as_deref() {
                Some(expression) => Some(projection_from_aws(expression, &names)?),
                None if names.is_empty() => None,
                None => {
                    return Err(Error::InvalidRequest(format!(
                        "TransactGetItems item {index} supplies ExpressionAttributeNames without ProjectionExpression"
                    )))
                }
            };
            requests.push(TransactGetRequest {
                table_name: get.table_name,
                key: item_from_aws(get.key)?,
                projection,
            });
        }

        let result = self.client.core().transact_get(requests).await?;
        let responses = result
            .responses
            .into_iter()
            .map(|response| {
                ItemResponse::builder()
                    .set_item(response.item.map(item_to_aws))
                    .build()
            })
            .collect();
        Ok(WithMetadata::multiple(
            TransactGetItemsOutput::builder()
                .set_responses(Some(responses))
                .build(),
            result.table_versions,
        ))
    }
}
