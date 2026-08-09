use std::collections::HashMap;

use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsOutput;
use aws_sdk_dynamodb::types::{
    CancellationReason, ReturnConsumedCapacity, ReturnItemCollectionMetrics, TransactWriteItem,
};
use prolly_dynamodb_core::{
    IdGenerator, SystemIdGenerator, TransactWriteAction, TransactionCancellationCode,
    TransactionCancellationReason,
};

use crate::conversion::{
    condition_from_aws, item_from_aws, item_to_aws, return_failure_old, update_from_aws,
};
use crate::{Client, Error, Result, TableTransitionMetadata, WithMetadata};

/// AWS SDK-shaped atomic transaction-write builder.
#[derive(Clone)]
pub struct TransactWriteItems {
    client: Client,
    transact_items: Vec<TransactWriteItem>,
    client_request_token: Option<String>,
    return_consumed_capacity: Option<ReturnConsumedCapacity>,
    return_item_collection_metrics: Option<ReturnItemCollectionMetrics>,
}

impl TransactWriteItems {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            transact_items: Vec::new(),
            client_request_token: None,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        }
    }

    pub fn transact_items(mut self, item: TransactWriteItem) -> Self {
        self.transact_items.push(item);
        self
    }

    pub fn set_transact_items(mut self, items: Option<Vec<TransactWriteItem>>) -> Self {
        self.transact_items = items.unwrap_or_default();
        self
    }

    pub fn client_request_token(mut self, token: impl Into<String>) -> Self {
        self.client_request_token = Some(token.into());
        self
    }

    pub fn set_client_request_token(mut self, token: Option<String>) -> Self {
        self.client_request_token = token;
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

    pub fn return_item_collection_metrics(mut self, value: ReturnItemCollectionMetrics) -> Self {
        self.return_item_collection_metrics = Some(value);
        self
    }

    pub fn set_return_item_collection_metrics(
        mut self,
        value: Option<ReturnItemCollectionMetrics>,
    ) -> Self {
        self.return_item_collection_metrics = value;
        self
    }

    pub async fn send(self) -> Result<TransactWriteItemsOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.TransactWriteItems",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "TransactWriteItems"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<TransactWriteItemsOutput>> {
        if self
            .return_consumed_capacity
            .as_ref()
            .is_some_and(|value| value != &ReturnConsumedCapacity::None)
        {
            return Err(Error::Unsupported(
                "TransactWriteItems consumed-capacity reporting is not implemented".into(),
            ));
        }
        if self
            .return_item_collection_metrics
            .as_ref()
            .is_some_and(|value| value != &ReturnItemCollectionMetrics::None)
        {
            return Err(Error::Unsupported(
                "TransactWriteItems item-collection metrics are not implemented".into(),
            ));
        }

        let mut actions = Vec::with_capacity(self.transact_items.len());
        for (index, item) in self.transact_items.into_iter().enumerate() {
            let operation_count = usize::from(item.put.is_some())
                + usize::from(item.delete.is_some())
                + usize::from(item.update.is_some())
                + usize::from(item.condition_check.is_some());
            if operation_count != 1 {
                return Err(Error::InvalidRequest(format!(
                    "TransactWriteItems item {index} must contain exactly one operation"
                )));
            }
            let action = if let Some(put) = item.put {
                let (condition, return_failure_old) = optional_condition(
                    put.condition_expression.as_deref(),
                    put.expression_attribute_names.unwrap_or_default(),
                    put.expression_attribute_values.unwrap_or_default(),
                    put.return_values_on_condition_check_failure.as_ref(),
                )?;
                TransactWriteAction::Put {
                    table_name: put.table_name,
                    item: item_from_aws(put.item)?,
                    condition,
                    return_failure_old,
                }
            } else if let Some(delete) = item.delete {
                let (condition, return_failure_old) = optional_condition(
                    delete.condition_expression.as_deref(),
                    delete.expression_attribute_names.unwrap_or_default(),
                    delete.expression_attribute_values.unwrap_or_default(),
                    delete.return_values_on_condition_check_failure.as_ref(),
                )?;
                TransactWriteAction::Delete {
                    table_name: delete.table_name,
                    key: item_from_aws(delete.key)?,
                    condition,
                    return_failure_old,
                }
            } else if let Some(update) = item.update {
                let names = update.expression_attribute_names.unwrap_or_default();
                let values = update.expression_attribute_values.unwrap_or_default();
                let parsed = update_from_aws(
                    &update.update_expression,
                    update.condition_expression.as_deref(),
                    &names,
                    &values,
                )?;
                TransactWriteAction::Update {
                    table_name: update.table_name,
                    key: item_from_aws(update.key)?,
                    condition: parsed.condition,
                    plan: parsed.plan,
                    return_failure_old: return_failure_old(
                        update.return_values_on_condition_check_failure.as_ref(),
                    )?,
                }
            } else {
                let check = item.condition_check.expect("operation count validated");
                let names = check.expression_attribute_names.unwrap_or_default();
                let values = check.expression_attribute_values.unwrap_or_default();
                TransactWriteAction::ConditionCheck {
                    table_name: check.table_name,
                    key: item_from_aws(check.key)?,
                    condition: condition_from_aws(&check.condition_expression, &names, &values)?,
                    return_failure_old: return_failure_old(
                        check.return_values_on_condition_check_failure.as_ref(),
                    )?,
                }
            };
            actions.push(action);
        }

        // The official AWS Rust SDK installs an idempotency token when the
        // caller omits one. Preserve that safety property for local execution.
        let client_request_token = match self.client_request_token {
            Some(token) => token,
            None => automatic_request_token()?,
        };
        let result = match self
            .client
            .core()
            .transact_write_idempotent(actions, Some(&client_request_token))
            .await
        {
            Ok(result) => result,
            Err(prolly_dynamodb_core::Error::TransactionCanceled { reasons }) => {
                return Err(Error::TransactionCanceled {
                    cancellation_reasons: reasons.into_iter().map(cancellation_reason).collect(),
                })
            }
            Err(error) => return Err(Error::Core(error)),
        };
        let transitions = result
            .transitions
            .into_iter()
            .map(|transition| TableTransitionMetadata {
                commit_id: Some(result.commit_id.clone()),
                table_name: transition.table_name,
                table_id: Some(transition.table_id),
                before: transition.before,
                after: transition.after,
                applied: transition.applied,
            })
            .collect();
        Ok(WithMetadata::transaction_writes(
            TransactWriteItemsOutput::builder().build(),
            result.commit_id,
            result.table_versions,
            transitions,
        ))
    }
}

fn automatic_request_token() -> Result<String> {
    use std::fmt::Write;

    let id = SystemIdGenerator.generate().map_err(Error::Core)?;
    let mut token = String::with_capacity(32);
    for byte in &id.0[..16] {
        write!(&mut token, "{byte:02x}").expect("writing to String is infallible");
    }
    Ok(token)
}

fn optional_condition(
    expression: Option<&str>,
    names: HashMap<String, String>,
    values: HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    return_values: Option<&aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure>,
) -> Result<(Option<prolly_dynamodb_core::Condition>, bool)> {
    let condition = match expression {
        Some(expression) => Some(condition_from_aws(expression, &names, &values)?),
        None if names.is_empty() && values.is_empty() => None,
        None => {
            return Err(Error::InvalidRequest(
                "transaction expression bindings require ConditionExpression".into(),
            ))
        }
    };
    Ok((condition, return_failure_old(return_values)?))
}

fn cancellation_reason(reason: TransactionCancellationReason) -> CancellationReason {
    let code = reason.code.map(|code| match code {
        TransactionCancellationCode::ConditionalCheckFailed => "ConditionalCheckFailed",
        TransactionCancellationCode::TransactionConflict => "TransactionConflict",
    });
    CancellationReason::builder()
        .set_code(code.map(str::to_owned))
        .set_message(reason.message)
        .set_item(reason.item.map(item_to_aws))
        .build()
}
