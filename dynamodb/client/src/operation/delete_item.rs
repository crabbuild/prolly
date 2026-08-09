use std::collections::HashMap;

use aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue, ReturnValuesOnConditionCheckFailure};
use prolly::{MapVersionId, VersionedMapUpdate};

use crate::conversion::{
    condition_from_aws, conditional_error_from_core, item_from_aws, item_to_aws, return_failure_old,
};
use crate::{Client, Error, Result, WithMetadata};

#[derive(Clone)]
pub struct DeleteItem {
    client: Client,
    table_name: Option<String>,
    key: HashMap<String, AttributeValue>,
    condition_expression: Option<String>,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
    return_values: Option<ReturnValue>,
    return_values_on_condition_check_failure: Option<ReturnValuesOnConditionCheckFailure>,
    expected_head: Option<MapVersionId>,
    request_token: Option<String>,
}

impl DeleteItem {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            table_name: None,
            key: HashMap::new(),
            condition_expression: None,
            expression_attribute_names: HashMap::new(),
            expression_attribute_values: HashMap::new(),
            return_values: None,
            return_values_on_condition_check_failure: None,
            expected_head: None,
            request_token: None,
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

    pub fn condition_expression(mut self, value: impl Into<String>) -> Self {
        self.condition_expression = Some(value.into());
        self
    }

    pub fn set_condition_expression(mut self, value: Option<String>) -> Self {
        self.condition_expression = value;
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

    pub fn expression_attribute_values(
        mut self,
        name: impl Into<String>,
        value: AttributeValue,
    ) -> Self {
        self.expression_attribute_values.insert(name.into(), value);
        self
    }

    pub fn set_expression_attribute_values(
        mut self,
        values: Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        self.expression_attribute_values = values.unwrap_or_default();
        self
    }

    pub fn return_values(mut self, value: ReturnValue) -> Self {
        self.return_values = Some(value);
        self
    }

    pub fn set_return_values(mut self, value: Option<ReturnValue>) -> Self {
        self.return_values = value;
        self
    }

    pub fn return_values_on_condition_check_failure(
        mut self,
        value: ReturnValuesOnConditionCheckFailure,
    ) -> Self {
        self.return_values_on_condition_check_failure = Some(value);
        self
    }

    pub fn set_return_values_on_condition_check_failure(
        mut self,
        value: Option<ReturnValuesOnConditionCheckFailure>,
    ) -> Self {
        self.return_values_on_condition_check_failure = value;
        self
    }

    /// Require the logical table head to equal this immutable version.
    pub fn expected_head(mut self, version: MapVersionId) -> Self {
        self.expected_head = Some(version);
        self
    }

    /// Add durable ten-minute replay protection for this logical write.
    pub fn request_token(mut self, token: impl Into<String>) -> Self {
        self.request_token = Some(token.into());
        self
    }

    pub fn set_request_token(mut self, token: Option<String>) -> Self {
        self.request_token = token;
        self
    }

    pub async fn send(self) -> Result<DeleteItemOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.DeleteItem",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "DeleteItem"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<DeleteItemOutput>> {
        let table = self
            .table_name
            .ok_or_else(|| Error::InvalidRequest("DeleteItem.table_name is required".into()))?;
        if self.key.is_empty() {
            return Err(Error::InvalidRequest("DeleteItem.key is required".into()));
        }
        let return_old = match self.return_values.as_ref().map(ReturnValue::as_str) {
            None | Some("NONE") => false,
            Some("ALL_OLD") => true,
            Some(value) => {
                return Err(Error::Unsupported(format!(
                    "unsupported DeleteItem ReturnValues value {value:?}"
                )))
            }
        };
        let return_failure_old =
            return_failure_old(self.return_values_on_condition_check_failure.as_ref())?;
        let key = item_from_aws(self.key)?;
        let expected_head = self.expected_head;
        let condition = match self.condition_expression {
            Some(expression) => Some(condition_from_aws(
                &expression,
                &self.expression_attribute_names,
                &self.expression_attribute_values,
            )?),
            None if self.expression_attribute_names.is_empty()
                && self.expression_attribute_values.is_empty() =>
            {
                None
            }
            None => {
                return Err(Error::InvalidRequest(
                    "expression placeholders require condition_expression".into(),
                ))
            }
        };
        let result = if let Some(token) = self.request_token {
            self.client
                .core()
                .delete_item_idempotent(
                    &table,
                    &key,
                    expected_head.as_ref(),
                    condition.as_ref(),
                    &token,
                    return_old,
                )
                .await
                .map_err(|error| conditional_error_from_core(error, return_failure_old))?
        } else {
            match condition {
                Some(condition) => {
                    if return_old {
                        self.client
                            .core()
                            .delete_item_conditionally_with_old(
                                &table,
                                &key,
                                expected_head.as_ref(),
                                &condition,
                            )
                            .await
                            .map_err(|error| {
                                conditional_error_from_core(error, return_failure_old)
                            })?
                    } else {
                        self.client
                            .core()
                            .delete_item_conditionally_result(
                                &table,
                                &key,
                                expected_head.as_ref(),
                                &condition,
                            )
                            .await
                            .map_err(|error| {
                                conditional_error_from_core(error, return_failure_old)
                            })?
                    }
                }
                None => {
                    if return_old {
                        self.client
                            .core()
                            .delete_item_with_old(&table, &key, expected_head.as_ref())
                            .await
                            .map_err(|error| {
                                conditional_error_from_core(error, return_failure_old)
                            })?
                    } else {
                        self.client
                            .core()
                            .delete_item_result(&table, &key, expected_head.as_ref())
                            .await
                            .map_err(|error| {
                                conditional_error_from_core(error, return_failure_old)
                            })?
                    }
                }
            }
        };
        let transition = crate::TableTransitionMetadata::from_update(
            table.clone(),
            &result.update,
            result.commit_id.clone(),
            result.table_id,
        );
        let version_id = match result.update {
            VersionedMapUpdate::Applied { current, .. } => Some(current.id),
            VersionedMapUpdate::Unchanged { current } => current.map(|version| version.id),
            VersionedMapUpdate::Conflict { current } => {
                let expected = expected_head.ok_or_else(|| {
                    Error::Core(prolly_dynamodb_core::Error::CorruptData(
                        "core returned a head conflict without an expected head".into(),
                    ))
                })?;
                return Err(Error::HeadConflict {
                    expected,
                    current: current.map(|version| version.id),
                });
            }
        };
        let attributes = if return_old {
            result.old_item.map(item_to_aws)
        } else {
            None
        };
        Ok(WithMetadata::single_write(
            DeleteItemOutput::builder()
                .set_attributes(attributes)
                .build(),
            table,
            version_id,
            result.commit_id,
            transition,
        ))
    }
}
