use std::collections::HashMap;

use aws_sdk_dynamodb::operation::update_item::UpdateItemOutput;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue, ReturnValuesOnConditionCheckFailure};
use prolly::{MapVersionId, VersionedMapUpdate};

use crate::conversion::{
    conditional_error_from_core, item_from_aws, item_to_aws, return_failure_old, update_from_aws,
};
use crate::{Client, Error, Result, WithMetadata};

#[derive(Clone)]
pub struct UpdateItem {
    client: Client,
    table_name: Option<String>,
    key: HashMap<String, AttributeValue>,
    update_expression: Option<String>,
    condition_expression: Option<String>,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
    return_values: Option<ReturnValue>,
    return_values_on_condition_check_failure: Option<ReturnValuesOnConditionCheckFailure>,
    expected_head: Option<MapVersionId>,
    request_token: Option<String>,
}

impl UpdateItem {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            table_name: None,
            key: HashMap::new(),
            update_expression: None,
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

    pub fn update_expression(mut self, value: impl Into<String>) -> Self {
        self.update_expression = Some(value.into());
        self
    }

    pub fn set_update_expression(mut self, value: Option<String>) -> Self {
        self.update_expression = value;
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

    pub async fn send(self) -> Result<UpdateItemOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.UpdateItem",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "UpdateItem"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<UpdateItemOutput>> {
        let table = self
            .table_name
            .ok_or_else(|| Error::InvalidRequest("UpdateItem.table_name is required".into()))?;
        if self.key.is_empty() {
            return Err(Error::InvalidRequest("UpdateItem.key is required".into()));
        }
        let update_expression = self.update_expression.ok_or_else(|| {
            Error::InvalidRequest("UpdateItem.update_expression is required".into())
        })?;
        let key = item_from_aws(self.key)?;
        let parsed = update_from_aws(
            &update_expression,
            self.condition_expression.as_deref(),
            &self.expression_attribute_names,
            &self.expression_attribute_values,
        )?;
        validate_return_values(self.return_values.as_ref())?;
        let return_failure_old =
            return_failure_old(self.return_values_on_condition_check_failure.as_ref())?;
        let expected_head = self.expected_head;
        let result = match self.request_token {
            Some(token) => {
                self.client
                    .core()
                    .update_item_idempotent(
                        &table,
                        &key,
                        expected_head.as_ref(),
                        parsed.condition.as_ref(),
                        &parsed.plan,
                        &token,
                    )
                    .await
            }
            None => {
                self.client
                    .core()
                    .update_item(
                        &table,
                        &key,
                        expected_head.as_ref(),
                        parsed.condition.as_ref(),
                        &parsed.plan,
                    )
                    .await
            }
        }
        .map_err(|error| conditional_error_from_core(error, return_failure_old))?;
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
        let attributes = return_attributes(
            self.return_values.as_ref(),
            result.old_item,
            result.new_item,
            &parsed.plan,
        )?;
        Ok(WithMetadata::single_write(
            UpdateItemOutput::builder()
                .set_attributes(attributes.map(item_to_aws))
                .build(),
            table,
            version_id,
            result.commit_id,
            transition,
        ))
    }
}

fn validate_return_values(return_values: Option<&ReturnValue>) -> Result<()> {
    match return_values.map(ReturnValue::as_str).unwrap_or("NONE") {
        "NONE" | "ALL_OLD" | "ALL_NEW" | "UPDATED_OLD" | "UPDATED_NEW" => Ok(()),
        value => Err(Error::Unsupported(format!(
            "unsupported UpdateItem ReturnValues value {value:?}"
        ))),
    }
}

fn return_attributes(
    return_values: Option<&ReturnValue>,
    old: Option<prolly_dynamodb_core::Item>,
    new: Option<prolly_dynamodb_core::Item>,
    plan: &prolly_dynamodb_core::UpdatePlan,
) -> Result<Option<prolly_dynamodb_core::Item>> {
    validate_return_values(return_values)?;
    match return_values.map(ReturnValue::as_str).unwrap_or("NONE") {
        "NONE" => Ok(None),
        "ALL_OLD" => Ok(old),
        "ALL_NEW" => Ok(new),
        "UPDATED_OLD" => Ok(select_targets(old, plan)),
        "UPDATED_NEW" => Ok(select_targets(new, plan)),
        _ => unreachable!("validated above"),
    }
}

fn select_targets(
    item: Option<prolly_dynamodb_core::Item>,
    plan: &prolly_dynamodb_core::UpdatePlan,
) -> Option<prolly_dynamodb_core::Item> {
    let item = item?;
    Some(plan.project_targets(&item))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use prolly_dynamodb_core::{parse_update, AttributeValue};

    use super::*;

    #[test]
    fn updated_return_images_include_only_action_targets() {
        let plan = parse_update(
            "SET #state = :closed REMOVE #obsolete",
            None,
            &BTreeMap::from([
                ("#state".into(), "state".into()),
                ("#obsolete".into(), "obsolete".into()),
            ]),
            &BTreeMap::from([(":closed".into(), AttributeValue::S("CLOSED".into()))]),
        )
        .unwrap()
        .plan;
        let old = BTreeMap::from([
            ("pk".into(), AttributeValue::S("1".into())),
            ("state".into(), AttributeValue::S("OPEN".into())),
            ("obsolete".into(), AttributeValue::Bool(true)),
        ]);
        let new = plan.apply(&old, ["pk"]).unwrap();
        assert_eq!(
            return_attributes(
                Some(&ReturnValue::UpdatedOld),
                Some(old),
                Some(new.clone()),
                &plan
            )
            .unwrap()
            .unwrap(),
            BTreeMap::from([
                ("state".into(), AttributeValue::S("OPEN".into())),
                ("obsolete".into(), AttributeValue::Bool(true)),
            ])
        );
        assert_eq!(
            return_attributes(Some(&ReturnValue::UpdatedNew), None, Some(new), &plan)
                .unwrap()
                .unwrap(),
            BTreeMap::from([("state".into(), AttributeValue::S("CLOSED".into()))])
        );
    }
}
