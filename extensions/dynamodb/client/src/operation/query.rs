use std::collections::HashMap;

use aws_sdk_dynamodb::operation::query::QueryOutput;
use aws_sdk_dynamodb::types::{AttributeValue, Select};
use futures_util::stream::{self, Stream};
use prolly::MapVersionId;

use crate::conversion::{item_from_aws, item_to_aws, read_expressions_from_aws};
use crate::{Client, Error, Result, WithMetadata};

#[derive(Clone)]
pub struct Query {
    client: Client,
    table_name: Option<String>,
    index_name: Option<String>,
    key_condition_expression: Option<String>,
    filter_expression: Option<String>,
    projection_expression: Option<String>,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
    exclusive_start_key: Option<HashMap<String, AttributeValue>>,
    limit: Option<i32>,
    scan_index_forward: Option<bool>,
    select: Option<Select>,
    consistent_read: Option<bool>,
    version: Option<MapVersionId>,
}

impl Query {
    pub(crate) fn new(client: Client, version: Option<MapVersionId>) -> Self {
        Self {
            client,
            table_name: None,
            index_name: None,
            key_condition_expression: None,
            filter_expression: None,
            projection_expression: None,
            expression_attribute_names: HashMap::new(),
            expression_attribute_values: HashMap::new(),
            exclusive_start_key: None,
            limit: None,
            scan_index_forward: None,
            select: None,
            consistent_read: None,
            version,
        }
    }

    pub fn table_name(mut self, value: impl Into<String>) -> Self {
        self.table_name = Some(value.into());
        self
    }

    pub fn index_name(mut self, value: impl Into<String>) -> Self {
        self.index_name = Some(value.into());
        self
    }

    pub fn set_index_name(mut self, value: Option<String>) -> Self {
        self.index_name = value;
        self
    }

    pub fn key_condition_expression(mut self, value: impl Into<String>) -> Self {
        self.key_condition_expression = Some(value.into());
        self
    }

    pub fn set_key_condition_expression(mut self, value: Option<String>) -> Self {
        self.key_condition_expression = value;
        self
    }

    pub fn filter_expression(mut self, value: impl Into<String>) -> Self {
        self.filter_expression = Some(value.into());
        self
    }

    pub fn set_filter_expression(mut self, value: Option<String>) -> Self {
        self.filter_expression = value;
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

    pub fn exclusive_start_key(mut self, name: impl Into<String>, value: AttributeValue) -> Self {
        self.exclusive_start_key
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value);
        self
    }

    pub fn set_exclusive_start_key(
        mut self,
        value: Option<HashMap<String, AttributeValue>>,
    ) -> Self {
        self.exclusive_start_key = value;
        self
    }

    pub fn limit(mut self, value: i32) -> Self {
        self.limit = Some(value);
        self
    }

    pub fn set_limit(mut self, value: Option<i32>) -> Self {
        self.limit = value;
        self
    }

    pub fn scan_index_forward(mut self, value: bool) -> Self {
        self.scan_index_forward = Some(value);
        self
    }

    pub fn set_scan_index_forward(mut self, value: Option<bool>) -> Self {
        self.scan_index_forward = value;
        self
    }

    pub fn select(mut self, value: Select) -> Self {
        self.select = Some(value);
        self
    }

    pub fn set_select(mut self, value: Option<Select>) -> Self {
        self.select = value;
        self
    }

    pub fn consistent_read(mut self, value: bool) -> Self {
        self.consistent_read = Some(value);
        self
    }

    pub fn set_consistent_read(mut self, value: Option<bool>) -> Self {
        self.consistent_read = value;
        self
    }

    /// Convert this request into an explicit page iterator.
    pub fn into_paginator(self) -> QueryPaginator {
        QueryPaginator {
            request: self,
            finished: false,
        }
    }

    pub async fn send(self) -> Result<QueryOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.Query",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "Query"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<QueryOutput>> {
        let table = self
            .table_name
            .ok_or_else(|| Error::InvalidRequest("Query.table_name is required".into()))?;
        let expression = self.key_condition_expression.ok_or_else(|| {
            Error::InvalidRequest("Query.key_condition_expression is required".into())
        })?;
        let select = validate_select(self.select.as_ref(), self.projection_expression.is_some())?;
        let expressions = read_expressions_from_aws(
            Some(&expression),
            self.filter_expression.as_deref(),
            self.projection_expression.as_deref(),
            &self.expression_attribute_names,
            &self.expression_attribute_values,
        )?;
        let key_condition = expressions.key_condition.ok_or_else(|| {
            Error::InvalidRequest("Query.key_condition_expression is required".into())
        })?;
        let start = self.exclusive_start_key.map(item_from_aws).transpose()?;
        let limit = self.limit.unwrap_or(100);
        let scan_forward = self.scan_index_forward.unwrap_or(true);
        if limit <= 0 {
            return Err(Error::InvalidRequest("Query.limit must be positive".into()));
        }
        let (mut items, last_evaluated_key, version_id) = if let Some(index_name) = &self.index_name
        {
            let description = self.client.core().describe_table(&table).await?;
            let index = description
                .secondary_indexes
                .iter()
                .find(|index| &index.name == index_name)
                .ok_or_else(|| Error::InvalidRequest(format!("unknown index {index_name:?}")))?;
            if self.consistent_read == Some(true)
                && index.kind == prolly_dynamodb_core::SecondaryIndexKind::Global
            {
                return Err(Error::InvalidRequest(
                    "ConsistentRead=true is not supported on a global secondary index".into(),
                ));
            }
            if self.select.as_ref() == Some(&Select::AllAttributes)
                && index.projection != prolly_dynamodb_core::SecondaryIndexProjection::All
            {
                return Err(Error::Unsupported(
                    "ALL_ATTRIBUTES on a non-ALL secondary index is not implemented; use ALL_PROJECTED_ATTRIBUTES or SPECIFIC_ATTRIBUTES"
                        .into(),
                ));
            }
            let page = self
                .client
                .core()
                .query_index(
                    &table,
                    index_name,
                    prolly_dynamodb_core::IndexQueryRequest::new(&key_condition, limit as usize)
                        .at(self.version.as_ref())
                        .after(start.as_ref())
                        .forward(scan_forward),
                )
                .await?;
            (page.items, page.last_evaluated_key, page.base_version_id)
        } else {
            let page = match &self.version {
                Some(version) => {
                    self.client
                        .core()
                        .query_key_condition_at_ordered(
                            &table,
                            version,
                            &key_condition,
                            start.as_ref(),
                            limit as usize,
                            scan_forward,
                        )
                        .await?
                }
                None => {
                    self.client
                        .core()
                        .query_key_condition_ordered(
                            &table,
                            &key_condition,
                            start.as_ref(),
                            limit as usize,
                            scan_forward,
                        )
                        .await?
                }
            };
            (page.items, page.last_evaluated_key, page.version_id)
        };
        let scanned_count = items.len() as i32;
        if let Some(filter) = &expressions.filter {
            let mut filtered = Vec::with_capacity(items.len());
            for item in items {
                if filter.evaluate(Some(&item))? {
                    filtered.push(item);
                }
            }
            items = filtered;
        }
        if let Some(projection) = &expressions.projection {
            items = items
                .into_iter()
                .map(|item| projection.apply(&item))
                .collect();
        }
        let count = items.len() as i32;
        Ok(WithMetadata::single(
            QueryOutput::builder()
                .set_items(
                    (select != SelectMode::Count)
                        .then(|| items.into_iter().map(item_to_aws).collect()),
                )
                .count(count)
                .scanned_count(scanned_count)
                .set_last_evaluated_key(last_evaluated_key.map(item_to_aws))
                .build(),
            table,
            Some(version_id),
        ))
    }
}

/// Stateful Query paginator. Each page exposes the exact immutable version it
/// observed through [`WithMetadata`]. Historical requests stay on their pinned
/// version; current-head requests intentionally resolve a new head per page.
pub struct QueryPaginator {
    request: Query,
    finished: bool,
}

impl QueryPaginator {
    pub async fn next_page(&mut self) -> Result<Option<WithMetadata<QueryOutput>>> {
        if self.finished {
            return Ok(None);
        }
        let previous = self.request.exclusive_start_key.clone();
        let page = self.request.clone().send_with_metadata().await?;
        let next = page.output.last_evaluated_key.clone();
        match next {
            Some(next) => {
                if previous.as_ref() == Some(&next) {
                    return Err(Error::Core(prolly_dynamodb_core::Error::CorruptData(
                        "Query paginator did not advance its continuation key".into(),
                    )));
                }
                self.request.exclusive_start_key = Some(next);
            }
            None => self.finished = true,
        }
        Ok(Some(page))
    }

    /// Consume this paginator as a fallible asynchronous page stream.
    pub fn into_stream(
        self,
    ) -> impl Stream<Item = Result<WithMetadata<QueryOutput>>> + Send + 'static {
        stream::try_unfold(self, |mut paginator| async move {
            match paginator.next_page().await? {
                Some(page) => Ok(Some((page, paginator))),
                None => Ok(None),
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SelectMode {
    Items,
    Count,
}

fn validate_select(select: Option<&Select>, has_projection: bool) -> Result<SelectMode> {
    match select.map(Select::as_str) {
        None => Ok(SelectMode::Items),
        Some("COUNT") if !has_projection => Ok(SelectMode::Count),
        Some("ALL_ATTRIBUTES" | "ALL_PROJECTED_ATTRIBUTES") if !has_projection => {
            Ok(SelectMode::Items)
        }
        Some("SPECIFIC_ATTRIBUTES") if has_projection => Ok(SelectMode::Items),
        Some("COUNT" | "ALL_ATTRIBUTES" | "ALL_PROJECTED_ATTRIBUTES") => Err(
            Error::InvalidRequest(
                "Select COUNT/ALL_ATTRIBUTES/ALL_PROJECTED_ATTRIBUTES cannot be combined with ProjectionExpression"
                    .into(),
            ),
        ),
        Some("SPECIFIC_ATTRIBUTES") => Err(Error::InvalidRequest(
            "Select SPECIFIC_ATTRIBUTES requires ProjectionExpression".into(),
        )),
        Some(value) => Err(Error::Unsupported(format!(
            "unsupported Query Select value {value:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_requires_a_consistent_projection_contract() {
        assert_eq!(validate_select(None, false).unwrap(), SelectMode::Items);
        assert_eq!(
            validate_select(Some(&Select::Count), false).unwrap(),
            SelectMode::Count
        );
        assert_eq!(
            validate_select(Some(&Select::SpecificAttributes), true).unwrap(),
            SelectMode::Items
        );
        assert!(matches!(
            validate_select(Some(&Select::Count), true),
            Err(Error::InvalidRequest(_))
        ));
        assert!(matches!(
            validate_select(Some(&Select::SpecificAttributes), false),
            Err(Error::InvalidRequest(_))
        ));
    }
}
