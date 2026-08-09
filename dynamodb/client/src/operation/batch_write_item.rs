use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
use aws_sdk_dynamodb::types::{ReturnConsumedCapacity, ReturnItemCollectionMetrics, WriteRequest};
use prolly_dynamodb_core::BatchWriteAction;

use crate::conversion::item_from_aws;
use crate::error::{classify_write_error, WriteErrorDisposition};
use crate::{
    BatchWriteFailure, BatchWriteFailureDisposition, Client, Error, Result,
    TableTransitionMetadata, WithMetadata,
};

/// AWS SDK-shaped non-atomic batch-write builder.
#[derive(Clone)]
pub struct BatchWriteItem {
    client: Client,
    request_items: HashMap<String, Vec<WriteRequest>>,
    return_consumed_capacity: Option<ReturnConsumedCapacity>,
    return_item_collection_metrics: Option<ReturnItemCollectionMetrics>,
}

impl BatchWriteItem {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            request_items: HashMap::new(),
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
        }
    }

    pub fn request_items(mut self, table: impl Into<String>, requests: Vec<WriteRequest>) -> Self {
        self.request_items.insert(table.into(), requests);
        self
    }

    pub fn set_request_items(
        mut self,
        requests: Option<HashMap<String, Vec<WriteRequest>>>,
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

    pub async fn send(self) -> Result<BatchWriteItemOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.BatchWriteItem",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "BatchWriteItem"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<BatchWriteItemOutput>> {
        if self
            .return_consumed_capacity
            .as_ref()
            .is_some_and(|value| value != &ReturnConsumedCapacity::None)
        {
            return Err(Error::Unsupported(
                "BatchWriteItem consumed-capacity reporting is not implemented".into(),
            ));
        }
        if self
            .return_item_collection_metrics
            .as_ref()
            .is_some_and(|value| value != &ReturnItemCollectionMetrics::None)
        {
            return Err(Error::Unsupported(
                "BatchWriteItem item-collection metrics are not implemented".into(),
            ));
        }

        let mut prepared = BTreeMap::<String, Vec<(WriteRequest, BatchWriteAction)>>::new();
        for (table, requests) in self.request_items {
            let mut actions = Vec::with_capacity(requests.len());
            for request in requests {
                let action = match (&request.put_request, &request.delete_request) {
                    (Some(put), None) => BatchWriteAction::Put(item_from_aws(put.item.clone())?),
                    (None, Some(delete)) => {
                        BatchWriteAction::Delete(item_from_aws(delete.key.clone())?)
                    }
                    (Some(_), Some(_)) => {
                        return Err(Error::InvalidRequest(format!(
                            "BatchWriteItem request for table {table:?} contains both PutRequest and DeleteRequest"
                        )))
                    }
                    (None, None) => {
                        return Err(Error::InvalidRequest(format!(
                            "BatchWriteItem request for table {table:?} contains no operation"
                        )))
                    }
                };
                actions.push((request, action));
            }
            prepared.insert(table, actions);
        }
        let core_requests = prepared
            .iter()
            .map(|(table, actions)| {
                (
                    table.clone(),
                    actions.iter().map(|(_, action)| action.clone()).collect(),
                )
            })
            .collect();
        match self.client.core().batch_write(core_requests).await {
            Ok(result) => {
                let (transitions, table_versions) = transition_metadata(result.transitions);
                Ok(WithMetadata::multiple_writes(
                    BatchWriteItemOutput::builder()
                        .set_unprocessed_items(Some(HashMap::new()))
                        .build(),
                    table_versions,
                    transitions,
                ))
            }
            Err(prolly_dynamodb_core::BatchWriteExecutionError::Validation { source }) => {
                Err(Error::from(source))
            }
            Err(prolly_dynamodb_core::BatchWriteExecutionError::Partial {
                table_name,
                action_index,
                applied_transitions,
                source,
            }) => {
                let failed_request = prepared
                    .get(&table_name)
                    .and_then(|actions| actions.get(action_index))
                    .map(|(request, _)| request.clone())
                    .ok_or_else(|| {
                        Error::InvalidRequest(format!(
                            "core BatchWriteItem failure target {table_name:?}[{action_index}] is invalid"
                        ))
                    })?;
                let unattempted = unattempted_items(&prepared, &table_name, action_index);
                let (transitions, table_versions) = transition_metadata(applied_transitions);
                let cause = Error::from(source);
                match classify_write_error(&cause) {
                    WriteErrorDisposition::RetryableNotApplied => {
                        let mut unprocessed = unattempted;
                        unprocessed
                            .entry(table_name)
                            .or_default()
                            .insert(0, failed_request);
                        Ok(WithMetadata::multiple_writes(
                            BatchWriteItemOutput::builder()
                                .set_unprocessed_items(Some(unprocessed))
                                .build(),
                            table_versions,
                            transitions,
                        ))
                    }
                    WriteErrorDisposition::OutcomeUnknown { token } => {
                        Err(Error::BatchWrite(Box::new(BatchWriteFailure::new(
                            BatchWriteFailureDisposition::OutcomeUnknown,
                            table_name,
                            failed_request,
                            unattempted,
                            transitions,
                            token,
                            cause,
                        ))))
                    }
                    WriteErrorDisposition::Terminal => {
                        Err(Error::BatchWrite(Box::new(BatchWriteFailure::new(
                            BatchWriteFailureDisposition::Terminal,
                            table_name,
                            failed_request,
                            unattempted,
                            transitions,
                            None,
                            cause,
                        ))))
                    }
                }
            }
        }
    }
}

fn unattempted_items(
    prepared: &BTreeMap<String, Vec<(WriteRequest, BatchWriteAction)>>,
    failed_table: &str,
    failed_index: usize,
) -> HashMap<String, Vec<WriteRequest>> {
    prepared
        .iter()
        .filter_map(|(table, actions)| {
            let start = match table.as_str().cmp(failed_table) {
                std::cmp::Ordering::Less => return None,
                std::cmp::Ordering::Equal => failed_index.saturating_add(1),
                std::cmp::Ordering::Greater => 0,
            };
            let requests = actions
                .get(start..)
                .unwrap_or_default()
                .iter()
                .map(|(request, _)| request.clone())
                .collect::<Vec<_>>();
            (!requests.is_empty()).then(|| (table.clone(), requests))
        })
        .collect()
}

fn transition_metadata(
    transitions: Vec<prolly_dynamodb_core::BatchWriteTransition>,
) -> (
    Vec<TableTransitionMetadata>,
    BTreeMap<String, prolly::MapVersionId>,
) {
    let mut metadata = Vec::with_capacity(transitions.len());
    let mut versions = BTreeMap::new();
    for transition in transitions {
        if let Some(current) = transition.update.current() {
            versions.insert(transition.table_name.clone(), current.id.clone());
        }
        metadata.push(TableTransitionMetadata::from_update(
            transition.table_name,
            &transition.update,
            Some(transition.commit_id),
            Some(transition.table_id),
        ));
    }
    (metadata, versions)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actions(count: usize) -> Vec<(WriteRequest, BatchWriteAction)> {
        (0..count)
            .map(|_| {
                (
                    WriteRequest::builder().build(),
                    BatchWriteAction::Delete(prolly_dynamodb_core::Item::new()),
                )
            })
            .collect()
    }

    #[test]
    fn unattempted_items_are_the_strict_deterministic_tail() {
        let prepared = BTreeMap::from([
            ("Accounts".into(), actions(2)),
            ("Orders".into(), actions(3)),
            ("Receipts".into(), actions(1)),
        ]);

        let tail = unattempted_items(&prepared, "Orders", 1);
        assert!(!tail.contains_key("Accounts"));
        assert_eq!(tail["Orders"].len(), 1);
        assert_eq!(tail["Receipts"].len(), 1);

        // A corrupt/out-of-range core boundary must fail closed without a
        // facade panic or accidental replay of an earlier request.
        let invalid_tail = unattempted_items(&prepared, "Orders", usize::MAX);
        assert!(!invalid_tail.contains_key("Orders"));
        assert_eq!(invalid_tail["Receipts"].len(), 1);
    }
}
