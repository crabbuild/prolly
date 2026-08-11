use std::collections::{BTreeMap, HashMap};

use aws_sdk_dynamodb::types::AttributeValue;
use prolly::MapVersionId;
use prolly_dynamodb_core::{LargeWriteOptions, TransactWriteAction};

use crate::{Client, Commit, Result};

/// Explicit, client-side large write session.
///
/// The session is inert until [`WriteSession::commit`]. A successful commit
/// publishes every buffered action as one table version and one durable commit.
/// Normal AWS-compatible write builders retain their original limits and
/// per-operation commit behavior.
pub struct WriteSession {
    client: Client,
    table_name: String,
    actions: Vec<TransactWriteAction>,
    expected_head: Option<MapVersionId>,
    request_token: Option<String>,
    options: LargeWriteOptions,
}

impl WriteSession {
    pub(crate) fn new(client: Client, table_name: String) -> Self {
        Self {
            client,
            table_name,
            actions: Vec::new(),
            expected_head: None,
            request_token: None,
            options: LargeWriteOptions::default(),
        }
    }

    pub fn options(mut self, options: LargeWriteOptions) -> Self {
        self.options = options;
        self
    }

    pub fn if_head(mut self, version: MapVersionId) -> Self {
        self.expected_head = Some(version);
        self
    }

    pub fn request_token(mut self, token: impl Into<String>) -> Self {
        self.request_token = Some(token.into());
        self
    }

    pub fn put(&mut self, item: HashMap<String, AttributeValue>) -> Result<&mut Self> {
        self.actions.push(TransactWriteAction::Put {
            table_name: self.table_name.clone(),
            item: crate::conversion::item_from_aws(item)?,
            condition: None,
            return_failure_old: false,
        });
        Ok(self)
    }

    pub fn delete(&mut self, key: HashMap<String, AttributeValue>) -> Result<&mut Self> {
        self.actions.push(TransactWriteAction::Delete {
            table_name: self.table_name.clone(),
            key: crate::conversion::item_from_aws(key)?,
            condition: None,
            return_failure_old: false,
        });
        Ok(self)
    }

    pub fn len(&self) -> usize {
        self.actions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub async fn commit(self) -> Result<Commit> {
        let expected_heads = self
            .expected_head
            .map(|head| BTreeMap::from([(self.table_name.clone(), head)]))
            .unwrap_or_default();
        Ok(self
            .client
            .core()
            .write_large(
                self.actions,
                self.request_token.as_deref(),
                &expected_heads,
                self.options,
            )
            .await?)
    }
}
