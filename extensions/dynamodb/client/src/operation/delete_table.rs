use aws_sdk_dynamodb::operation::delete_table::DeleteTableOutput;

use crate::conversion::table_to_aws;
use crate::{Client, Error, Result, TableTransitionMetadata, WithMetadata};

#[derive(Clone)]
pub struct DeleteTable {
    client: Client,
    table_name: Option<String>,
    request_token: Option<String>,
}

impl DeleteTable {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            table_name: None,
            request_token: None,
        }
    }

    pub fn table_name(mut self, value: impl Into<String>) -> Self {
        self.table_name = Some(value.into());
        self
    }

    /// Add durable ten-minute replay protection for logical table deletion.
    pub fn request_token(mut self, token: impl Into<String>) -> Self {
        self.request_token = Some(token.into());
        self
    }

    pub fn set_request_token(mut self, token: Option<String>) -> Self {
        self.request_token = token;
        self
    }

    pub async fn send(self) -> Result<DeleteTableOutput> {
        Ok(self.send_with_metadata().await?.output)
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.DeleteTable",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "DeleteTable"),
        err
    )]
    pub async fn send_with_metadata(self) -> Result<WithMetadata<DeleteTableOutput>> {
        let name = self
            .table_name
            .ok_or_else(|| Error::InvalidRequest("DeleteTable.table_name is required".into()))?;
        let lifecycle = match self.request_token {
            Some(token) => {
                self.client
                    .core()
                    .delete_table_idempotent_result(&name, &token)
                    .await?
            }
            None => self.client.core().delete_table_result(&name).await?,
        };
        let transition = TableTransitionMetadata {
            commit_id: Some(lifecycle.commit_id.clone()),
            table_name: name.clone(),
            table_id: Some(lifecycle.transition.table_id),
            before: lifecycle.transition.before,
            after: lifecycle.transition.after,
            applied: lifecycle.transition.applied,
        };
        Ok(WithMetadata::single_write(
            DeleteTableOutput::builder()
                .table_description(table_to_aws(&lifecycle.description))
                .build(),
            name,
            None,
            Some(lifecycle.commit_id),
            transition,
        ))
    }
}
