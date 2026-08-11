use aws_sdk_dynamodb::operation::list_tables::ListTablesOutput;

use crate::{Client, Error, Result};

#[derive(Clone)]
pub struct ListTables {
    client: Client,
    exclusive_start_table_name: Option<String>,
    limit: Option<i32>,
}

impl ListTables {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            exclusive_start_table_name: None,
            limit: None,
        }
    }

    pub fn exclusive_start_table_name(mut self, value: impl Into<String>) -> Self {
        self.exclusive_start_table_name = Some(value.into());
        self
    }

    pub fn set_exclusive_start_table_name(mut self, value: Option<String>) -> Self {
        self.exclusive_start_table_name = value;
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

    #[tracing::instrument(
        name = "prolly_dynamodb.ListTables",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "ListTables"),
        err
    )]
    pub async fn send(self) -> Result<ListTablesOutput> {
        let limit = self.limit.unwrap_or(100);
        if !(1..=100).contains(&limit) {
            return Err(Error::InvalidRequest(
                "ListTables.limit must be 1..=100".into(),
            ));
        }
        let names = self.client.core().list_tables().await?;
        let start = self
            .exclusive_start_table_name
            .as_ref()
            .map(|start| names.partition_point(|name| name <= start))
            .unwrap_or(0);
        let end = (start + limit as usize).min(names.len());
        let page = names[start..end].to_vec();
        let last = (end < names.len()).then(|| page.last().cloned()).flatten();
        Ok(ListTablesOutput::builder()
            .set_table_names(Some(page))
            .set_last_evaluated_table_name(last)
            .build())
    }
}
