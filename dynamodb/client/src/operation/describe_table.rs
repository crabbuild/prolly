use aws_sdk_dynamodb::operation::describe_table::DescribeTableOutput;

use crate::conversion::table_to_aws;
use crate::{Client, Error, Result};

#[derive(Clone)]
pub struct DescribeTable {
    client: Client,
    table_name: Option<String>,
}

impl DescribeTable {
    pub(crate) fn new(client: Client) -> Self {
        Self {
            client,
            table_name: None,
        }
    }

    pub fn table_name(mut self, value: impl Into<String>) -> Self {
        self.table_name = Some(value.into());
        self
    }

    #[tracing::instrument(
        name = "prolly_dynamodb.DescribeTable",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "DescribeTable"),
        err
    )]
    pub async fn send(self) -> Result<DescribeTableOutput> {
        let name = self
            .table_name
            .ok_or_else(|| Error::InvalidRequest("DescribeTable.table_name is required".into()))?;
        let table = self.client.core().describe_table(&name).await?;
        Ok(DescribeTableOutput::builder()
            .table(table_to_aws(&table))
            .build())
    }
}
