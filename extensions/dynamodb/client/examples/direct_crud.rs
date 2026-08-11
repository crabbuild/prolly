use std::error::Error;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use prolly_dynamodb_client::{Client, Error as ClientError};
use prolly_store_dynamodb::DynamoDbBackend;

fn main() -> Result<(), Box<dyn Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let physical_table = std::env::var("PROLLY_STORE_DYNAMODB_TABLE")?;
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let mut dynamodb_config = aws_sdk_dynamodb::config::Builder::from(&config);
    if let Ok(endpoint) = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT") {
        dynamodb_config = dynamodb_config.endpoint_url(endpoint);
    }
    let backend = DynamoDbBackend::new(
        aws_sdk_dynamodb::Client::from_conf(dynamodb_config.build()),
        physical_table,
    )
    .with_key_prefix(b"direct-crud-example:".to_vec());
    backend.initialize_schema().await?;
    let client = Client::open(backend).await?;

    match client.describe_table().table_name("Orders").send().await {
        Ok(_) => {}
        Err(ClientError::Core(prolly_dynamodb_core::Error::TableNotFound(_))) => {
            client
                .create_table()
                .table_name("Orders")
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("accountId")
                        .attribute_type(ScalarAttributeType::S)
                        .build()?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("accountId")
                        .key_type(KeyType::Hash)
                        .build()?,
                )
                .send()
                .await?;
        }
        Err(error) => return Err(error.into()),
    }

    let before = client.table("Orders").head().await?;
    client
        .table("Orders")
        .if_head(before.id.clone())
        .put_item()
        .item("accountId", AttributeValue::S("acct-1".into()))
        .item("status", AttributeValue::S("OPEN".into()))
        .send()
        .await?;

    let current = client
        .get_item()
        .table_name("Orders")
        .key("accountId", AttributeValue::S("acct-1".into()))
        .send_with_metadata()
        .await?;
    assert!(current.output.item().is_some());
    println!("current version: {:?}", current.version_id);
    println!("capabilities: {}", client.capabilities().to_json()?);
    Ok(())
}
