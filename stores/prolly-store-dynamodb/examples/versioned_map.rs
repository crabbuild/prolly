use std::error::Error;

use prolly::{AsyncProlly, LargeValueConfig, RemoteProllyStore, VersionedMapUpdate};
use prolly_store_dynamodb::{
    dynamodb_safe_config, DynamoDbBackend, DynamoDbBlobStore, DynamoDbStore,
};

fn main() -> Result<(), Box<dyn Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let table = std::env::var("PROLLY_STORE_DYNAMODB_TABLE")?;
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let backend = DynamoDbBackend::new(aws_sdk_dynamodb::Client::new(&config), table)
        .with_key_prefix(b"versioned-map-example:".to_vec());
    backend.initialize_schema().await?;

    let blobs = DynamoDbBlobStore::new(backend.clone());
    let store: DynamoDbStore = RemoteProllyStore::new(backend);
    let engine = AsyncProlly::new(store, dynamodb_safe_config());
    let evidence = engine.versioned_map(b"evidence");
    let first = evidence
        .put_large_value(
            &blobs,
            b"document/1",
            vec![0x5a; 390 * 1024],
            LargeValueConfig::new(32 * 1024),
        )
        .await?;
    let second = evidence.put(b"status", b"accepted").await?;
    assert_eq!(evidence.versions().await?.len(), 2);
    assert!(!evidence.diff(&first.id, &second.id).await?.is_empty());

    match evidence.restore_if(Some(&second.id), &first.id).await? {
        VersionedMapUpdate::Applied { .. } | VersionedMapUpdate::Unchanged { .. } => {}
        VersionedMapUpdate::Conflict { .. } => return Err("unexpected restore conflict".into()),
    }

    let transaction = engine.begin_transaction()?;
    {
        let maps = transaction.versioned_maps();
        maps.put(b"ledger", b"entry/1", b"debit").await?;
        maps.put(b"balances", b"account/1", b"100").await?;
    }
    transaction.commit().await?;
    Ok(())
}
