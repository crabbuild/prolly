use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use google_cloud_spanner::client::ClientConfig;
use prolly::{AsyncProlly, Config, Error as ProllyError, Mutation, RemoteProllyStore};
use prolly::{Resolution, Resolver};
use prolly_store_spanner::SpannerBackend;

fn main() -> Result<(), Box<dyn Error>> {
    runtime().block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let database = required_env("PROLLY_STORE_SPANNER_DATABASE")?;
    let mut config = ClientConfig::default();
    if std::env::var("PROLLY_STORE_SPANNER_AUTH").is_ok() {
        config = config.with_auth().await?;
    }
    let backend = SpannerBackend::connect(&database, config).await?;
    let client = backend.client().clone();

    let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
    let base = seed_tree(&prolly).await?;
    let left = prolly
        .batch(
            &base,
            vec![
                upsert("account/001", "suspended"),
                upsert("account/003", "active"),
            ],
        )
        .await?;
    let right = prolly
        .batch(&base, vec![upsert("account/002", "closed")])
        .await?;

    let diffs = prolly.diff(&base, &left).await?;
    assert_eq!(diffs.len(), 2);

    let merged = prolly.merge(&base, &left, &right, None).await?;
    assert_value(&prolly, &merged, "account/001", "suspended").await?;
    assert_value(&prolly, &merged, "account/002", "closed").await?;
    assert_value(&prolly, &merged, "account/003", "active").await?;

    let root_name = format!("examples/spanner/{}/main", now_nanos());
    prolly
        .publish_named_root(root_name.as_bytes(), &merged)
        .await?;
    let loaded = prolly
        .load_named_root(root_name.as_bytes())
        .await?
        .expect("named root");
    assert_value(&prolly, &loaded, "account/002", "closed").await?;

    let conflict_left = prolly
        .batch(&base, vec![upsert("account/001", "left")])
        .await?;
    let conflict_right = prolly
        .batch(&base, vec![upsert("account/001", "right")])
        .await?;
    assert!(matches!(
        prolly
            .merge(&base, &conflict_left, &conflict_right, None)
            .await,
        Err(ProllyError::Conflict(_))
    ));

    let resolver: Resolver = Box::new(|conflict| {
        let mut value = conflict.left.clone().unwrap_or_default();
        value.extend_from_slice(b"+");
        value.extend_from_slice(
            conflict
                .right
                .as_ref()
                .map(Vec::as_slice)
                .unwrap_or_default(),
        );
        Resolution::value(value)
    });
    let resolved = prolly
        .merge(&base, &conflict_left, &conflict_right, Some(resolver))
        .await?;
    assert_value(&prolly, &resolved, "account/001", "left+right").await?;

    let roots = prolly.list_named_roots().await?;
    println!("spanner example ok; named_roots={}", roots.len());
    client.close().await;
    Ok(())
}

async fn seed_tree<S>(prolly: &AsyncProlly<S>) -> Result<prolly::Tree, prolly::Error>
where
    S: prolly::AsyncStore,
    S::Error: Send + Sync,
{
    prolly
        .batch(
            &prolly.create(),
            vec![
                upsert("account/001", "active"),
                upsert("account/002", "active"),
            ],
        )
        .await
}

async fn assert_value<S>(
    prolly: &AsyncProlly<S>,
    tree: &prolly::Tree,
    key: &str,
    expected: &str,
) -> Result<(), prolly::Error>
where
    S: prolly::AsyncStore,
    S::Error: Send + Sync,
{
    assert_eq!(
        prolly.get(tree, key.as_bytes()).await?,
        Some(expected.as_bytes().to_vec())
    );
    Ok(())
}

fn upsert(key: &str, value: &str) -> Mutation {
    Mutation::Upsert {
        key: key.as_bytes().to_vec(),
        val: value.as_bytes().to_vec(),
    }
}

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    std::env::var(name).map_err(|_| format!("set {name} to run this example").into())
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
