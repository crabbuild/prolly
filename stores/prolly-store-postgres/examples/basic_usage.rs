use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{AsyncProlly, Config, Error as ProllyError, Mutation, RemoteProllyStore};
use prolly::{Resolution, Resolver};
use prolly_store_postgres::PostgresBackend;

fn main() -> Result<(), Box<dyn Error>> {
    runtime().block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let database_url = std::env::var("PROLLY_STORE_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://prolly:prolly@127.0.0.1:55432/prolly".to_string());
    let backend = PostgresBackend::connect(&database_url).await?;
    backend.initialize_schema().await?;

    let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
    let base = seed_tree(&prolly).await?;
    let left = prolly
        .batch(
            &base,
            vec![
                upsert("user/001", "Ada Lovelace"),
                upsert("user/003", "Katherine Johnson"),
            ],
        )
        .await?;
    let right = prolly
        .batch(&base, vec![upsert("user/002", "Grace Hopper")])
        .await?;

    let diffs = prolly.diff(&base, &left).await?;
    assert_eq!(diffs.len(), 2);

    let merged = prolly.merge(&base, &left, &right, None).await?;
    assert_value(&prolly, &merged, "user/001", "Ada Lovelace").await?;
    assert_value(&prolly, &merged, "user/002", "Grace Hopper").await?;
    assert_value(&prolly, &merged, "user/003", "Katherine Johnson").await?;

    let root_name = format!("examples/postgres/{}/main", now_nanos());
    prolly
        .publish_named_root(root_name.as_bytes(), &merged)
        .await?;
    let loaded = prolly
        .load_named_root(root_name.as_bytes())
        .await?
        .expect("named root");
    assert_value(&prolly, &loaded, "user/002", "Grace Hopper").await?;

    let conflict_left = prolly
        .batch(&base, vec![upsert("user/001", "left")])
        .await?;
    let conflict_right = prolly
        .batch(&base, vec![upsert("user/001", "right")])
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
    assert_value(&prolly, &resolved, "user/001", "left+right").await?;

    let roots = prolly.list_named_roots().await?;
    println!("postgres example ok; named_roots={}", roots.len());
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
            vec![upsert("user/001", "Ada"), upsert("user/002", "Grace")],
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
