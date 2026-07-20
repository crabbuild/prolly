use prolly::{AsyncProlly, Config};
use prolly_store_turso::{TursoBackend, TursoStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "target/prolly-turso-example.db".to_string());
    if let Some(parent) = std::path::Path::new(&path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let backend = TursoBackend::open(&path).await?;
    let prolly = AsyncProlly::new(TursoStore::new(backend), Config::default());
    let base = prolly
        .load_named_root(b"main")
        .await?
        .unwrap_or_else(|| prolly.create());
    let tree = prolly
        .put(&base, b"user/1".to_vec(), b"Ada".to_vec())
        .await?;
    prolly.publish_named_root(b"main", &tree).await?;

    let reopened = prolly
        .load_named_root(b"main")
        .await?
        .expect("published main root");
    let value = prolly.get(&reopened, b"user/1").await?;
    assert_eq!(value, Some(b"Ada".to_vec()));
    println!("stored user/1 in {path}");
    Ok(())
}
