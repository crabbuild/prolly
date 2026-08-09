//! Durable SQLite-backed versions and branches with reopen verification.
//!
//! Run with: `cargo run --features sqlite --example sqlite_durable`

use {
    prolly_gluesql::{gluesql_core::prelude::Value, Glue, Payload, SqliteProllyStorage},
    std::{
        error::Error,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let path = temporary_database_path();
    let result = run(&path).await;
    let _ = std::fs::remove_file(&path);
    result
}

async fn run(path: &Path) -> Result<(), Box<dyn Error>> {
    let (main_id, snapshot_id) = {
        let storage = SqliteProllyStorage::open_sqlite(path)?;
        let mut db = Glue::new(storage);
        db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, message TEXT NOT NULL);")
            .await?;
        db.execute("INSERT INTO events VALUES (1, 'persisted');")
            .await?;
        let snapshot = db.storage.create_branch("snapshot")?;
        let snapshot_id = snapshot.id().unwrap().to_string();

        db.execute("INSERT INTO events VALUES (2, 'main only');")
            .await?;
        let main_id = db.storage.head()?.unwrap().id().unwrap().to_string();
        (main_id, snapshot_id)
    };

    {
        let storage = SqliteProllyStorage::open_sqlite(path)?;
        let mut db = Glue::new(storage);
        assert_eq!(db.storage.head()?.unwrap().id().unwrap().as_str(), main_id);
        assert_eq!(select_messages(&mut db).await?.len(), 2);

        db.storage.checkout_branch("snapshot")?;
        assert_eq!(
            db.storage.head()?.unwrap().id().unwrap().as_str(),
            snapshot_id
        );
        assert_eq!(
            select_messages(&mut db).await?,
            vec![vec![Value::I64(1), Value::Str("persisted".to_owned())]]
        );
    }

    println!("reopened SQLite database: {}", path.display());
    println!("main version:     {main_id}");
    println!("snapshot version: {snapshot_id}");
    Ok(())
}

async fn select_messages(
    db: &mut Glue<SqliteProllyStorage>,
) -> Result<Vec<Vec<Value>>, Box<dyn Error>> {
    let payloads = db
        .execute("SELECT id, message FROM events ORDER BY id;")
        .await?;
    let Payload::Select { rows, .. } = &payloads[0] else {
        return Err("expected a SELECT payload".into());
    };
    Ok(rows.clone())
}

fn temporary_database_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "prolly-gluesql-example-{}-{nonce}.sqlite",
        std::process::id()
    ))
}
