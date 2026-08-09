//! Durable redb-backed versions, shared connections, and branch reopen.
//!
//! Run with: `cargo run --features redb --example redb_durable`

use {
    prolly_gluesql::{gluesql_core::prelude::Value, Glue, Payload, RedbProllyStorage},
    prolly_store_redb::RedbStore,
    std::{
        error::Error,
        path::{Path, PathBuf},
        sync::Arc,
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
        // Redb permits one writable database handle per file. Share that handle
        // when an application needs multiple GlueSQL connections.
        let backend = Arc::new(RedbStore::open(path)?);
        let storage = RedbProllyStorage::new(Arc::clone(&backend))?;
        let mut db = Glue::new(storage);
        db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, message TEXT NOT NULL);")
            .await?;
        db.execute("INSERT INTO events VALUES (1, 'persisted');")
            .await?;
        let snapshot = db.storage.create_branch("snapshot")?;
        let snapshot_id = snapshot.id().unwrap().to_string();

        let mut reader = Glue::new(RedbProllyStorage::new(Arc::clone(&backend))?);
        assert_eq!(select_messages(&mut reader).await?.len(), 1);

        db.execute("INSERT INTO events VALUES (2, 'main only');")
            .await?;
        assert_eq!(select_messages(&mut reader).await?.len(), 2);
        let main_id = db.storage.head()?.unwrap().id().unwrap().to_string();
        (main_id, snapshot_id)
    };

    {
        let storage = RedbProllyStorage::open_redb(path)?;
        let mut db = Glue::new(storage);
        assert_eq!(db.storage.head()?.unwrap().id().unwrap().as_str(), main_id);
        assert_eq!(select_messages(&mut db).await?.len(), 2);
    }

    {
        let storage = RedbProllyStorage::open_redb_with_branch(path, "snapshot")?;
        let mut db = Glue::new(storage);
        assert_eq!(
            db.storage.head()?.unwrap().id().unwrap().as_str(),
            snapshot_id
        );
        assert_eq!(
            select_messages(&mut db).await?,
            vec![vec![Value::I64(1), Value::Str("persisted".to_owned())]]
        );
    }

    println!("reopened redb database: {}", path.display());
    println!("main version:     {main_id}");
    println!("snapshot version: {snapshot_id}");
    Ok(())
}

async fn select_messages(
    db: &mut Glue<RedbProllyStorage>,
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
        "prolly-gluesql-example-{}-{nonce}.redb",
        std::process::id()
    ))
}
