#![cfg(feature = "redb")]

use {
    gluesql_core::{data::Value, executor::Payload, prelude::Glue},
    prolly_gluesql::{CommitOptions, DatabaseRef, RedbProllyStorage},
    prolly_store_redb::RedbStore,
    std::sync::Arc,
    tempfile::TempDir,
};

fn database_path() -> (TempDir, std::path::PathBuf) {
    let directory = TempDir::new().expect("create temporary directory");
    let path = directory.path().join("database.redb");
    (directory, path)
}

#[tokio::test]
async fn redb_reopen_preserves_catalog_rows_indexes_and_functions() {
    let (_directory, path) = database_path();
    {
        let storage = RedbProllyStorage::open_redb(&path).unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);")
            .await
            .unwrap();
        glue.execute("CREATE INDEX users_email ON users (email);")
            .await
            .unwrap();
        glue.execute("INSERT INTO users VALUES (1, 'ada@example.com');")
            .await
            .unwrap();
        glue.execute("CREATE FUNCTION plus_one(n INT) RETURN n + 1;")
            .await
            .unwrap();
    }

    let storage = RedbProllyStorage::open_redb(&path).unwrap();
    let mut glue = Glue::new(storage);
    let payloads = glue
        .execute("SELECT id FROM users WHERE email = 'ada@example.com';")
        .await
        .unwrap();
    assert!(matches!(
        &payloads[0],
        Payload::Select { rows, .. } if rows == &vec![vec![Value::I64(1)]]
    ));
    let function_result = glue.execute("SELECT plus_one(41);").await.unwrap();
    assert!(matches!(
        &function_result[0],
        Payload::Select { rows, .. } if rows == &vec![vec![Value::I64(42)]]
    ));
}

#[tokio::test]
async fn shared_redb_connections_get_a_serialization_conflict() {
    let (_directory, path) = database_path();
    let backend = Arc::new(RedbStore::open(&path).unwrap());
    let mut first = Glue::new(RedbProllyStorage::new(Arc::clone(&backend)).unwrap());
    first
        .execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER);")
        .await
        .unwrap();
    first
        .execute("INSERT INTO counter VALUES (1, 0);")
        .await
        .unwrap();

    let mut second = Glue::new(RedbProllyStorage::new(Arc::clone(&backend)).unwrap());
    first.execute("START TRANSACTION;").await.unwrap();
    second.execute("START TRANSACTION;").await.unwrap();
    first
        .execute("UPDATE counter SET value = 1 WHERE id = 1;")
        .await
        .unwrap();
    second
        .execute("UPDATE counter SET value = 2 WHERE id = 1;")
        .await
        .unwrap();
    first.execute("COMMIT;").await.unwrap();
    let conflict = second.execute("COMMIT;").await.unwrap_err();
    assert!(conflict.to_string().contains("serialization conflict"));

    let payloads = first.execute("SELECT value FROM counter;").await.unwrap();
    assert!(matches!(
        &payloads[0],
        Payload::Select { rows, .. } if rows == &vec![vec![Value::I64(1)]]
    ));
}

#[tokio::test]
async fn redb_branches_remain_isolated_after_reopen() {
    let (_directory, path) = database_path();
    {
        let storage = RedbProllyStorage::open_redb(&path).unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE settings (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        glue.execute("INSERT INTO settings VALUES (1, 'main');")
            .await
            .unwrap();
        glue.storage.create_branch("experiment").unwrap();
    }

    {
        let storage = RedbProllyStorage::open_redb_with_branch(&path, "experiment").unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("UPDATE settings SET value = 'experiment' WHERE id = 1;")
            .await
            .unwrap();
    }

    let experiment_rows = {
        let storage = RedbProllyStorage::open_redb_with_branch(&path, "experiment").unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("SELECT value FROM settings;").await.unwrap()
    };
    let main_rows = {
        let storage = RedbProllyStorage::open_redb(&path).unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("SELECT value FROM settings;").await.unwrap()
    };
    assert!(matches!(
        &experiment_rows[0],
        Payload::Select { rows, .. } if rows == &vec![vec![Value::Str("experiment".to_owned())]]
    ));
    assert!(matches!(
        &main_rows[0],
        Payload::Select { rows, .. } if rows == &vec![vec![Value::Str("main".to_owned())]]
    ));
}

#[tokio::test]
async fn redb_reopens_commit_graph_refs_and_retained_snapshots() {
    let (_directory, path) = database_path();
    let commit_id = {
        let storage = RedbProllyStorage::open_redb(&path).unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, message TEXT);")
            .await
            .unwrap();
        glue.execute("INSERT INTO events VALUES (1, 'committed');")
            .await
            .unwrap();
        let commit = glue
            .storage
            .commit_with(CommitOptions::new("checkpoint").created_at_millis(1_000))
            .unwrap();
        assert!(glue
            .storage
            .create_ref("refs/tags/checkpoint", &commit.id)
            .unwrap()
            .is_applied());
        glue.execute("INSERT INTO events VALUES (2, 'uncommitted head');")
            .await
            .unwrap();
        commit.id
    };

    let storage = RedbProllyStorage::open_redb(&path).unwrap();
    let mut glue = Glue::new(storage);
    assert_eq!(
        glue.storage
            .resolve_ref("refs/tags/checkpoint")
            .unwrap()
            .unwrap()
            .target,
        commit_id
    );
    assert_eq!(
        glue.storage
            .get_commit(&commit_id)
            .unwrap()
            .unwrap()
            .message,
        "checkpoint"
    );
    glue.storage
        .create_branch_from("archive", &DatabaseRef::Commit(commit_id))
        .unwrap();
    glue.storage.checkout_branch("archive").unwrap();
    let rows = glue
        .execute("SELECT id, message FROM events ORDER BY id;")
        .await
        .unwrap();
    assert!(matches!(
        &rows[0],
        Payload::Select { rows, .. }
            if rows == &vec![vec![Value::I64(1), Value::Str("committed".to_owned())]]
    ));
}
