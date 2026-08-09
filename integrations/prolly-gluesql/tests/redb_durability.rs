#![cfg(feature = "redb")]

use {
    gluesql_core::{data::Value, executor::Payload, prelude::Glue},
    prolly_gluesql::RedbProllyStorage,
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
