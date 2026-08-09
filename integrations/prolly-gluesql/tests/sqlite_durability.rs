#![cfg(feature = "sqlite")]

use {
    gluesql_core::{data::Value, executor::Payload, prelude::Glue},
    prolly_gluesql::{ProllyStorageConfig, SqliteProllyStorage},
    tempfile::TempDir,
};

fn database_path() -> (TempDir, std::path::PathBuf) {
    let directory = TempDir::new().expect("create temporary directory");
    let path = directory.path().join("database.sqlite");
    (directory, path)
}

#[tokio::test]
async fn sqlite_reopen_preserves_catalog_rows_and_indexes() {
    let (_directory, path) = database_path();
    {
        let storage = SqliteProllyStorage::open_sqlite(&path).unwrap();
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

    let storage = SqliteProllyStorage::open_sqlite(&path).unwrap();
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
async fn concurrent_writers_get_a_serialization_conflict() {
    let (_directory, path) = database_path();
    {
        let storage = SqliteProllyStorage::open_sqlite(&path).unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER);")
            .await
            .unwrap();
        glue.execute("INSERT INTO counter VALUES (1, 0);")
            .await
            .unwrap();
    }

    let mut first = Glue::new(SqliteProllyStorage::open_sqlite(&path).unwrap());
    let mut second = Glue::new(SqliteProllyStorage::open_sqlite(&path).unwrap());
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
async fn branches_remain_isolated_after_reopen() {
    let (_directory, path) = database_path();
    {
        let storage = SqliteProllyStorage::open_sqlite(&path).unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("CREATE TABLE settings (id INTEGER PRIMARY KEY, value TEXT);")
            .await
            .unwrap();
        glue.execute("INSERT INTO settings VALUES (1, 'main');")
            .await
            .unwrap();
        glue.storage.create_branch("experiment").unwrap();
    }

    let config = ProllyStorageConfig {
        branch: "experiment".to_owned(),
        ..ProllyStorageConfig::default()
    };
    {
        let storage = SqliteProllyStorage::open_sqlite_with_config(&path, config.clone()).unwrap();
        let mut glue = Glue::new(storage);
        glue.execute("UPDATE settings SET value = 'experiment' WHERE id = 1;")
            .await
            .unwrap();
    }

    let mut experiment =
        Glue::new(SqliteProllyStorage::open_sqlite_with_config(&path, config).unwrap());
    let mut main = Glue::new(SqliteProllyStorage::open_sqlite(&path).unwrap());
    let experiment_rows = experiment
        .execute("SELECT value FROM settings;")
        .await
        .unwrap();
    let main_rows = main.execute("SELECT value FROM settings;").await.unwrap();
    assert!(matches!(
        &experiment_rows[0],
        Payload::Select { rows, .. } if rows == &vec![vec![Value::Str("experiment".to_owned())]]
    ));
    assert!(matches!(
        &main_rows[0],
        Payload::Select { rows, .. } if rows == &vec![vec![Value::Str("main".to_owned())]]
    ));
}
