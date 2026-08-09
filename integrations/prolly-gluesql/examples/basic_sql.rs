//! End-to-end in-memory SQL, transaction, index, and function example.
//!
//! Run with: `cargo run --example basic_sql`

use {
    prolly_gluesql::{gluesql_core::prelude::Value, Glue, Payload, ProllyStorage},
    std::error::Error,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let storage = ProllyStorage::in_memory()?;
    let mut db = Glue::new(storage);

    db.execute(
        "CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );",
    )
    .await?;
    db.execute(
        "CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,
            project_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            done BOOLEAN NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects (id)
        );",
    )
    .await?;
    db.execute("CREATE INDEX tasks_done ON tasks (done);")
        .await?;
    db.execute("CREATE FUNCTION next_id(n INT) RETURN n + 1;")
        .await?;

    db.execute("START TRANSACTION;").await?;
    db.execute("INSERT INTO projects VALUES (1, 'Prolly GlueSQL');")
        .await?;
    db.execute(
        "INSERT INTO tasks VALUES
            (1, 1, 'build typed storage', false),
            (2, 1, 'ship examples', false);",
    )
    .await?;
    db.execute("COMMIT;").await?;

    db.execute("START TRANSACTION;").await?;
    db.execute("UPDATE tasks SET done = true WHERE id = 1;")
        .await?;
    db.execute("ROLLBACK;").await?;

    let payloads = db
        .execute(
            "SELECT next_id(id), title
             FROM tasks
             WHERE done = false
             ORDER BY id;",
        )
        .await?;
    let Payload::Select { rows, .. } = &payloads[0] else {
        return Err("expected a SELECT payload".into());
    };
    assert_eq!(
        rows,
        &vec![
            vec![Value::I64(2), Value::Str("build typed storage".to_owned())],
            vec![Value::I64(3), Value::Str("ship examples".to_owned())],
        ]
    );

    println!("selected rows through the covering index and custom function:");
    for row in rows {
        println!("  {row:?}");
    }
    println!("rollback preserved both unfinished tasks");
    Ok(())
}
