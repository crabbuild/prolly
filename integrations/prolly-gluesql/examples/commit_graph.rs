//! Git-like commits, refs, graph queries, and branch creation targets.
//!
//! Run with: `cargo run --example commit_graph`

use {
    prolly_gluesql::{
        CommitActor, CommitMetadata, CommitOptions, DatabaseRef, Glue, ProllyStorage,
    },
    std::error::Error,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut db = Glue::new(ProllyStorage::in_memory()?);
    db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, message TEXT NOT NULL);")
        .await?;
    db.execute("INSERT INTO events VALUES (1, 'base');").await?;

    let base = db.storage.commit_with(
        CommitOptions::new("initialize event log")
            .author(CommitActor::named("Ada"))
            .metadata(b"request-id", b"req-001"),
    )?;

    // Branch directly from an immutable commit, then create divergent commits.
    db.storage
        .create_branch_from("feature", &DatabaseRef::Commit(base.id.clone()))?;
    db.execute("INSERT INTO events VALUES (2, 'main');").await?;
    let main = db.storage.commit("add main event")?;

    db.storage.checkout_branch("feature")?;
    db.execute("INSERT INTO events VALUES (3, 'feature');")
        .await?;
    let feature = db.storage.commit("add feature event")?;

    let common = db
        .storage
        .merge_base(&main.id, &feature.id)?
        .expect("divergent commits share the base");
    assert_eq!(common.id, base.id);

    db.storage.checkout_branch("main")?;
    assert!(db
        .storage
        .merge(&common.version, &feature.version)
        .await?
        .is_applied());
    let merged = db.storage.commit_with(
        CommitOptions::new("merge feature")
            .parents([main.id.clone(), feature.id.clone()])
            .metadata(b"merge-strategy", b"three-way"),
    )?;
    assert!(db.storage.is_ancestor(&main.id, &merged.id)?);
    assert!(db.storage.is_ancestor(&feature.id, &merged.id)?);

    // Named refs can act as lightweight tags or application checkpoints.
    assert!(db
        .storage
        .create_ref("refs/tags/v1", &base.id)?
        .is_applied());
    db.storage
        .create_branch_from("from-tag", &DatabaseRef::Ref("refs/tags/v1".to_owned()))?;
    db.storage
        .create_branch_from("from-branch", &DatabaseRef::Branch("main".to_owned()))?;
    db.storage.create_branch_from(
        "from-version",
        &DatabaseRef::Version(main.version.id().unwrap().clone()),
    )?;

    let log = db.storage.log(&merged.id, 20)?;
    println!("commit graph from merge head:");
    for commit in log {
        println!(
            "  {} generation={} parents={} {:?}",
            commit.id,
            commit.generation,
            commit.parents.len(),
            commit.message
        );
    }
    println!("refs:");
    for reference in db.storage.list_refs("refs/")? {
        println!("  {} -> {}", reference.name, reference.target);
    }

    // Ref metadata is byte-oriented so applications can extend it freely.
    let metadata = CommitMetadata::from([(b"environment".to_vec(), b"demo".to_vec())]);
    let tag = db.storage.resolve_ref("refs/tags/v1")?.unwrap();
    assert!(db
        .storage
        .compare_and_swap_ref(
            "refs/tags/v1",
            Some(&tag.target),
            Some(&merged.id),
            metadata,
        )?
        .is_applied());
    Ok(())
}
