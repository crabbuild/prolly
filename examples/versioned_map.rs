use prolly::{Config, MemStore, Prolly, RangeCursor};

fn main() -> Result<(), prolly::Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let users = prolly.versioned_map(b"users");

    let initial = users.edit(|edit| {
        edit.put(b"user/1", b"Ada");
        edit.put(b"user/2", b"Grace");
    })?;
    let updated = users.put(b"user/1", b"Ada Lovelace")?;

    println!("current version: {}", updated.id);
    println!("cataloged versions: {}", users.versions()?.len());
    println!(
        "changes since initial: {:?}",
        users.diff(&initial.id, &updated.id)?
    );

    let values = users.get_many(&[b"user/2".as_slice(), b"missing".as_slice()])?;
    assert_eq!(values, vec![Some(b"Grace".to_vec()), None]);
    let page = users.prefix_page_at(&initial.id, b"user/", &RangeCursor::start(), 100)?;
    assert_eq!(page.entries.len(), 2);

    users.rollback_to(&initial.id)?;
    assert_eq!(users.get(b"user/1")?, Some(b"Ada".to_vec()));

    // Bound catalog growth before using the matching policy for node GC.
    let pruned = users.prune_versions(1)?;
    println!("pruned versions: {}", pruned.removed_count());

    let _retention = users.retention_policy();
    Ok(())
}
