use prolly::{resolver, Config, MemStore, Prolly, Resolution, Resolver};

fn main() -> Result<(), prolly::Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());

    let base = prolly.create();
    let base = prolly.put(&base, b"setting:theme".to_vec(), b"system".to_vec())?;

    let left = prolly.delete(&base, b"setting:theme")?;
    let right = prolly.put(&base, b"setting:theme".to_vec(), b"dark".to_vec())?;

    let update_wins = prolly.merge(&base, &left, &right, Some(Box::new(resolver::update_wins)))?;
    assert_eq!(
        prolly.get(&update_wins, b"setting:theme")?,
        Some(b"dark".to_vec())
    );

    let delete_wins = prolly.merge(&base, &left, &right, Some(Box::new(resolver::delete_wins)))?;
    assert_eq!(prolly.get(&delete_wins, b"setting:theme")?, None);

    let leave_unresolved: Resolver = Box::new(|conflict| {
        if conflict.key.starts_with(b"setting:") {
            Resolution::unresolved()
        } else {
            resolver::prefer_right(conflict)
        }
    });

    assert!(prolly
        .merge(&base, &left, &right, Some(leave_unresolved))
        .is_err());

    println!("delete/update conflict can resolve to value, delete, or error");
    Ok(())
}
