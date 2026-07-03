use prolly::{Config, Diff, MemStore, Prolly};

fn main() -> Result<(), prolly::Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());

    let base = prolly.create();
    let base = prolly.put(&base, b"doc:title".to_vec(), b"Draft".to_vec())?;

    let left = prolly.put(&base, b"doc:body".to_vec(), b"Hello".to_vec())?;
    let right = prolly.put(&base, b"doc:tags".to_vec(), b"example".to_vec())?;

    let left_changes = prolly.diff(&base, &left)?;
    assert!(matches!(
        left_changes.as_slice(),
        [Diff::Added { key, val }] if key == b"doc:body" && val == b"Hello"
    ));

    let merged = prolly.merge(&base, &left, &right, None)?;
    assert_eq!(prolly.get(&merged, b"doc:body")?, Some(b"Hello".to_vec()));
    assert_eq!(prolly.get(&merged, b"doc:tags")?, Some(b"example".to_vec()));

    println!("merged {} left-side change", left_changes.len());
    Ok(())
}
