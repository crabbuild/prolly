use prolly::{Config, CrdtConfig, CrdtResolution, MemStore, Prolly};

fn main() -> Result<(), prolly::Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());

    let base = prolly.create();
    let base = prolly.put(&base, b"counter".to_vec(), b"1".to_vec())?;

    let left = prolly.put(&base, b"counter".to_vec(), b"2".to_vec())?;
    let right = prolly.put(&base, b"counter".to_vec(), b"3".to_vec())?;

    let sum_values = CrdtConfig::custom(|conflict| {
        let left = conflict
            .left
            .as_deref()
            .and_then(parse_u64)
            .unwrap_or_default();
        let right = conflict
            .right
            .as_deref()
            .and_then(parse_u64)
            .unwrap_or_default();

        CrdtResolution::value((left + right).to_string().into_bytes())
    });

    let merged = prolly.crdt_merge(&base, &left, &right, &sum_values)?;
    assert_eq!(prolly.get(&merged, b"counter")?, Some(b"5".to_vec()));

    let delete_absent_keys = CrdtConfig::custom(|conflict| {
        if conflict.left.is_none() || conflict.right.is_none() {
            CrdtResolution::delete()
        } else {
            CrdtResolution::value(conflict.right.clone().unwrap())
        }
    });

    let deleted = prolly.delete(&base, b"counter")?;
    let resolved = prolly.crdt_merge(&base, &deleted, &right, &delete_absent_keys)?;
    assert_eq!(prolly.get(&resolved, b"counter")?, None);

    println!("custom CRDT merge resolved value/value and delete/update conflicts");
    Ok(())
}

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}
