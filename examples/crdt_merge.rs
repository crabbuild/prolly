use prolly::{
    Config, CrdtConfig, CrdtResolution, DeletePolicy, MemStore, MergeTraceEvent, MultiValueSet,
    Prolly, TimestampedValue,
};

fn main() -> Result<(), prolly::Error> {
    let prolly = Prolly::new(MemStore::new(), Config::default());

    last_writer_wins(&prolly)?;
    preserve_concurrent_values(&prolly)?;
    resolve_delete_update(&prolly)?;
    base_aware_custom_policy(&prolly)?;

    Ok(())
}

fn last_writer_wins(prolly: &Prolly<MemStore>) -> Result<(), prolly::Error> {
    let base = prolly.put(
        &prolly.create(),
        b"title".to_vec(),
        TimestampedValue::new(b"base".to_vec(), 100).to_bytes(),
    )?;
    let left = prolly.put(
        &base,
        b"title".to_vec(),
        TimestampedValue::new(b"left".to_vec(), 300).to_bytes(),
    )?;
    let right = prolly.put(
        &base,
        b"title".to_vec(),
        TimestampedValue::new(b"right".to_vec(), 200).to_bytes(),
    )?;

    let explanation = prolly.crdt_merge_explain(&base, &left, &right, &CrdtConfig::lww());
    let merged = explanation.result?;
    let stored = prolly.get(&merged, b"title")?.expect("merged title");
    let winner = TimestampedValue::from_bytes(&stored).expect("timestamped value");

    assert_eq!(winner.value, b"left");
    assert_eq!(winner.timestamp, 300);
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::ResolverCalled { key, .. } if key == b"title"
    )));
    println!("LWW winner: {}", String::from_utf8_lossy(&winner.value));
    Ok(())
}

fn preserve_concurrent_values(prolly: &Prolly<MemStore>) -> Result<(), prolly::Error> {
    let base = prolly.put(&prolly.create(), b"status".to_vec(), b"draft".to_vec())?;
    let left = prolly.put(&base, b"status".to_vec(), b"approved".to_vec())?;
    let right = prolly.put(&base, b"status".to_vec(), b"rejected".to_vec())?;

    let merged = prolly.crdt_merge(&base, &left, &right, &CrdtConfig::multi_value())?;
    let stored = prolly.get(&merged, b"status")?.expect("merged status");
    let values = MultiValueSet::from_bytes(&stored).expect("multi-value register");

    assert_eq!(
        values.values,
        vec![b"approved".to_vec(), b"rejected".to_vec()]
    );
    println!("MV register preserved {} values", values.len());
    Ok(())
}

fn resolve_delete_update(prolly: &Prolly<MemStore>) -> Result<(), prolly::Error> {
    let base = prolly.put(&prolly.create(), b"session".to_vec(), b"v1".to_vec())?;
    let deleted = prolly.delete(&base, b"session")?;
    let updated = prolly.put(&base, b"session".to_vec(), b"v2".to_vec())?;

    let delete_wins = CrdtConfig::lww().with_delete_policy(DeletePolicy::DeleteWins);
    let merged = prolly.crdt_merge(&base, &deleted, &updated, &delete_wins)?;
    assert_eq!(prolly.get(&merged, b"session")?, None);

    let update_wins = CrdtConfig::lww().with_delete_policy(DeletePolicy::UpdateWins);
    let merged = prolly.crdt_merge(&base, &deleted, &updated, &update_wins)?;
    assert_eq!(prolly.get(&merged, b"session")?, Some(b"v2".to_vec()));
    println!("delete/update policies: DeleteWins and UpdateWins");
    Ok(())
}

fn base_aware_custom_policy(prolly: &Prolly<MemStore>) -> Result<(), prolly::Error> {
    let base = prolly.put(&prolly.create(), b"count".to_vec(), b"10".to_vec())?;
    let left = prolly.put(&base, b"count".to_vec(), b"12".to_vec())?;
    let right = prolly.put(&base, b"count".to_vec(), b"13".to_vec())?;

    // This combines changes relative to a known common ancestor:
    // 10 + (12 - 10) + (13 - 10) = 15.
    // It is a useful three-way merge policy, but it is not a G-Counter or
    // PN-Counter: it still requires `base` and carries no per-replica state.
    let add_deltas = CrdtConfig::custom(|conflict| {
        let base = parse_i64(conflict.base.as_deref()).expect("numeric base");
        let left = parse_i64(conflict.left.as_deref()).expect("numeric left");
        let right = parse_i64(conflict.right.as_deref()).expect("numeric right");
        CrdtResolution::value((left + right - base).to_string().into_bytes())
    });

    let merged = prolly.crdt_merge(&base, &left, &right, &add_deltas)?;
    assert_eq!(prolly.get(&merged, b"count")?, Some(b"15".to_vec()));
    println!("base-aware custom counter policy: 15 (not an operation CRDT)");
    Ok(())
}

fn parse_i64(value: Option<&[u8]>) -> Option<i64> {
    std::str::from_utf8(value?).ok()?.parse().ok()
}
