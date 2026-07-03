use prolly::{
    debug_key, decode_segments, i64_key, prefix_range, Config, KeyBuilder, MemStore, Prolly,
};

#[test]
fn composite_prefix_range_selects_ordered_rows() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let mut tree = prolly.create();

    let conversation_prefix = KeyBuilder::new()
        .push_str("tenant")
        .push_str("t1")
        .push_str("conversation")
        .push_str("c1")
        .finish();
    let other_conversation_prefix = KeyBuilder::new()
        .push_str("tenant")
        .push_str("t1")
        .push_str("conversation")
        .push_str("c2")
        .finish();
    let user_prefix = KeyBuilder::new()
        .push_str("tenant")
        .push_str("t1")
        .push_str("user")
        .finish();

    for sequence in [5, 1, 3] {
        let key = KeyBuilder::from_prefix(conversation_prefix.clone())
            .push_u64(sequence)
            .finish();
        tree = prolly
            .put(&tree, key, format!("message-{sequence}").into_bytes())
            .unwrap();
    }

    tree = prolly
        .put(
            &tree,
            KeyBuilder::from_prefix(other_conversation_prefix)
                .push_u64(2)
                .finish(),
            b"other-conversation".to_vec(),
        )
        .unwrap();
    tree = prolly
        .put(
            &tree,
            KeyBuilder::from_prefix(user_prefix).push_str("u1").finish(),
            b"user".to_vec(),
        )
        .unwrap();

    let (start, end) = prefix_range(&conversation_prefix);
    let entries = prolly
        .range(&tree, &start, end.as_deref())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let values = entries
        .iter()
        .map(|(_, value)| String::from_utf8(value.clone()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(values, ["message-1", "message-3", "message-5"]);

    let decoded_key = decode_segments(&entries[0].0).unwrap();
    assert_eq!(decoded_key[0], b"tenant");
    assert_eq!(decoded_key[1], b"t1");
    assert_eq!(decoded_key[2], b"conversation");
    assert_eq!(decoded_key[3], b"c1");
    assert_eq!(decoded_key[4], 1u64.to_be_bytes());
}

#[test]
fn numeric_and_debug_helpers_are_crate_root_api() {
    let mut values = vec![0, i64::MAX, -1, 42, i64::MIN];
    values.sort_by_key(|value| i64_key(*value));
    assert_eq!(values, [i64::MIN, -1, 0, 42, i64::MAX]);

    let key = KeyBuilder::new()
        .push_str("raw")
        .push_segment(b"a\0b")
        .finish();
    assert_eq!(
        decode_segments(&key).unwrap(),
        vec![b"raw".to_vec(), b"a\0b".to_vec()]
    );
    assert_eq!(debug_key(b"a\n\0"), "\"a\\n\\x00\"");
}
