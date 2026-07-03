mod common;

use common::configured_prolly;

#[test]
fn range_cursor_matches_range_iterator_across_leaf_boundaries() {
    let prolly = configured_prolly();
    let mut tree = prolly.create();
    for i in 0..32 {
        tree = prolly
            .put(
                &tree,
                format!("k{i:02}").into_bytes(),
                format!("v{i:02}").into_bytes(),
            )
            .unwrap();
    }

    let range_entries = prolly
        .range(&tree, b"k07", Some(b"k25"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let cursor_entries = prolly
        .range_cursor(&tree, b"k07", Some(b"k25"))
        .unwrap()
        .collect::<Vec<_>>();

    assert_eq!(cursor_entries, range_entries);
}
