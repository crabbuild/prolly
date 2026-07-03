mod common;

use common::{canonical_diffs, configured_prolly};
use prolly::{append_batch, resolver, Diff, Error, Mutation, Resolution};

#[test]
fn diff_and_three_way_merge_work_for_disjoint_changes() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
    let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
    let right = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();

    assert_eq!(
        prolly.diff(&base, &left).unwrap(),
        vec![Diff::Added {
            key: b"b".to_vec(),
            val: b"2".to_vec()
        }]
    );

    let merged = prolly.merge(&base, &left, &right, None).unwrap();
    assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(prolly.get(&merged, b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn conflicting_three_way_merge_requires_resolver() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"base".to_vec()).unwrap();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    assert!(matches!(
        prolly.merge(&base, &left, &right, None),
        Err(Error::Conflict(_))
    ));

    let merged = prolly
        .merge(
            &base,
            &left,
            &right,
            Some(Box::new(|conflict| {
                let mut value = conflict.left.clone().unwrap();
                value.extend_from_slice(b"+");
                value.extend_from_slice(conflict.right.as_ref().unwrap());
                Resolution::value(value)
            })),
        )
        .unwrap();
    assert_eq!(
        prolly.get(&merged, b"k").unwrap(),
        Some(b"left+right".to_vec())
    );
}

#[test]
fn conflicting_three_way_merge_can_use_helper_resolvers() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"base".to_vec()).unwrap();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let prefer_left = prolly
        .merge(&base, &left, &right, Some(Box::new(resolver::prefer_left)))
        .unwrap();
    assert_eq!(
        prolly.get(&prefer_left, b"k").unwrap(),
        Some(b"left".to_vec())
    );

    let prefer_right = prolly
        .merge(&base, &left, &right, Some(Box::new(resolver::prefer_right)))
        .unwrap();
    assert_eq!(
        prolly.get(&prefer_right, b"k").unwrap(),
        Some(b"right".to_vec())
    );
}

#[test]
fn unresolved_resolution_returns_conflict() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"base".to_vec()).unwrap();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    assert!(matches!(
        prolly.merge(
            &base,
            &left,
            &right,
            Some(Box::new(|_| Resolution::unresolved())),
        ),
        Err(Error::Conflict(_))
    ));
}

#[test]
fn delete_update_conflict_can_resolve_to_delete_or_value() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"base".to_vec()).unwrap();
    let left = prolly.delete(&base, b"k").unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let deleted = prolly
        .merge(&base, &left, &right, Some(Box::new(resolver::delete_wins)))
        .unwrap();
    assert_eq!(prolly.get(&deleted, b"k").unwrap(), None);

    let updated = prolly
        .merge(&base, &left, &right, Some(Box::new(resolver::update_wins)))
        .unwrap();
    assert_eq!(prolly.get(&updated, b"k").unwrap(), Some(b"right".to_vec()));
}

#[test]
fn add_add_conflict_preserves_absent_base() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let result = prolly.merge(&base, &left, &right, None);
    let Err(Error::Conflict(conflict)) = result else {
        panic!("expected add/add conflict");
    };

    assert_eq!(conflict.key, b"k".to_vec());
    assert_eq!(conflict.base, None);
    assert_eq!(conflict.left, Some(b"left".to_vec()));
    assert_eq!(conflict.right, Some(b"right".to_vec()));
}

#[test]
fn empty_value_is_distinct_from_delete_in_conflicts() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"base".to_vec()).unwrap();
    let left = prolly.put(&base, b"k".to_vec(), Vec::new()).unwrap();
    let right = prolly.delete(&base, b"k").unwrap();

    let result = prolly.merge(&base, &left, &right, None);
    let Err(Error::Conflict(conflict)) = result else {
        panic!("expected empty-value/delete conflict");
    };

    assert_eq!(conflict.left, Some(Vec::new()));
    assert_eq!(conflict.right, None);
}

#[test]
fn delete_resolution_from_structural_candidate_produces_valid_tree() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"base".to_vec()).unwrap();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let merged = prolly
        .merge(
            &base,
            &left,
            &right,
            Some(Box::new(|_| Resolution::delete())),
        )
        .unwrap();

    assert_eq!(prolly.get(&merged, b"k").unwrap(), None);
}

#[test]
fn streaming_diff_matches_eager_diff_content() {
    let prolly = configured_prolly();
    let mut base = prolly.create();
    for i in 0..24 {
        base = prolly
            .put(
                &base,
                format!("k{i:02}").into_bytes(),
                format!("base-{i}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = base.clone();
    other = prolly
        .put(&other, b"k03".to_vec(), b"changed".to_vec())
        .unwrap();
    other = prolly.delete(&other, b"k05").unwrap();
    other = prolly
        .put(&other, b"k99".to_vec(), b"added".to_vec())
        .unwrap();

    let eager = prolly.diff(&base, &other).unwrap();
    let streaming = prolly
        .stream_diff(&base, &other)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(canonical_diffs(streaming), canonical_diffs(eager));
}

#[test]
fn streaming_diff_matches_eager_diff_for_append_suffix() {
    let prolly = configured_prolly();
    let mut base = prolly.create();
    for i in 0..64 {
        base = prolly
            .put(
                &base,
                format!("k{i:03}").into_bytes(),
                format!("base-{i}").into_bytes(),
            )
            .unwrap();
    }

    let suffix = (64..96)
        .map(|i| Mutation::Upsert {
            key: format!("k{i:03}").into_bytes(),
            val: format!("suffix-{i}").into_bytes(),
        })
        .collect();
    let other = append_batch(&prolly, &base, suffix).unwrap();

    let eager = prolly.diff(&base, &other).unwrap();
    let streaming = prolly
        .stream_diff(&base, &other)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(streaming, eager);
    assert_eq!(streaming.len(), 32);
}

#[test]
fn range_diff_matches_filtered_eager_diff() {
    let prolly = configured_prolly();
    let mut base = prolly.create();
    for i in 0..128 {
        base = prolly
            .put(
                &base,
                format!("k{i:03}").into_bytes(),
                format!("base-{i}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = base.clone();
    other = prolly
        .put(&other, b"k010".to_vec(), b"outside-low".to_vec())
        .unwrap();
    other = prolly
        .put(&other, b"k033".to_vec(), b"changed-in-range".to_vec())
        .unwrap();
    other = prolly.delete(&other, b"k035").unwrap();
    other = prolly
        .put(&other, b"k040-extra".to_vec(), b"added-in-range".to_vec())
        .unwrap();
    other = prolly
        .put(&other, b"k090".to_vec(), b"outside-high".to_vec())
        .unwrap();
    other = prolly
        .put(&other, b"k130".to_vec(), b"outside-new".to_vec())
        .unwrap();

    let start = b"k030".as_slice();
    let end = b"k070".as_slice();
    let expected = prolly
        .diff(&base, &other)
        .unwrap()
        .into_iter()
        .filter(|diff| diff_key(diff) >= start && diff_key(diff) < end)
        .collect();
    let actual = prolly.range_diff(&base, &other, start, Some(end)).unwrap();

    assert_eq!(canonical_diffs(actual), canonical_diffs(expected));
}

#[test]
fn range_diff_does_not_duplicate_prior_child_diffs_after_local_fallback() {
    let prolly = configured_prolly();
    let mut base = prolly.create();
    for i in 0..48 {
        base = prolly
            .put(
                &base,
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = prolly
        .put(&base, b"k006".to_vec(), b"outside-left".to_vec())
        .unwrap();
    other = prolly
        .put(&other, b"k021".to_vec(), b"inside-update".to_vec())
        .unwrap();
    other = prolly.delete(&other, b"k024").unwrap();
    other = prolly
        .put(&other, b"k026a".to_vec(), b"inside-add".to_vec())
        .unwrap();
    other = prolly
        .put(&other, b"k044".to_vec(), b"outside-right".to_vec())
        .unwrap();

    let actual = prolly
        .range_diff(&base, &other, b"k020", Some(b"k030"))
        .unwrap();

    assert_eq!(
        actual,
        vec![
            Diff::Changed {
                key: b"k021".to_vec(),
                old: b"v021".to_vec(),
                new: b"inside-update".to_vec(),
            },
            Diff::Removed {
                key: b"k024".to_vec(),
                val: b"v024".to_vec(),
            },
            Diff::Added {
                key: b"k026a".to_vec(),
                val: b"inside-add".to_vec(),
            },
        ]
    );
}

#[test]
fn range_diff_handles_empty_roots_and_empty_ranges() {
    let prolly = configured_prolly();
    let empty = prolly.create();
    let mut tree = empty.clone();
    for i in 0..16 {
        tree = prolly
            .put(
                &tree,
                format!("k{i:03}").into_bytes(),
                format!("value-{i}").into_bytes(),
            )
            .unwrap();
    }

    let added = prolly
        .range_diff(&empty, &tree, b"k004", Some(b"k008"))
        .unwrap();
    assert_eq!(added.len(), 4);
    assert!(added.iter().all(|diff| matches!(diff, Diff::Added { .. })));

    let removed = prolly
        .range_diff(&tree, &empty, b"k004", Some(b"k008"))
        .unwrap();
    assert_eq!(removed.len(), 4);
    assert!(removed
        .iter()
        .all(|diff| matches!(diff, Diff::Removed { .. })));

    assert!(prolly
        .range_diff(&empty, &tree, b"k008", Some(b"k004"))
        .unwrap()
        .is_empty());
}

fn diff_key(diff: &Diff) -> &[u8] {
    match diff {
        Diff::Added { key, .. } | Diff::Changed { key, .. } | Diff::Removed { key, .. } => key,
    }
}
