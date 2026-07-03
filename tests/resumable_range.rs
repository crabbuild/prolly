use prolly::{Config, MemStore, Prolly, RangeCursor};

fn small_node_config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(177)
        .build()
}

fn populated_tree(count: usize) -> (Prolly<MemStore>, prolly::Tree) {
    let prolly = Prolly::new(MemStore::new(), small_node_config());
    let mut tree = prolly.create();

    for idx in 0..count {
        tree = prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    (prolly, tree)
}

#[test]
fn range_after_resumes_without_duplicate_for_exact_and_gap_keys() {
    let (prolly, tree) = populated_tree(12);

    let exact_resume = prolly
        .range_after(&tree, b"k004", Some(b"k008"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        exact_resume,
        vec![
            (b"k005".to_vec(), b"v005".to_vec()),
            (b"k006".to_vec(), b"v006".to_vec()),
            (b"k007".to_vec(), b"v007".to_vec()),
        ]
    );

    let gap_resume = prolly
        .range_after(&tree, b"k004a", Some(b"k007"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        gap_resume,
        vec![
            (b"k005".to_vec(), b"v005".to_vec()),
            (b"k006".to_vec(), b"v006".to_vec()),
        ]
    );
}

#[test]
fn range_pages_resume_to_reconstruct_bounded_scan() {
    let (prolly, tree) = populated_tree(27);
    let expected = prolly
        .range(&tree, b"k004", Some(b"k021"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let mut cursor = RangeCursor::after_key(b"k003".to_vec());
    let mut actual = Vec::new();
    let mut page_count = 0usize;

    loop {
        let page = prolly.range_page(&tree, &cursor, Some(b"k021"), 5).unwrap();
        page_count += 1;
        actual.extend(page.entries);

        let Some(next) = page.next_cursor else {
            break;
        };
        assert!(!next.is_start());
        cursor = next;
    }

    assert_eq!(actual, expected);
    assert!(page_count > 1);
}

#[test]
fn range_cursor_tracks_iterator_progress_and_zero_limit_is_noop() {
    let (prolly, tree) = populated_tree(4);

    let mut iter = prolly
        .range_from_cursor(&tree, &RangeCursor::start(), None)
        .unwrap();
    assert!(iter.resume_cursor().is_start());

    let first = iter.next().unwrap().unwrap();
    assert_eq!(first.0, b"k000".to_vec());
    assert_eq!(iter.resume_cursor().after(), Some(b"k000".as_slice()));

    let cursor = RangeCursor::after_key(b"k001".to_vec());
    let page = prolly.range_page(&tree, &cursor, None, 0).unwrap();
    assert!(page.entries.is_empty());
    assert_eq!(page.next_cursor, Some(cursor));
}
