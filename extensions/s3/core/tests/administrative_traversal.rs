use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    decode_canonical, encode_canonical, MemoryObjectPlane, ObjectHeaders, Repository,
    RepositoryOptions, TraversalBudget,
};

#[tokio::test]
async fn commit_closure_cursor_is_constant_size_parent_first_and_restartable() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        repository_prefix: "administrative-closure".to_string(),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let root = repository.head("main").await.unwrap();
    repository.create_branch("side", root).await.unwrap();
    let main_one = repository
        .put_bytes(
            "main",
            b"main/one".to_vec(),
            b"one".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let main_two = repository
        .put_bytes(
            "main",
            b"main/two".to_vec(),
            b"two".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let side = repository
        .put_bytes(
            "side",
            b"side/one".to_vec(),
            b"side".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;

    let mut cursor = repository.start_commit_closure(&[main_two]).await.unwrap();
    repository
        .extend_commit_closure(&mut cursor, &[side])
        .await
        .unwrap();
    assert!(encode_canonical(&cursor).unwrap().len() < 256);
    plane.reset_request_counts();

    let mut emitted = Vec::new();
    let mut pages = 0usize;
    loop {
        // Model durable storage and a process boundary on every page.
        let encoded = encode_canonical(&cursor).unwrap();
        cursor = decode_canonical(&encoded).unwrap();
        let reader = Repository::open(
            plane.clone(),
            RepositoryOptions {
                read_only: true,
                ..options.clone()
            },
        )
        .await
        .unwrap();
        let page = reader.commit_closure_page(&cursor, 2, 1).await.unwrap();
        assert!(page.steps <= 2);
        assert!(page.commits.len() <= 1);
        emitted.extend(page.commits.into_iter().map(|(id, _)| id));
        cursor = page.cursor;
        pages += 1;
        if page.complete {
            break;
        }
        assert!(page.budget_exhausted || !emitted.is_empty());
        assert!(pages < 100);
    }

    assert!(pages > emitted.len());
    let unique = emitted.iter().copied().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), emitted.len());
    assert_eq!(unique.len(), 4);
    let position = |id| emitted.iter().position(|candidate| *candidate == id).unwrap();
    assert!(position(root) < position(main_one));
    assert!(position(main_one) < position(main_two));
    assert!(position(root) < position(side));
    assert_eq!(plane.request_snapshot().list, 0);

    let complete = repository
        .commit_closure_page(&cursor, 2, 1)
        .await
        .unwrap();
    assert!(complete.complete);
    assert!(complete.commits.is_empty());
    for _ in 0..100 {
        let cleanup = repository
            .cleanup_commit_closure(&cursor, 1)
            .await
            .unwrap();
        if cleanup.complete {
            break;
        }
    }
    assert!(repository
        .cleanup_commit_closure(&cursor, 1)
        .await
        .unwrap()
        .complete);
}

#[tokio::test]
async fn pins_and_reflogs_have_bounded_stable_pages() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane,
        RepositoryOptions {
            repository_prefix: "administrative-pages".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let root = repository.head("main").await.unwrap();
    for ordinal in 0..3 {
        repository
            .create_retention_pin(
                &format!("pin-{ordinal}"),
                root,
                "operator",
                "paged audit",
                None,
            )
            .await
            .unwrap();
    }
    let first = repository.list_retention_pins_page(None, 2).await.unwrap();
    assert_eq!(first.pins.len(), 2);
    let second = repository
        .list_retention_pins_page(first.continuation, 2)
        .await
        .unwrap();
    assert_eq!(second.pins.len(), 1);
    assert!(second.continuation.is_none());

    repository.create_tag("audit", root).await.unwrap();
    repository.delete_tag("audit", root).await.unwrap();
    let first = repository
        .list_tag_reflog_page("audit", None, 1)
        .await
        .unwrap();
    assert_eq!(first.entries.len(), 1);
    assert!(first.continuation.is_some());
    let second = repository
        .list_tag_reflog_page("audit", first.continuation, 1)
        .await
        .unwrap();
    assert_eq!(second.entries.len(), 1);

    let old_head = repository
        .put_bytes(
            "main",
            b"before-cursor".to_vec(),
            b"old".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let first = repository
        .list_branch_reflog_page("main", None, 1, TraversalBudget::default())
        .await
        .unwrap();
    let cursor = first.continuation.unwrap();
    let new_head = repository
        .put_bytes(
            "main",
            b"after-cursor".to_vec(),
            b"new".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let resumed = repository
        .list_branch_reflog_page("main", Some(&cursor), 100, TraversalBudget::default())
        .await
        .unwrap();
    let mut stable_entries = first.entries;
    stable_entries.extend(resumed.entries);
    assert!(stable_entries
        .iter()
        .any(|(_, entry)| entry.new_target == old_head));
    assert!(!stable_entries
        .iter()
        .any(|(_, entry)| entry.new_target == new_head));
}
