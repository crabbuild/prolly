use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly_s3_core::{
    FixedClock, MemoryObjectPlane, ObjectHeaders, ProviderPerKeyVersionLimit, Repository,
    RepositoryOptions, SequenceIdSource, TraversalBudget,
};

#[tokio::test]
async fn history_diff_reflog_reset_and_recovery_are_bounded_and_audited() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(50_000));
    let repository = Repository::initialize(
        plane,
        RepositoryOptions {
            repository_prefix: ".tests/history-recovery".to_string(),
            writer: "history-writer".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0x91, 1)),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let root = repository.head("main").await.unwrap();

    clock.advance(1).unwrap();
    let first = repository
        .put_object(
            "main",
            b"a.txt".to_vec(),
            b"one".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    clock.advance(1).unwrap();
    let second = repository
        .put_object(
            "main",
            b"b.txt".to_vec(),
            b"two".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();

    let first_page = repository
        .log_page_bounded(
            "main",
            second.id,
            None,
            1,
            TraversalBudget {
                max_commits: 1,
                max_decoded_bytes: 1024 * 1024,
                max_elapsed: Duration::from_secs(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(first_page.commits[0].0, second.id);
    let second_page = repository
        .log_page_bounded(
            "main",
            second.id,
            first_page.continuation.as_ref(),
            2,
            TraversalBudget::default(),
        )
        .await
        .unwrap();
    assert_eq!(second_page.commits[0].0, first.id);
    assert_eq!(second_page.commits[1].0, root);

    let mut closure = repository.start_commit_closure(&[second.id]).await.unwrap();
    let mut closure_ids = Vec::new();
    loop {
        let page = repository
            .commit_closure_page(&closure, 1, 1)
            .await
            .unwrap();
        closure_ids.extend(page.commits.iter().map(|(id, _)| *id));
        closure = page.cursor;
        if page.complete {
            break;
        }
    }
    assert_eq!(closure_ids, vec![root, first.id, second.id]);

    let diff = repository.diff("main", first.id, second.id).await.unwrap();
    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].key, b"b.txt");
    assert!(diff[0].from.is_none());
    assert!(diff[0].to.is_some());

    let before_reset = repository.open_reflog("main").await.unwrap();
    let newest = repository
        .read_reflog_page(&before_reset, 1)
        .await
        .unwrap()
        .entries
        .remove(0);
    assert_eq!(newest.event.new_target, second.id);

    clock.advance(1).unwrap();
    let reset = repository
        .reset_branch("main", first.id, second.id, "undo b.txt")
        .await
        .unwrap();
    assert_eq!(reset.old_target, second.id);
    assert_eq!(repository.head("main").await.unwrap(), first.id);

    let reflog = repository.open_reflog("main").await.unwrap();
    let reset_event = repository
        .read_reflog_page(&reflog, 1)
        .await
        .unwrap()
        .entries
        .remove(0);
    assert_eq!(reset_event.event.old_target, Some(second.id));
    assert_eq!(reset_event.event.new_target, first.id);

    clock.advance(1).unwrap();
    let recovered = repository
        .recover_branch(
            "main",
            reset_event.event.reflog,
            first.id,
            "recover reset target",
        )
        .await
        .unwrap();
    assert_eq!(recovered.new_target, second.id);
    assert_eq!(repository.head("main").await.unwrap(), second.id);

    let mut fsck = repository.start_fsck("main", true).await.unwrap();
    let report = loop {
        let page = repository.advance_fsck(&fsck, 1).await.unwrap();
        fsck = page.cursor;
        if page.complete {
            break fsck.report;
        }
    };
    assert_eq!(report.commits, 3);
    assert_eq!(report.current_objects, 2);
    assert_eq!(report.logical_versions, 2);
    assert_eq!(report.deep_content_bytes_verified, 12);
}
