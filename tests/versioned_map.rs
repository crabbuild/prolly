use prolly::{
    verify_authenticated_proof_bundle, ChangedSpan, Config, Diff, FileNodeStore, LargeValueConfig,
    MapVersionId, MemBlobStore, MemStore, MergePolicyRegistry, Mutation, ParallelConfig, Prolly,
    ProofAuthentication, RangeCursor, Resolution, ReverseCursor, StringKeyCodec, ValueRef,
    VersionedJsonCodec, VersionedMapBackup, VersionedMapUpdate,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn versioned_map_supports_history_diff_rollback_and_retention() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let map = prolly.versioned_map(b"documents");

    assert_eq!(map.get(b"doc/1").unwrap(), None);
    let empty = map.apply_at_millis(Vec::new(), 1_000).unwrap();
    let first = map
        .apply_at_millis(
            vec![Mutation::Upsert {
                key: b"doc/1".to_vec(),
                val: b"draft".to_vec(),
            }],
            2_000,
        )
        .unwrap();
    let second = map
        .apply_at_millis(
            vec![
                Mutation::Upsert {
                    key: b"doc/1".to_vec(),
                    val: b"published".to_vec(),
                },
                Mutation::Upsert {
                    key: b"doc/2".to_vec(),
                    val: b"new".to_vec(),
                },
            ],
            3_000,
        )
        .unwrap();

    assert_eq!(map.get(b"doc/1").unwrap(), Some(b"published".to_vec()));
    assert_eq!(
        map.get_at(&first.id, b"doc/1").unwrap(),
        Some(b"draft".to_vec())
    );
    let first_entries = map
        .range_at(&first.id, b"doc/", Some(b"doc0"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(first_entries, vec![(b"doc/1".to_vec(), b"draft".to_vec())]);
    let current_entries = map
        .range(b"doc/", Some(b"doc0"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(current_entries.len(), 2);
    assert_eq!(
        map.diff(&first.id, &second.id).unwrap(),
        vec![
            Diff::Changed {
                key: b"doc/1".to_vec(),
                old: b"draft".to_vec(),
                new: b"published".to_vec(),
            },
            Diff::Added {
                key: b"doc/2".to_vec(),
                val: b"new".to_vec(),
            },
        ]
    );

    let versions = map.versions().unwrap();
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0].id, second.id);
    assert_eq!(versions[1].id, first.id);
    assert_eq!(versions[2].id, empty.id);
    assert!(versions[0].is_head);

    let rolled_back = map.rollback_to(&first.id).unwrap();
    assert_eq!(rolled_back.id, first.id);
    assert_eq!(map.get(b"doc/1").unwrap(), Some(b"draft".to_vec()));
    assert_eq!(map.get(b"doc/2").unwrap(), None);
    assert_eq!(map.versions().unwrap().len(), 3);

    let retained = prolly
        .load_retained_named_roots(&map.retention_policy())
        .unwrap();
    assert_eq!(retained.roots.len(), 4); // head plus three immutable versions
    let plan = prolly
        .plan_store_gc_for_retention(&map.retention_policy())
        .unwrap();
    assert_eq!(plan.reclaimable_nodes, 0);
}

#[test]
fn conditional_update_detects_a_stale_head_without_writing() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let map = prolly.versioned_map(b"settings");
    let initial = map.put(b"theme".to_vec(), b"light".to_vec()).unwrap();
    let current = map.put(b"theme".to_vec(), b"dark".to_vec()).unwrap();

    let update = map
        .apply_if(
            Some(&initial.id),
            vec![Mutation::Upsert {
                key: b"theme".to_vec(),
                val: b"stale".to_vec(),
            }],
        )
        .unwrap();

    assert!(matches!(
        update,
        VersionedMapUpdate::Conflict {
            current: Some(ref observed)
        } if observed.id == current.id
    ));
    assert_eq!(map.get(b"theme").unwrap(), Some(b"dark".to_vec()));
    assert_eq!(map.versions().unwrap().len(), 2);
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct UserV1 {
    display_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct UserV2 {
    display_name: String,
    active: bool,
}

#[test]
fn typed_map_validates_schema_migrates_with_cas_and_subscribes_to_changes() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let map = prolly.versioned_map(b"typed-users");
    let old_codec = VersionedJsonCodec::new("app.User", 1);
    let old = map.typed::<String, UserV1, _, _>(StringKeyCodec, old_codec.clone());
    let source = old
        .put(
            &"user/1".to_string(),
            &UserV1 {
                display_name: "Ada".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        old.get(&"user/1".to_string()).unwrap(),
        Some(UserV1 {
            display_name: "Ada".to_string(),
        })
    );

    let current_codec = VersionedJsonCodec::new("app.User", 2);
    let current = map.typed::<String, UserV2, _, _>(StringKeyCodec, current_codec.clone());
    assert!(current.get(&"user/1".to_string()).is_err());

    let migrated = current
        .migrate_from::<UserV1, _>(&source.id, &old_codec, |old| {
            Ok(UserV2 {
                display_name: old.display_name,
                active: true,
            })
        })
        .unwrap();
    assert_eq!(migrated.scanned_values, 1);
    assert_eq!(migrated.rewritten_values, 1);
    let migrated_head = migrated.update.current().unwrap().id.clone();
    assert_eq!(
        current.get(&"user/1".to_string()).unwrap(),
        Some(UserV2 {
            display_name: "Ada".to_string(),
            active: true,
        })
    );

    let mut subscription = map.subscribe().unwrap();
    assert!(subscription.poll().unwrap().is_none());
    current
        .put(
            &"user/2".to_string(),
            &UserV2 {
                display_name: "Grace".to_string(),
                active: false,
            },
        )
        .unwrap();
    let event = subscription.poll().unwrap().unwrap();
    assert_eq!(event.previous, Some(migrated_head));
    assert_eq!(event.diffs.len(), 1);
    assert!(subscription.poll().unwrap().is_none());

    let mut resumed = map.subscribe_from(Some(source.id));
    let resumed_event = resumed.poll().unwrap().unwrap();
    assert_eq!(resumed_event.diffs.len(), 2);
}

#[test]
fn managed_read_pagination_and_conditional_edit_helpers_share_snapshots() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let map = prolly.versioned_map(b"read-helpers");
    assert!(!map.is_initialized().unwrap());

    let first = map
        .edit(|edit| {
            edit.put(b"user/1", b"Ada");
            edit.put(b"user/2", b"Grace");
            edit.put(b"team/1", b"Compiler");
        })
        .unwrap();
    assert!(map.is_initialized().unwrap());
    assert_eq!(map.head_id().unwrap(), Some(first.id.clone()));
    assert!(map.contains_key(b"user/1").unwrap());
    assert!(!map.contains_key(b"missing").unwrap());

    let values = map
        .get_many(&[b"user/2".to_vec(), b"missing".to_vec(), b"user/1".to_vec()])
        .unwrap();
    assert_eq!(
        values,
        vec![Some(b"Grace".to_vec()), None, Some(b"Ada".to_vec())]
    );
    let users = map
        .prefix(b"user/")
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(users.len(), 2);

    let first_page = map
        .prefix_page_at(&first.id, b"user/", &RangeCursor::start(), 1)
        .unwrap();
    assert_eq!(first_page.entries.len(), 1);
    let second_page = map
        .prefix_page_at(
            &first.id,
            b"user/",
            first_page.next_cursor.as_ref().unwrap(),
            1,
        )
        .unwrap();
    assert_eq!(second_page.entries.len(), 1);
    assert_ne!(first_page.entries[0].0, second_page.entries[0].0);

    let applied = map
        .edit_if(Some(&first.id), |edit| {
            edit.put(b"user/3", b"Margaret");
            edit.delete(b"team/1");
        })
        .unwrap();
    let current = match applied {
        VersionedMapUpdate::Applied { current, .. } => current,
        other => panic!("expected applied conditional edit, got {other:?}"),
    };
    assert_eq!(map.changes_since(&first.id).unwrap().len(), 2);
    assert_eq!(
        map.get_many_at(&first.id, &[b"user/3", b"team/1"]).unwrap(),
        vec![None, Some(b"Compiler".to_vec())]
    );

    let stale = map.put_if(Some(&first.id), b"user/4", b"stale").unwrap();
    assert!(matches!(stale, VersionedMapUpdate::Conflict { .. }));
    assert_eq!(map.head_id().unwrap(), Some(current.id));
}

#[test]
fn pruning_keeps_newest_versions_and_an_older_rolled_back_head() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let map = prolly.versioned_map(b"pruning");
    let first = map
        .apply_at_millis(
            vec![Mutation::Upsert {
                key: b"value".to_vec(),
                val: b"one".to_vec(),
            }],
            1_000,
        )
        .unwrap();
    let middle = map
        .apply_at_millis(
            vec![Mutation::Upsert {
                key: b"value".to_vec(),
                val: b"two".to_vec(),
            }],
            2_000,
        )
        .unwrap();
    let newest = map
        .apply_at_millis(
            vec![Mutation::Upsert {
                key: b"value".to_vec(),
                val: b"three".to_vec(),
            }],
            3_000,
        )
        .unwrap();

    map.rollback_to(&first.id).unwrap();
    let pruned = map.prune_versions(1).unwrap();
    assert_eq!(pruned.removed, vec![middle.id.clone()]);
    assert!(pruned.retained.contains(&first.id));
    assert!(pruned.retained.contains(&newest.id));
    assert_eq!(map.versions().unwrap().len(), 2);
    assert!(map.version(&middle.id).unwrap().is_none());
    assert_eq!(map.get(b"value").unwrap(), Some(b"one".to_vec()));
    assert_eq!(
        map.get_at(&newest.id, b"value").unwrap(),
        Some(b"three".to_vec())
    );

    let other = prolly.versioned_map(b"other-map");
    other.put(b"must-survive", vec![7; 4_096]).unwrap();
    let plan = prolly.versioned_map(b"pruning").plan_gc().unwrap();
    assert!(plan.reclaimable_nodes > 0);
    map.sweep_gc().unwrap();
    assert_eq!(map.get(b"value").unwrap(), Some(b"one".to_vec()));
    assert_eq!(
        map.get_at(&newest.id, b"value").unwrap(),
        Some(b"three".to_vec())
    );
    assert_eq!(other.get(b"must-survive").unwrap(), Some(vec![7; 4_096]));
    let verified = map.verify_catalog().unwrap();
    assert_eq!(verified.version_count, 2);
    assert!(verified.reachable_nodes > 0);
}

#[test]
fn retention_presets_keep_time_windows_and_explicit_versions_plus_head() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let map = prolly.versioned_map(b"retention-presets");
    let first = map
        .apply_at_millis(
            vec![Mutation::Upsert {
                key: b"k".to_vec(),
                val: b"1".to_vec(),
            }],
            1_000,
        )
        .unwrap();
    let second = map
        .apply_at_millis(
            vec![Mutation::Upsert {
                key: b"k".to_vec(),
                val: b"2".to_vec(),
            }],
            2_000,
        )
        .unwrap();
    let third = map
        .apply_at_millis(
            vec![Mutation::Upsert {
                key: b"k".to_vec(),
                val: b"3".to_vec(),
            }],
            3_000,
        )
        .unwrap();

    let time_prune = map
        .keep_for_at(3_500, std::time::Duration::from_millis(1_600))
        .unwrap();
    assert_eq!(time_prune.removed, vec![first.id]);
    assert!(time_prune.retained.contains(&second.id));
    assert!(time_prune.retained.contains(&third.id));

    let exact_prune = map.keep_versions([&second.id]).unwrap();
    assert!(exact_prune.retained.contains(&second.id));
    assert!(exact_prune.retained.contains(&third.id)); // current head is mandatory
    assert!(map
        .keep_versions([&MapVersionId::for_tree(&prolly.create()).unwrap()])
        .is_err());
}

#[test]
fn pinned_snapshot_exposes_queries_proofs_export_sync_and_cache_controls() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let map = prolly.versioned_map(b"snapshot-surface");
    let version = map
        .edit(|edit| {
            edit.put(b"doc/1", b"one");
            edit.put(b"doc/2", b"two");
            edit.put(b"doc/3", b"three");
        })
        .unwrap();
    let snapshot = map.snapshot().unwrap().unwrap();
    assert_eq!(snapshot.id(), &version.id);
    assert_eq!(snapshot.first_entry().unwrap().unwrap().0, b"doc/1");
    assert_eq!(snapshot.last_entry().unwrap().unwrap().0, b"doc/3");
    assert_eq!(snapshot.lower_bound(b"doc/2").unwrap().unwrap().0, b"doc/2");
    assert_eq!(snapshot.upper_bound(b"doc/2").unwrap().unwrap().0, b"doc/3");

    let reverse = snapshot
        .prefix_reverse_page(b"doc/", &ReverseCursor::end(), 2)
        .unwrap();
    assert_eq!(reverse.entries.len(), 2);
    assert_eq!(reverse.entries[0].0, b"doc/3");
    let reverse_scan = snapshot
        .prefix_reverse_scan(b"doc/", 1)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        reverse_scan
            .iter()
            .map(|(key, _)| key.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"doc/3".as_slice(),
            b"doc/2".as_slice(),
            b"doc/1".as_slice()
        ]
    );
    let window = snapshot.cursor_window(b"doc/2", None, 2).unwrap();
    assert!(window.found);
    assert_eq!(window.entries.len(), 2);
    assert_eq!(snapshot.stats().unwrap().total_key_value_pairs, 3);
    assert!(!snapshot.debug_view().unwrap().levels.is_empty());

    let key_proof = snapshot.prove_key(b"doc/2").unwrap();
    assert_eq!(key_proof.verify().value, Some(b"two".to_vec()));
    assert!(snapshot
        .prove_keys(&[b"doc/1".as_slice(), b"missing".as_slice()])
        .unwrap()
        .verify()
        .all_valid());
    assert_eq!(
        snapshot
            .prove_prefix(b"doc/")
            .unwrap()
            .verify()
            .entries
            .len(),
        3
    );
    let proved_page = snapshot
        .prove_range_page(&RangeCursor::start(), None, 2)
        .unwrap();
    assert!(proved_page.proof.verify().valid);

    let envelope = snapshot
        .authenticate_proof_bundle(
            key_proof.to_bundle_bytes().unwrap(),
            b"secret",
            ProofAuthentication::new(b"test-key")
                .with_context(b"snapshot-test")
                .with_validity(Some(1_000), Some(2_000))
                .with_nonce(b"nonce"),
        )
        .unwrap();
    let authenticated =
        verify_authenticated_proof_bundle(&envelope.to_bytes().unwrap(), b"secret", Some(1_500))
            .unwrap();
    assert!(authenticated.valid);

    let bundle = snapshot.export().unwrap();
    assert!(bundle.verify().unwrap().valid);
    let destination = MemStore::new();
    assert!(
        snapshot
            .plan_missing_nodes(&destination)
            .unwrap()
            .missing_nodes
            > 0
    );
    snapshot.copy_missing_nodes(&destination).unwrap();
    let destination_prolly = Prolly::new(destination, snapshot.tree().config.clone());
    assert_eq!(
        destination_prolly.get(snapshot.tree(), b"doc/3").unwrap(),
        Some(b"three".to_vec())
    );
    snapshot.pin_root().unwrap();
    snapshot.pin_path(b"doc/").unwrap();
}

#[test]
fn pinned_comparison_exposes_streaming_pages_proofs_stats_and_hints() {
    let path = temporary_store_path("comparison-hints");
    let prolly = Prolly::new(FileNodeStore::open(&path).unwrap(), Config::default());
    let map = prolly.versioned_map(b"comparison-surface");
    let base = map
        .edit(|edit| {
            edit.put(b"a", b"1");
            edit.put(b"b", b"2");
        })
        .unwrap();
    let target = map
        .edit(|edit| {
            edit.put(b"b", b"changed");
            edit.put(b"c", b"3");
        })
        .unwrap();
    let comparison = map.compare(&base.id, &target.id).unwrap();

    let eager = comparison.diff().unwrap();
    let streamed = comparison
        .stream_diff()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(streamed, eager);
    assert_eq!(
        comparison
            .diff_page(&RangeCursor::start(), None, 1)
            .unwrap()
            .diffs
            .len(),
        1
    );
    assert_eq!(
        comparison
            .structural_diff_page(None, 1)
            .unwrap()
            .diffs
            .len(),
        1
    );
    assert!(
        comparison
            .prove_diff_page(&RangeCursor::start(), None, 10)
            .unwrap()
            .proof
            .verify()
            .valid
    );
    assert_eq!(comparison.stats().unwrap().after.total_key_value_pairs, 3);
    assert!(comparison.debug_view().unwrap().right_only_nodes > 0);

    assert!(comparison
        .publish_changed_spans([ChangedSpan::from_key(b"b")])
        .unwrap());
    let hint = comparison.changed_spans().unwrap().unwrap();
    assert_eq!(hint.spans, vec![ChangedSpan::from_key(b"b")]);

    drop(comparison);
    drop(map);
    drop(prolly);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn complete_catalog_backup_restore_and_snapshot_push_round_trip() {
    let source = Prolly::new(MemStore::new(), Config::default());
    let source_map = source.versioned_map(b"portable");
    let first = source_map.put(b"value", b"one").unwrap();
    let second = source_map.put(b"value", b"two").unwrap();
    let third = source_map.put(b"value", b"three").unwrap();
    source_map.rollback_to(&second.id).unwrap();

    let backup = source_map.backup().unwrap();
    backup.verify().unwrap();
    assert_eq!(backup.head, second.id);
    assert_eq!(backup.versions.len(), 3);
    let decoded = VersionedMapBackup::from_bytes(&backup.to_bytes().unwrap()).unwrap();
    assert_eq!(decoded, backup);

    let restored_engine = Prolly::new(MemStore::new(), Config::default());
    let restored = restored_engine.versioned_map(b"portable");
    let restored_head = restored.restore_backup(&decoded).unwrap();
    assert_eq!(restored_head.id, second.id);
    assert_eq!(restored.versions().unwrap().len(), 3);
    assert_eq!(restored.get(b"value").unwrap(), Some(b"two".to_vec()));
    assert_eq!(
        restored.get_at(&first.id, b"value").unwrap(),
        Some(b"one".to_vec())
    );

    let pushed_engine = Prolly::new(MemStore::new(), Config::default());
    let pushed = pushed_engine.versioned_map(b"different-name");
    let third_snapshot = source_map.snapshot_at(&third.id).unwrap().unwrap();
    let pushed_head = third_snapshot.push_to(&pushed).unwrap();
    assert_eq!(pushed_head.id, third.id);
    assert_eq!(pushed.get(b"value").unwrap(), Some(b"three".to_vec()));
    assert!(pushed.restore_backup(&decoded).is_err());
}

#[test]
fn pinned_merge_streams_conflicts_and_cas_publishes_standard_and_policy_merges() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let map = prolly.versioned_map(b"merge-workflow");
    let base = map.put(b"base", b"1").unwrap();
    let head = map.put(b"left", b"2").unwrap();
    map.rollback_to(&base.id).unwrap();
    let candidate = map.put(b"right", b"3").unwrap();
    map.rollback_to(&head.id).unwrap();

    let merge = map.prepare_merge(&base.id, &candidate.id).unwrap();
    assert!(merge
        .stream_conflicts()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .is_empty());
    assert!(merge.publish(None).unwrap().is_applied());
    assert_eq!(map.get(b"left").unwrap(), Some(b"2".to_vec()));
    assert_eq!(map.get(b"right").unwrap(), Some(b"3".to_vec()));

    let conflict_base = map.put(b"choice", b"base").unwrap();
    let conflict_head = map.put(b"choice", b"head").unwrap();
    map.rollback_to(&conflict_base.id).unwrap();
    let conflict_candidate = map.put(b"choice", b"candidate").unwrap();
    map.rollback_to(&conflict_head.id).unwrap();
    let conflict_merge = map
        .prepare_merge(&conflict_base.id, &conflict_candidate.id)
        .unwrap();
    assert_eq!(
        conflict_merge
            .stream_conflicts()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        1
    );
    let policies = MergePolicyRegistry::with_default(|conflict| {
        conflict
            .right
            .clone()
            .map(Resolution::value)
            .unwrap_or_else(Resolution::delete)
    });
    assert!(conflict_merge
        .publish_with_policy(&policies)
        .unwrap()
        .is_applied());
    assert_eq!(map.get(b"choice").unwrap(), Some(b"candidate".to_vec()));

    let stale = map
        .prepare_merge(&conflict_base.id, &conflict_candidate.id)
        .unwrap();
    map.put(b"concurrent", b"change").unwrap();
    assert!(stale.publish_with_policy(&policies).unwrap().is_conflict());
}

#[test]
fn large_values_and_blob_gc_follow_retained_map_versions() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let blobs = MemBlobStore::new();
    let map = prolly.versioned_map(b"blob-values");
    let policy = LargeValueConfig::new(2);
    let first = map
        .put_large_value(&blobs, b"body", b"first body", policy.clone())
        .unwrap();
    let second = map
        .put_large_value(&blobs, b"body", b"second body", policy)
        .unwrap();
    let other = prolly.versioned_map(b"other-blob-values");
    other
        .put_large_value(&blobs, b"body", b"other map body", LargeValueConfig::new(2))
        .unwrap();

    assert_eq!(
        map.get_large_value(&blobs, b"body").unwrap(),
        Some(b"second body".to_vec())
    );
    let first_snapshot = map.snapshot_at(&first.id).unwrap().unwrap();
    assert!(matches!(
        first_snapshot.get_value_ref(b"body").unwrap(),
        Some(ValueRef::Blob(_))
    ));
    assert_eq!(
        first_snapshot.get_large_value(&blobs, b"body").unwrap(),
        Some(b"first body".to_vec())
    );
    assert!(map.plan_blob_gc(&blobs).unwrap().is_empty());

    let stale = map
        .put_large_value_if(
            &blobs,
            Some(&first.id),
            b"body",
            b"stale body",
            LargeValueConfig::new(2),
        )
        .unwrap();
    assert!(stale.is_conflict());

    map.keep_versions([&second.id]).unwrap();
    let plan = map.plan_blob_gc(&blobs).unwrap();
    assert_eq!(plan.reclaimable_blob_count, 1);
    assert_eq!(map.sweep_blob_gc(&blobs).unwrap().deleted_blobs, 1);
    assert_eq!(
        map.get_large_value(&blobs, b"body").unwrap(),
        Some(b"second body".to_vec())
    );
    assert_eq!(
        other.get_large_value(&blobs, b"body").unwrap(),
        Some(b"other map body".to_vec())
    );
}

#[test]
fn sorted_rebuild_append_and_parallel_ingestion_publish_managed_versions() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let map = prolly.versioned_map(b"ingestion");
    let initialized = map
        .initialize_sorted([
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ])
        .unwrap();
    let initial = initialized.current().unwrap().clone();
    assert_eq!(map.get(b"b").unwrap(), Some(b"2".to_vec()));

    let appended = map
        .append(vec![
            Mutation::Upsert {
                key: b"c".to_vec(),
                val: b"3".to_vec(),
            },
            Mutation::Upsert {
                key: b"d".to_vec(),
                val: b"4".to_vec(),
            },
        ])
        .unwrap();
    assert_ne!(appended.id, initial.id);

    let parallel = map
        .parallel_apply(
            vec![
                Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"updated".to_vec(),
                },
                Mutation::Delete { key: b"d".to_vec() },
            ],
            &ParallelConfig::default(),
        )
        .unwrap();
    assert_eq!(map.get(b"a").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(map.get(b"d").unwrap(), None);

    let rebuilt = map
        .rebuild_from_iter_if(
            Some(&parallel.version.id),
            [
                (b"z".to_vec(), b"last".to_vec()),
                (b"x".to_vec(), b"first".to_vec()),
            ],
        )
        .unwrap();
    assert!(rebuilt.is_applied());
    let rebuilt_snapshot = map.snapshot().unwrap().unwrap();
    assert_eq!(rebuilt_snapshot.first_entry().unwrap().unwrap().0, b"x");
    assert_eq!(rebuilt_snapshot.last_entry().unwrap().unwrap().0, b"z");

    let stale = map
        .rebuild_sorted_if(Some(&initial.id), [(b"no".to_vec(), b"write".to_vec())])
        .unwrap();
    assert!(stale.is_conflict());
}

#[test]
fn multi_map_transaction_keeps_source_secondary_index_and_view_atomic() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    prolly
        .versioned_maps_transaction(|maps| {
            maps.put(b"users", b"user/1", b"Ada")?;
            maps.put(b"users", b"user/1/status", b"active")?;
            maps.put(b"users_by_email", b"ada@example.com/user/1", b"")?;
            maps.put(b"active_user_count", b"count", 1_u64.to_be_bytes())?;
            Ok(())
        })
        .unwrap();

    let users = prolly.versioned_map(b"users");
    let emails = prolly.versioned_map(b"users_by_email");
    let count = prolly.versioned_map(b"active_user_count");
    assert_eq!(users.get(b"user/1").unwrap(), Some(b"Ada".to_vec()));
    assert_eq!(
        users.get(b"user/1/status").unwrap(),
        Some(b"active".to_vec())
    );
    assert!(emails.contains_key(b"ada@example.com/user/1").unwrap());
    assert_eq!(
        count.get(b"count").unwrap(),
        Some(1_u64.to_be_bytes().to_vec())
    );

    let failed = prolly.versioned_maps_transaction(|maps| {
        maps.put(b"users", b"user/2", b"Grace")?;
        maps.put(b"users_by_email", b"grace@example.com/user/2", b"")?;
        Err::<(), _>(prolly::Error::InvalidVersionedMap(
            "simulated derived-index failure".to_string(),
        ))
    });
    assert!(failed.is_err());
    assert_eq!(users.get(b"user/2").unwrap(), None);
    assert!(!emails.contains_key(b"grace@example.com/user/2").unwrap());
}

#[test]
fn map_ids_and_retention_prefixes_are_isolated() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let short = prolly.versioned_map(b"a");
    let longer = prolly.versioned_map(b"ab");
    short.put(b"owner", b"short").unwrap();
    longer.put(b"owner", b"longer").unwrap();

    assert_ne!(short.head_name(), longer.head_name());
    let retained = prolly
        .load_retained_named_roots(&short.retention_policy())
        .unwrap();
    assert_eq!(retained.roots.len(), 2); // short index head plus one version
}

#[test]
fn concurrent_convenience_updates_retry_and_preserve_independent_keys() {
    let prolly = Arc::new(Prolly::new(Arc::new(MemStore::new()), Config::default()));
    prolly.versioned_map(b"concurrent").initialize().unwrap();
    let barrier = Arc::new(Barrier::new(3));

    let handles = [
        (b"left".to_vec(), b"1".to_vec()),
        (b"right".to_vec(), b"2".to_vec()),
    ]
    .into_iter()
    .map(|(key, value)| {
        let prolly = prolly.clone();
        let barrier = barrier.clone();
        thread::spawn(move || {
            barrier.wait();
            prolly.versioned_map(b"concurrent").put(key, value).unwrap()
        })
    })
    .collect::<Vec<_>>();

    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }

    let map = prolly.versioned_map(b"concurrent");
    assert_eq!(map.get(b"left").unwrap(), Some(b"1".to_vec()));
    assert_eq!(map.get(b"right").unwrap(), Some(b"2".to_vec()));
    assert_eq!(map.versions().unwrap().len(), 3);
}

#[test]
fn file_store_reopens_head_and_version_catalog() {
    let path = temporary_store_path("versioned-map");
    let first_id;

    {
        let store = FileNodeStore::open(&path).unwrap();
        let prolly = Prolly::new(store, Config::default());
        let map = prolly.versioned_map(b"durable");
        first_id = map
            .edit(|edit| {
                edit.put(b"a".to_vec(), b"1".to_vec());
                edit.put(b"b".to_vec(), b"2".to_vec());
            })
            .unwrap()
            .id;
    }

    {
        let store = FileNodeStore::open(&path).unwrap();
        let prolly = Prolly::new(store, Config::default());
        let map = prolly.versioned_map(b"durable");
        assert_eq!(map.head().unwrap().unwrap().id, first_id);
        assert_eq!(map.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(map.versions().unwrap().len(), 1);
    }

    std::fs::remove_dir_all(path).unwrap();
}

fn temporary_store_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("prolly-{label}-{}-{nonce}", std::process::id()))
}
