use std::ops::ControlFlow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(feature = "async-store")]
use std::{
    future::Future,
    task::{Context, Poll},
};

#[cfg(feature = "async-store")]
use prolly::{AsyncProlly, SyncStoreAsAsync};
use prolly::{
    BatchOp, BlobRef, BorrowedMergeResolver, Config, ConflictRef, Error, MemStore, MergeDecision,
    Mutation, NodeLayoutSpec, Prolly, Store, ValueRef, ValueRefView,
};

#[derive(Clone)]
struct NativeSharedCountingStore {
    inner: Arc<MemStore>,
    owned_reads: Arc<AtomicUsize>,
    shared_reads: Arc<AtomicUsize>,
    owned_batch_reads: Arc<AtomicUsize>,
    shared_batch_reads: Arc<AtomicUsize>,
}

impl NativeSharedCountingStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemStore::new()),
            owned_reads: Arc::new(AtomicUsize::new(0)),
            shared_reads: Arc::new(AtomicUsize::new(0)),
            owned_batch_reads: Arc::new(AtomicUsize::new(0)),
            shared_batch_reads: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Store for NativeSharedCountingStore {
    type Error = <MemStore as Store>::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.owned_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key)
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        self.shared_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_shared(key)
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.owned_batch_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.batch_get_ordered_unique(keys)
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        self.shared_batch_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.batch_get_shared_ordered_unique(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops)
    }
}

struct Select(MergeDecision);

impl BorrowedMergeResolver for Select {
    fn resolve(&self, _conflict: ConflictRef<'_>) -> MergeDecision {
        self.0.clone()
    }
}

#[cfg(feature = "async-store")]
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn fixture(layout: NodeLayoutSpec, count: usize) -> (Prolly<Arc<MemStore>>, prolly::Tree) {
    let config = Config::builder().node_layout(layout).build();
    let prolly = Prolly::new(Arc::new(MemStore::new()), config);
    let entries = (0..count)
        .map(|index| {
            (
                format!("key/{index:06}").into_bytes(),
                format!("value/{index:06}").into_bytes(),
            )
        })
        .collect();
    let tree = prolly.build_from_sorted_entries(entries).unwrap();
    (prolly, tree)
}

fn layouts() -> [NodeLayoutSpec; 3] {
    [
        NodeLayoutSpec::Plain,
        NodeLayoutSpec::PrefixCompressed,
        NodeLayoutSpec::OffsetTable,
    ]
}

fn tracked_batch(
    prolly: &Prolly<Arc<MemStore>>,
    base: &prolly::Tree,
    mutations: Vec<Mutation>,
) -> prolly::Tree {
    prolly
        .batch_with_lineage(base, Arc::new(mutations))
        .unwrap()
}

#[test]
fn lineage_merge_splices_materialized_append_suffix() {
    let (prolly, base) = fixture(NodeLayoutSpec::PrefixCompressed, 20_000);
    let left = tracked_batch(
        &prolly,
        &base,
        (20_000..22_000)
            .map(|index| Mutation::Upsert {
                key: format!("key/{index:06}").into_bytes(),
                val: format!("left/{index:06}").into_bytes(),
            })
            .collect(),
    );
    let right_mutations = (22_000..24_000)
        .map(|index| Mutation::Upsert {
            key: format!("key/{index:06}").into_bytes(),
            val: format!("right/{index:06}").into_bytes(),
        })
        .collect::<Vec<_>>();
    let right = tracked_batch(&prolly, &base, right_mutations.clone());
    let canonical = prolly.batch(&left, right_mutations).unwrap();

    let merged = prolly.merge_with(&base, &left, &right, None).unwrap();
    assert_eq!(merged.root, canonical.root);
    assert_eq!(prolly.len(&merged).unwrap(), 24_000);
    assert_eq!(
        prolly.get(&merged, b"key/021999").unwrap(),
        Some(b"left/021999".to_vec())
    );
    assert_eq!(
        prolly.get(&merged, b"key/023999").unwrap(),
        Some(b"right/023999".to_vec())
    );
}

#[test]
fn lineage_merge_reuses_disjoint_version_segments() {
    let (prolly, base) = fixture(NodeLayoutSpec::PrefixCompressed, 12_000);
    let left = tracked_batch(
        &prolly,
        &base,
        (1_000..2_000)
            .map(|index| Mutation::Upsert {
                key: format!("key/{index:06}").into_bytes(),
                val: format!("left/{index:06}").into_bytes(),
            })
            .collect(),
    );
    let right_mutations = (7_000..8_000)
        .map(|index| Mutation::Upsert {
            key: format!("key/{index:06}").into_bytes(),
            val: format!("right/{index:06}").into_bytes(),
        })
        .collect::<Vec<_>>();
    let right = tracked_batch(&prolly, &base, right_mutations.clone());
    let canonical = prolly.batch(&left, right_mutations).unwrap();

    let merged = prolly.merge_with(&base, &left, &right, None).unwrap();
    assert_eq!(merged.root, canonical.root);
    assert_eq!(prolly.len(&merged).unwrap(), 12_000);
    assert_eq!(
        prolly.get(&merged, b"key/001500").unwrap(),
        Some(b"left/001500".to_vec())
    );
    assert_eq!(
        prolly.get(&merged, b"key/007500").unwrap(),
        Some(b"right/007500".to_vec())
    );
}

#[test]
fn lineage_merge_resolves_overlapping_changes_without_replaying_tree() {
    let (prolly, base) = fixture(NodeLayoutSpec::PrefixCompressed, 8_000);
    let left = tracked_batch(
        &prolly,
        &base,
        (2_000..4_000)
            .map(|index| Mutation::Upsert {
                key: format!("key/{index:06}").into_bytes(),
                val: format!("left/{index:06}").into_bytes(),
            })
            .collect(),
    );
    let right = tracked_batch(
        &prolly,
        &base,
        (2_000..4_000)
            .map(|index| Mutation::Upsert {
                key: format!("key/{index:06}").into_bytes(),
                val: format!("right/{index:06}").into_bytes(),
            })
            .collect(),
    );

    let merged = prolly
        .merge_with(&base, &left, &right, Some(&Select(MergeDecision::UseLeft)))
        .unwrap();
    assert_eq!(merged.root, left.root);
}

#[test]
fn cold_sync_mutation_uses_native_shared_node_reads() {
    let store = NativeSharedCountingStore::new();
    let owned_reads = store.owned_reads.clone();
    let shared_reads = store.shared_reads.clone();
    let prolly = Prolly::new(store, Config::default());
    let entries = (0..1_024)
        .map(|index| {
            (
                format!("key/{index:06}").into_bytes(),
                format!("value/{index:06}").into_bytes(),
            )
        })
        .collect();
    let tree = prolly.build_from_sorted_entries(entries).unwrap();
    prolly.clear_cache();
    owned_reads.store(0, Ordering::Relaxed);
    shared_reads.store(0, Ordering::Relaxed);

    prolly
        .put(&tree, b"key/999999".to_vec(), b"new-value".to_vec())
        .unwrap();

    assert_eq!(owned_reads.load(Ordering::Relaxed), 0);
    assert!(shared_reads.load(Ordering::Relaxed) > 0);
}

#[test]
fn cold_sync_batch_mutation_uses_native_shared_node_reads() {
    let store = NativeSharedCountingStore::new();
    let owned_reads = store.owned_reads.clone();
    let shared_reads = store.shared_reads.clone();
    let owned_batch_reads = store.owned_batch_reads.clone();
    let shared_batch_reads = store.shared_batch_reads.clone();
    let prolly = Prolly::new(store, Config::default());
    let entries = (0..4_096)
        .map(|index| {
            (
                format!("key/{index:06}").into_bytes(),
                format!("value/{index:06}").into_bytes(),
            )
        })
        .collect();
    let tree = prolly.build_from_sorted_entries(entries).unwrap();
    prolly.clear_cache();
    owned_reads.store(0, Ordering::Relaxed);
    shared_reads.store(0, Ordering::Relaxed);
    owned_batch_reads.store(0, Ordering::Relaxed);
    shared_batch_reads.store(0, Ordering::Relaxed);
    let mutations = (0..4_096)
        .step_by(8)
        .map(|index| Mutation::Upsert {
            key: format!("key/{index:06}").into_bytes(),
            val: format!("changed/{index:06}").into_bytes(),
        })
        .collect();

    prolly.batch(&tree, mutations).unwrap();

    assert_eq!(owned_reads.load(Ordering::Relaxed), 0);
    assert_eq!(owned_batch_reads.load(Ordering::Relaxed), 0);
    assert!(shared_reads.load(Ordering::Relaxed) + shared_batch_reads.load(Ordering::Relaxed) > 0);
}

#[test]
fn borrowed_point_reads_match_owned_reads_for_every_layout() {
    for layout in layouts() {
        let (prolly, tree) = fixture(layout, 1_024);
        let mut session = prolly.read(&tree).unwrap();

        for index in [0, 1, 127, 511, 1_023] {
            let key = format!("key/{index:06}");
            let expected = format!("value/{index:06}").into_bytes();
            assert_eq!(
                session
                    .get_with(key.as_bytes(), |value| value.to_vec())
                    .unwrap(),
                Some(expected.clone())
            );
            assert_eq!(prolly.get(&tree, key.as_bytes()).unwrap(), Some(expected));
            assert!(session.contains_key(key.as_bytes()).unwrap());
        }

        let mut called = false;
        assert_eq!(
            session
                .get_with(b"key/missing", |_| {
                    called = true;
                })
                .unwrap(),
            None
        );
        assert!(!called);
        assert!(!session.contains_key(b"key/missing").unwrap());
    }
}

#[test]
fn borrowed_ranges_match_owned_forward_reverse_prefix_and_bounds() {
    for layout in layouts() {
        let (prolly, tree) = fixture(layout, 2_000);
        let start = b"key/000317";
        let end = Some(b"key/001743".as_slice());
        let expected = prolly
            .range(&tree, start, end)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let mut session = prolly.read(&tree).unwrap();
        let mut forward = Vec::new();
        assert_eq!(
            session
                .scan_range(start, end, |entry| forward.push(entry.to_owned()))
                .unwrap(),
            expected.len() as u64
        );
        assert_eq!(forward, expected);

        let mut reverse = Vec::new();
        assert_eq!(
            session
                .scan_range_reverse(start, end, |entry| reverse.push(entry.to_owned()))
                .unwrap(),
            expected.len() as u64
        );
        let mut expected_reverse = expected.clone();
        expected_reverse.reverse();
        assert_eq!(reverse, expected_reverse);

        let mut prefix = Vec::new();
        session
            .scan_prefix(b"key/0003", |entry| prefix.push(entry.to_owned()))
            .unwrap();
        assert!(prefix.iter().all(|(key, _)| key.starts_with(b"key/0003")));
        assert_eq!(prefix.len(), 100);

        assert_eq!(
            session
                .lower_bound_with(b"key/000317", |entry| entry.to_owned())
                .unwrap(),
            expected.first().cloned()
        );
        assert_eq!(
            session
                .upper_bound_with(b"key/000317", |entry| entry.to_owned())
                .unwrap()
                .unwrap()
                .0,
            b"key/000318"
        );
        assert_eq!(
            session
                .select_with(1_337, |entry| entry.to_owned())
                .unwrap()
                .unwrap()
                .0,
            b"key/001337"
        );
        assert_eq!(session.rank(b"key/001337").unwrap(), 1_337);
        assert_eq!(session.len().unwrap(), 2_000);
    }
}

#[test]
fn early_stop_counts_the_break_entry_and_preserves_order() {
    let (prolly, tree) = fixture(NodeLayoutSpec::PrefixCompressed, 1_000);
    let mut session = prolly.read(&tree).unwrap();
    let mut keys = Vec::new();
    let outcome = session
        .scan_range_until(b"key/000100", Some(b"key/000900"), |entry| {
            keys.push(entry.key().to_vec());
            if keys.len() == 37 {
                ControlFlow::Break(entry.value().to_vec())
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap();
    assert_eq!(outcome.visited, 37);
    assert_eq!(outcome.break_value, Some(b"value/000136".to_vec()));
    assert_eq!(keys.first().unwrap(), b"key/000100");
    assert_eq!(keys.last().unwrap(), b"key/000136");

    let mut reverse_keys = Vec::new();
    let outcome = session
        .scan_range_reverse_until(b"key/000100", Some(b"key/000900"), |entry| {
            reverse_keys.push(entry.key().to_vec());
            if reverse_keys.len() == 23 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap();
    assert_eq!(outcome.visited, 23);
    assert_eq!(reverse_keys.first().unwrap(), b"key/000899");
    assert_eq!(reverse_keys.last().unwrap(), b"key/000877");
}

#[test]
fn borrowed_multi_get_preserves_positions_duplicates_and_misses() {
    let (prolly, tree) = fixture(NodeLayoutSpec::OffsetTable, 512);
    let keys = [
        b"key/000400".as_slice(),
        b"missing".as_slice(),
        b"key/000002".as_slice(),
        b"key/000400".as_slice(),
    ];
    let mut actual = Vec::new();
    let mut session = prolly.read(&tree).unwrap();
    session
        .get_many_with(&keys, |position, key, value| {
            actual.push((position, key.to_vec(), value.map(<[u8]>::to_vec)));
        })
        .unwrap();

    assert_eq!(actual.len(), keys.len());
    assert_eq!(actual[0].2, Some(b"value/000400".to_vec()));
    assert_eq!(actual[1].2, None);
    assert_eq!(actual[2].2, Some(b"value/000002".to_vec()));
    assert_eq!(actual[3].2, Some(b"value/000400".to_vec()));
    assert_eq!(
        actual.iter().map(|entry| entry.0).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
}

#[test]
fn borrowed_large_value_views_match_owned_decoding() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let mut tree = prolly.create();
    tree = prolly
        .put(&tree, b"raw".to_vec(), b"raw-value".to_vec())
        .unwrap();
    tree = prolly
        .put(
            &tree,
            b"escaped".to_vec(),
            ValueRef::Inline(b"PLVB-inline".to_vec()).to_bytes(),
        )
        .unwrap();
    let blob = BlobRef::from_bytes(b"external-value");
    tree = prolly
        .put(
            &tree,
            b"blob".to_vec(),
            ValueRef::Blob(blob.clone()).to_bytes(),
        )
        .unwrap();

    assert_eq!(
        prolly
            .get_value_ref_with(&tree, b"raw", |view| view.to_owned())
            .unwrap(),
        Some(ValueRef::Inline(b"raw-value".to_vec()))
    );
    assert_eq!(
        prolly
            .get_value_ref_with(&tree, b"escaped", |view| match view {
                ValueRefView::Inline(value) => value.to_vec(),
                ValueRefView::Blob { .. } => unreachable!(),
            })
            .unwrap(),
        Some(b"PLVB-inline".to_vec())
    );
    assert_eq!(
        prolly
            .get_value_ref_with(&tree, b"blob", |view| view.to_owned())
            .unwrap(),
        Some(ValueRef::Blob(blob))
    );
}

#[test]
fn read_session_rejects_persisted_format_mismatch_even_for_empty_tree() {
    let source = Prolly::new(
        MemStore::new(),
        Config::builder().node_layout(NodeLayoutSpec::Plain).build(),
    );
    let tree = source.create();
    let other = Prolly::new(
        MemStore::new(),
        Config::builder()
            .node_layout(NodeLayoutSpec::OffsetTable)
            .build(),
    );
    assert!(matches!(
        other.read(&tree),
        Err(Error::FormatMismatch { .. })
    ));
}

#[test]
fn callbacks_can_reenter_manager_reads_without_cache_lock_deadlock() {
    let (prolly, tree) = fixture(NodeLayoutSpec::Plain, 128);
    let mut checked = 0usize;
    prolly
        .scan_range(&tree, b"key/000010", Some(b"key/000020"), |entry| {
            let owned = prolly.get(&tree, entry.key()).unwrap().unwrap();
            assert_eq!(owned, entry.value());
            checked += 1;
        })
        .unwrap();
    assert_eq!(checked, 10);
}

#[test]
fn borrowed_structural_diff_matches_owned_diff_and_range_filtering() {
    for layout in layouts() {
        let (prolly, base) = fixture(layout, 2_000);
        let mut other = base.clone();
        for index in (100..1_900).step_by(137) {
            other = prolly
                .put(
                    &other,
                    format!("key/{index:06}").into_bytes(),
                    format!("changed/{index:06}").into_bytes(),
                )
                .unwrap();
        }
        for index in (150..1_700).step_by(211) {
            other = prolly
                .delete(&other, format!("key/{index:06}").as_bytes())
                .unwrap();
        }
        for index in 2_000..2_075 {
            other = prolly
                .put(
                    &other,
                    format!("key/{index:06}").into_bytes(),
                    format!("added/{index:06}").into_bytes(),
                )
                .unwrap();
        }

        let expected = prolly.diff(&base, &other).unwrap();
        let mut actual = Vec::new();
        assert_eq!(
            prolly
                .scan_diff(&base, &other, |diff| actual.push(diff.to_owned()))
                .unwrap(),
            expected.len() as u64
        );
        assert_eq!(actual, expected);

        let start = b"key/000500";
        let end = Some(b"key/001500".as_slice());
        let expected_range = expected
            .iter()
            .filter(|diff| diff.key() >= start && end.map_or(true, |end| diff.key() < end))
            .cloned()
            .collect::<Vec<_>>();
        let mut actual_range = Vec::new();
        prolly
            .scan_range_diff(&base, &other, start, end, |diff| {
                actual_range.push(diff.to_owned())
            })
            .unwrap();
        assert_eq!(actual_range, expected_range);

        let stopped = prolly
            .scan_diff_until(&base, &other, |_| ControlFlow::Break("done"))
            .unwrap();
        assert_eq!(stopped.visited, u64::from(!expected.is_empty()));
        assert_eq!(
            stopped.break_value,
            (!expected.is_empty()).then_some("done")
        );
        assert_eq!(prolly.scan_diff(&base, &base, |_| {}).unwrap(), 0);
    }
}

#[test]
fn borrowed_conflict_scan_matches_legacy_conflict_stream() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let mut base = prolly.create();
    for (key, value) in [(b"a", b"base-a"), (b"b", b"base-b"), (b"c", b"base-c")] {
        base = prolly.put(&base, key.to_vec(), value.to_vec()).unwrap();
    }
    let mut left = prolly
        .put(&base, b"a".to_vec(), b"left-a".to_vec())
        .unwrap();
    left = prolly.delete(&left, b"b").unwrap();
    left = prolly
        .put(&left, b"d".to_vec(), b"left-d".to_vec())
        .unwrap();

    let mut right = prolly
        .put(&base, b"a".to_vec(), b"right-a".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"b".to_vec(), b"right-b".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"c".to_vec(), b"right-c".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"d".to_vec(), b"right-d".to_vec())
        .unwrap();

    let legacy = prolly
        .stream_conflicts(&base, &left, &right)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .map(|conflict| (conflict.key, conflict.base, conflict.left, conflict.right))
        .collect::<Vec<_>>();
    let mut borrowed = Vec::new();
    assert_eq!(
        prolly
            .scan_conflicts(&base, &left, &right, |conflict| {
                let conflict = conflict.to_owned();
                borrowed.push((conflict.key, conflict.base, conflict.left, conflict.right));
            })
            .unwrap(),
        legacy.len() as u64
    );
    assert_eq!(borrowed, legacy);
    assert_eq!(
        borrowed
            .iter()
            .map(|conflict| conflict.0.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"b".as_slice(), b"d".as_slice()]
    );

    let stopped = prolly
        .scan_conflicts_until(&base, &left, &right, |conflict| {
            ControlFlow::Break(conflict.key.to_vec())
        })
        .unwrap();
    assert_eq!(stopped.visited, 1);
    assert_eq!(stopped.break_value, Some(b"a".to_vec()));
}

#[test]
fn borrowed_merge_matches_legacy_and_supports_symbolic_decisions() {
    for layout in layouts() {
        let (prolly, base) = fixture(layout, 1_000);
        let mut left = base.clone();
        let mut right = base.clone();

        for index in (50..850).step_by(79) {
            left = prolly
                .put(
                    &left,
                    format!("key/{index:06}").into_bytes(),
                    format!("left/{index:06}").into_bytes(),
                )
                .unwrap();
            right = prolly
                .put(
                    &right,
                    format!("key/{index:06}").into_bytes(),
                    format!("right/{index:06}").into_bytes(),
                )
                .unwrap();
        }
        left = prolly
            .put(&left, b"left-only".to_vec(), b"left".to_vec())
            .unwrap();
        right = prolly
            .put(&right, b"right-only".to_vec(), b"right".to_vec())
            .unwrap();

        let legacy = prolly
            .merge(
                &base,
                &left,
                &right,
                Some(Box::new(|conflict| {
                    conflict
                        .left
                        .clone()
                        .map_or_else(prolly::Resolution::delete, prolly::Resolution::value)
                })),
            )
            .unwrap();
        let borrowed = prolly
            .merge_with(&base, &left, &right, Some(&Select(MergeDecision::UseLeft)))
            .unwrap();
        assert_eq!(borrowed.root, legacy.root);

        let prefer_right = prolly
            .merge_with(&base, &left, &right, Some(&Select(MergeDecision::UseRight)))
            .unwrap();
        for index in (50..850).step_by(79) {
            assert_eq!(
                prolly
                    .get(&prefer_right, format!("key/{index:06}").as_bytes())
                    .unwrap(),
                Some(format!("right/{index:06}").into_bytes())
            );
        }

        let prefer_base = prolly
            .merge_with(&base, &left, &right, Some(&Select(MergeDecision::UseBase)))
            .unwrap();
        for index in (50..850).step_by(79) {
            assert_eq!(
                prolly
                    .get(&prefer_base, format!("key/{index:06}").as_bytes())
                    .unwrap(),
                Some(format!("value/{index:06}").into_bytes())
            );
        }

        assert!(matches!(
            prolly.merge_with(
                &base,
                &left,
                &right,
                Some(&Select(MergeDecision::Unresolved)),
            ),
            Err(Error::Conflict(_))
        ));
    }
}

#[test]
fn borrowed_merge_batches_large_fallbacks_without_changing_the_canonical_root() {
    let (prolly, base) = fixture(NodeLayoutSpec::OffsetTable, 10_000);
    let left = prolly
        .put(&base, b"key/left-only".to_vec(), b"left".to_vec())
        .unwrap();
    let right_mutations = (0..9_000)
        .step_by(2)
        .map(|index| Mutation::Upsert {
            key: format!("key/{index:06}").into_bytes(),
            val: format!("right/{index:06}").into_bytes(),
        })
        .collect::<Vec<_>>();
    assert!(right_mutations.len() > 4_096);
    let right = prolly.batch(&base, right_mutations.clone()).unwrap();

    let expected = prolly.batch(&left, right_mutations).unwrap();
    let merged = prolly.merge_with(&base, &left, &right, None).unwrap();
    assert_eq!(merged.root, expected.root);
}

#[test]
fn borrowed_range_and_prefix_merge_do_not_modify_outside_selection() {
    let (prolly, base) = fixture(NodeLayoutSpec::PrefixCompressed, 200);
    let left = prolly
        .put(&base, b"key/000150".to_vec(), b"left-150".to_vec())
        .unwrap();
    let mut right = prolly
        .put(&base, b"key/000050".to_vec(), b"right-050".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"key/000150".to_vec(), b"right-150".to_vec())
        .unwrap();

    let ranged = prolly
        .merge_range_with(
            &base,
            &left,
            &right,
            b"key/000000",
            Some(b"key/000100"),
            None,
        )
        .unwrap();
    assert_eq!(
        prolly.get(&ranged, b"key/000050").unwrap(),
        Some(b"right-050".to_vec())
    );
    assert_eq!(
        prolly.get(&ranged, b"key/000150").unwrap(),
        Some(b"left-150".to_vec())
    );

    let prefixed = prolly
        .merge_prefix_with(&base, &left, &right, b"key/0000", None)
        .unwrap();
    assert_eq!(
        prolly.get(&prefixed, b"key/000050").unwrap(),
        Some(b"right-050".to_vec())
    );
    assert_eq!(
        prolly.get(&prefixed, b"key/000150").unwrap(),
        Some(b"left-150".to_vec())
    );
}

#[cfg(feature = "async-store")]
#[test]
fn sync_to_async_adapter_preserves_native_shared_reads() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        Store::put(&store, b"shared", b"retained").unwrap();
        let adapter = SyncStoreAsAsync::new(store.clone());

        assert!(prolly::AsyncStore::has_native_shared_reads(&adapter));
        let direct = Store::get_shared(&store, b"shared").unwrap().unwrap();
        let adapted = prolly::AsyncStore::get_shared(&adapter, b"shared")
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&direct, &adapted));
    });
}

#[cfg(feature = "async-store")]
#[test]
fn async_borrowed_reads_match_sync_results_without_owned_iterator_entries() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let config = Config::builder()
            .node_layout(NodeLayoutSpec::OffsetTable)
            .build();
        let sync = Prolly::new(store.clone(), config.clone());
        let entries = (0..512)
            .map(|index| {
                (
                    format!("key/{index:06}").into_bytes(),
                    format!("value/{index:06}").into_bytes(),
                )
            })
            .collect();
        let tree = sync.build_from_sorted_entries(entries).unwrap();
        let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
        let mut session = async_prolly.read(&tree).await.unwrap();

        assert_eq!(
            session
                .get_with(b"key/000123", |value| value.to_vec())
                .await
                .unwrap(),
            Some(b"value/000123".to_vec())
        );
        assert_eq!(
            session
                .get_with(b"missing", |_| unreachable!())
                .await
                .unwrap(),
            None
        );

        let keys = [
            b"key/000400".as_slice(),
            b"missing".as_slice(),
            b"key/000002".as_slice(),
            b"key/000400".as_slice(),
        ];
        let mut values = Vec::new();
        session
            .get_many_with(&keys, |position, _, value| {
                values.push((position, value.map(<[u8]>::to_vec)));
            })
            .await
            .unwrap();
        assert_eq!(values[0].1, Some(b"value/000400".to_vec()));
        assert_eq!(values[1].1, None);
        assert_eq!(values[3].1, values[0].1);

        let mut scanned = Vec::new();
        assert_eq!(
            session
                .scan_range(b"key/000100", Some(b"key/000125"), |entry| {
                    scanned.push(entry.to_owned())
                })
                .await
                .unwrap(),
            25
        );
        assert_eq!(scanned.first().unwrap().0, b"key/000100");
        assert_eq!(scanned.last().unwrap().0, b"key/000124");

        let mut reverse = Vec::new();
        let stopped = session
            .scan_range_reverse_until(b"key/000100", Some(b"key/000125"), |entry| {
                reverse.push(entry.key().to_vec());
                if reverse.len() == 7 {
                    ControlFlow::Break(entry.value().to_vec())
                } else {
                    ControlFlow::Continue(())
                }
            })
            .await
            .unwrap();
        assert_eq!(stopped.visited, 7);
        assert_eq!(reverse.first().unwrap(), b"key/000124");
        assert_eq!(reverse.last().unwrap(), b"key/000118");
        assert_eq!(stopped.break_value, Some(b"value/000118".to_vec()));

        let mut iterator = async_prolly
            .range(&tree, b"key/000200", Some(b"key/000202"))
            .await
            .unwrap();
        let first = iterator
            .next_with(|entry| (entry.key().to_vec(), entry.value().to_vec()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.0, b"key/000200");
        assert_eq!(
            iterator.resume_cursor().after(),
            Some(b"key/000200".as_slice())
        );

        let mut other = async_prolly
            .put(&tree, b"key/000123".to_vec(), b"changed".to_vec())
            .await
            .unwrap();
        other = async_prolly.delete(&other, b"key/000321").await.unwrap();
        other = async_prolly
            .put(&other, b"key/000999".to_vec(), b"added".to_vec())
            .await
            .unwrap();
        let expected = sync.diff(&tree, &other).unwrap();
        let mut actual = Vec::new();
        assert_eq!(
            async_prolly
                .scan_diff(&tree, &other, |diff| actual.push(diff.to_owned()))
                .await
                .unwrap(),
            expected.len() as u64
        );
        assert_eq!(actual, expected);
        let stopped = async_prolly
            .scan_diff_until(&tree, &other, |diff| {
                ControlFlow::Break(diff.key().to_vec())
            })
            .await
            .unwrap();
        assert_eq!(stopped.visited, 1);
        assert_eq!(stopped.break_value, Some(expected[0].key().to_vec()));

        let left = async_prolly
            .put(&tree, b"key/000123".to_vec(), b"left".to_vec())
            .await
            .unwrap();
        let mut right = async_prolly
            .put(&tree, b"key/000123".to_vec(), b"right".to_vec())
            .await
            .unwrap();
        right = async_prolly
            .put(&right, b"key/000999".to_vec(), b"right-only".to_vec())
            .await
            .unwrap();
        let mut conflicts = Vec::new();
        assert_eq!(
            async_prolly
                .scan_conflicts(&tree, &left, &right, |conflict| {
                    conflicts.push(conflict.to_owned())
                })
                .await
                .unwrap(),
            1
        );
        assert_eq!(conflicts[0].key, b"key/000123");

        let resolver = Select(MergeDecision::UseRight);
        let merged = async_prolly
            .merge_with(&tree, &left, &right, Some(&resolver))
            .await
            .unwrap();
        assert_eq!(
            async_prolly.get(&merged, b"key/000123").await.unwrap(),
            Some(b"right".to_vec())
        );
        assert_eq!(
            async_prolly.get(&merged, b"key/000999").await.unwrap(),
            Some(b"right-only".to_vec())
        );
    });
}
