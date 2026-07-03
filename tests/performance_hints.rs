use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use prolly::{BatchOp, ChangedSpan, Config, Prolly, Store};

type HintKey = (Vec<u8>, Vec<u8>);

#[derive(Default)]
struct CountingHintStore {
    nodes: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    hints: Mutex<HashMap<HintKey, Vec<u8>>>,
    get_calls: AtomicUsize,
    batch_get_ordered_calls: AtomicUsize,
    get_hint_calls: AtomicUsize,
    put_hint_calls: AtomicUsize,
}

impl CountingHintStore {
    fn reset_counts(&self) {
        self.get_calls.store(0, Ordering::Relaxed);
        self.batch_get_ordered_calls.store(0, Ordering::Relaxed);
        self.get_hint_calls.store(0, Ordering::Relaxed);
        self.put_hint_calls.store(0, Ordering::Relaxed);
    }

    fn get_calls(&self) -> usize {
        self.get_calls.load(Ordering::Relaxed)
    }

    fn batch_get_ordered_calls(&self) -> usize {
        self.batch_get_ordered_calls.load(Ordering::Relaxed)
    }

    fn get_hint_calls(&self) -> usize {
        self.get_hint_calls.load(Ordering::Relaxed)
    }

    fn put_hint_calls(&self) -> usize {
        self.put_hint_calls.load(Ordering::Relaxed)
    }

    fn corrupt_hints(&self) {
        for value in self.hints.lock().unwrap().values_mut() {
            *value = b"not-cbor".to_vec();
        }
    }
}

impl Store for CountingHintStore {
    type Error = Infallible;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.nodes.lock().unwrap().get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.nodes
            .lock()
            .unwrap()
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.nodes.lock().unwrap().remove(key);
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let mut nodes = self.nodes.lock().unwrap();
        for op in ops {
            match op {
                BatchOp::Upsert { key, value } => {
                    nodes.insert((*key).to_vec(), (*value).to_vec());
                }
                BatchOp::Delete { key } => {
                    nodes.remove(*key);
                }
            }
        }
        Ok(())
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.batch_get_ordered_calls.fetch_add(1, Ordering::Relaxed);
        let nodes = self.nodes.lock().unwrap();
        Ok(keys.iter().map(|key| nodes.get(*key).cloned()).collect())
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let mut nodes = self.nodes.lock().unwrap();
        for (key, value) in entries {
            nodes.insert((*key).to_vec(), (*value).to_vec());
        }
        Ok(())
    }

    fn supports_hints(&self) -> bool {
        true
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.get_hint_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .hints
            .lock()
            .unwrap()
            .get(&(namespace.to_vec(), key.to_vec()))
            .cloned())
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.put_hint_calls.fetch_add(1, Ordering::Relaxed);
        self.hints
            .lock()
            .unwrap()
            .insert((namespace.to_vec(), key.to_vec()), value.to_vec());
        Ok(())
    }
}

fn hint_test_config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build()
}

fn build_tenant_tree(
    prolly: &Prolly<Arc<CountingHintStore>>,
) -> Result<prolly::Tree, prolly::Error> {
    let mut tree = prolly.create();
    for tenant in 0..5 {
        for item in 0..20 {
            let key = format!("tenant/{tenant:02}/k{item:03}").into_bytes();
            let value = format!("value/{tenant:02}/{item:03}").into_bytes();
            tree = prolly.put(&tree, key, value)?;
        }
    }
    Ok(tree)
}

#[test]
fn prefix_path_hint_hydrates_hot_range_path_in_fresh_manager() {
    let store = Arc::new(CountingHintStore::default());
    let config = hint_test_config();
    let prolly = Prolly::new(store.clone(), config.clone());
    let tree = build_tenant_tree(&prolly).unwrap();
    let prefix = b"tenant/03/";

    assert!(prolly.publish_prefix_path_hint(&tree, prefix).unwrap());
    assert!(store.put_hint_calls() > 0);

    let fresh = Prolly::new(store.clone(), config);
    store.reset_counts();

    assert!(fresh.hydrate_prefix_path_hint(&tree, prefix).unwrap());
    assert_eq!(store.get_hint_calls(), 1);
    assert_eq!(store.batch_get_ordered_calls(), 1);
    assert!(fresh.cache_len() > 0);

    let point_gets_after_hydrate = store.get_calls();
    let value = fresh.get(&tree, b"tenant/03/k000").unwrap();
    assert_eq!(value, Some(b"value/03/000".to_vec()));
    assert_eq!(
        store.get_calls(),
        point_gets_after_hydrate,
        "hydrated prefix path should let the first hot-prefix lookup reuse cached nodes"
    );
}

#[test]
fn malformed_prefix_path_hint_is_ignored_without_breaking_lookup() {
    let store = Arc::new(CountingHintStore::default());
    let config = hint_test_config();
    let prolly = Prolly::new(store.clone(), config.clone());
    let tree = build_tenant_tree(&prolly).unwrap();
    let prefix = b"tenant/02/";

    assert!(prolly.publish_prefix_path_hint(&tree, prefix).unwrap());
    store.corrupt_hints();

    let fresh = Prolly::new(store, config);
    assert!(!fresh.hydrate_prefix_path_hint(&tree, prefix).unwrap());
    assert_eq!(
        fresh.get(&tree, b"tenant/02/k000").unwrap(),
        Some(b"value/02/000".to_vec())
    );
}

#[test]
fn missing_prefix_path_hint_is_a_noop() {
    let store = Arc::new(CountingHintStore::default());
    let config = hint_test_config();
    let prolly = Prolly::new(store, config);
    let tree = build_tenant_tree(&prolly).unwrap();

    assert!(!prolly
        .hydrate_prefix_path_hint(&tree, b"tenant/not-published/")
        .unwrap());
}

#[test]
fn changed_span_hint_round_trips_normalized_spans_for_index_jobs() {
    let store = Arc::new(CountingHintStore::default());
    let config = hint_test_config();
    let prolly = Prolly::new(store, config);
    let base = build_tenant_tree(&prolly).unwrap();

    let changed = prolly
        .put(
            &base,
            b"tenant/03/k000".to_vec(),
            b"value/03/000-updated".to_vec(),
        )
        .unwrap();
    let changed = prolly
        .put(
            &changed,
            b"tenant/04/k001".to_vec(),
            b"value/04/001-updated".to_vec(),
        )
        .unwrap();

    assert!(prolly
        .publish_changed_spans_hint(
            &base,
            &changed,
            vec![
                ChangedSpan::from_key(b"tenant/04/k001".to_vec()),
                ChangedSpan::from_key(b"tenant/03/k000".to_vec()),
                ChangedSpan::for_prefix(b"tenant/03/".to_vec()),
            ],
        )
        .unwrap());

    let hint = prolly
        .load_changed_spans_hint(&base, &changed)
        .unwrap()
        .expect("changed span hint exists");
    assert_eq!(hint.base_root, base.root);
    assert_eq!(hint.changed_root, changed.root);
    assert_eq!(
        hint.spans,
        vec![
            ChangedSpan::for_prefix(b"tenant/03/".to_vec()),
            ChangedSpan::from_key(b"tenant/04/k001".to_vec()),
        ]
    );

    let mut hinted_diffs = Vec::new();
    for span in &hint.spans {
        hinted_diffs.extend(
            prolly
                .range_diff(&base, &changed, &span.start, span.end.as_deref())
                .unwrap(),
        );
    }
    assert_eq!(hinted_diffs.len(), 2);
    assert!(hinted_diffs
        .iter()
        .any(|diff| diff.key() == b"tenant/03/k000"));
    assert!(hinted_diffs
        .iter()
        .any(|diff| diff.key() == b"tenant/04/k001"));
}

#[test]
fn changed_span_hint_ignores_malformed_or_missing_hints() {
    let store = Arc::new(CountingHintStore::default());
    let config = hint_test_config();
    let prolly = Prolly::new(store.clone(), config);
    let base = build_tenant_tree(&prolly).unwrap();
    let changed = prolly
        .put(&base, b"tenant/01/k000".to_vec(), b"changed".to_vec())
        .unwrap();

    assert_eq!(
        prolly.load_changed_spans_hint(&base, &changed).unwrap(),
        None
    );
    assert!(prolly
        .publish_changed_spans_hint(
            &base,
            &changed,
            vec![ChangedSpan::from_key(b"tenant/01/k000".to_vec())],
        )
        .unwrap());

    store.corrupt_hints();
    assert_eq!(
        prolly.load_changed_spans_hint(&base, &changed).unwrap(),
        None
    );
}

#[test]
fn invalid_changed_spans_are_not_persisted() {
    let store = Arc::new(CountingHintStore::default());
    let config = hint_test_config();
    let prolly = Prolly::new(store, config);
    let base = build_tenant_tree(&prolly).unwrap();
    let changed = prolly
        .put(&base, b"tenant/00/k000".to_vec(), b"changed".to_vec())
        .unwrap();

    assert!(!prolly
        .publish_changed_spans_hint(
            &base,
            &changed,
            vec![ChangedSpan::new(
                b"tenant/02/".to_vec(),
                Some(b"tenant/01/".to_vec()),
            )],
        )
        .unwrap());
}
