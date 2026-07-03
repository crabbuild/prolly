use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{
    BlobRef, BlobStore, BlobStoreScan, Config, Error, FileBlobStore, LargeValueConfig,
    MemBlobStore, MemStore, Prolly, ValueRef,
};

struct TempBlobDir {
    path: PathBuf,
}

impl TempBlobDir {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let path =
            std::env::temp_dir().join(format!("prolly-{name}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempBlobDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn large_value_helpers_keep_small_values_inline_and_offload_large_values() {
    let node_store = Arc::new(MemStore::new());
    let blob_store = MemBlobStore::new();
    let prolly = Prolly::new(node_store, Config::default());
    let config = LargeValueConfig::new(8);

    let tree = prolly.create();
    let tree = prolly
        .put_large_value(
            &blob_store,
            &tree,
            b"small".to_vec(),
            b"tiny".to_vec(),
            config.clone(),
        )
        .unwrap();
    let large = b"this payload is larger than the inline threshold".to_vec();
    let tree = prolly
        .put_large_value(&blob_store, &tree, b"large".to_vec(), large.clone(), config)
        .unwrap();

    assert_eq!(blob_store.len().unwrap(), 1);
    assert_eq!(prolly.get(&tree, b"small").unwrap(), Some(b"tiny".to_vec()));
    assert_eq!(
        prolly
            .get_large_value(&blob_store, &tree, b"small")
            .unwrap(),
        Some(b"tiny".to_vec())
    );
    assert_eq!(
        prolly
            .get_large_value(&blob_store, &tree, b"large")
            .unwrap(),
        Some(large.clone())
    );

    let stored = prolly.get_value_ref(&tree, b"large").unwrap().unwrap();
    let ValueRef::Blob(reference) = stored else {
        panic!("large value should be represented by a blob reference");
    };
    assert_eq!(reference.len, large.len() as u64);
    assert_eq!(blob_store.get_blob(&reference).unwrap(), Some(large));
}

#[test]
fn small_values_with_value_ref_magic_are_escaped() {
    let blob_store = MemBlobStore::new();
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let config = LargeValueConfig::new(64);
    let value = b"PLVB-user-data".to_vec();

    let tree = prolly.create();
    let tree = prolly
        .put_large_value(&blob_store, &tree, b"magic".to_vec(), value.clone(), config)
        .unwrap();

    assert!(blob_store.is_empty().unwrap());
    assert_ne!(prolly.get(&tree, b"magic").unwrap(), Some(value.clone()));
    assert_eq!(
        prolly
            .get_large_value(&blob_store, &tree, b"magic")
            .unwrap(),
        Some(value.clone())
    );
    assert_eq!(
        prolly.get_value_ref(&tree, b"magic").unwrap(),
        Some(ValueRef::Inline(value))
    );
}

#[test]
fn missing_offloaded_blob_returns_not_found() {
    let blob_store = MemBlobStore::new();
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let config = LargeValueConfig::new(1);
    let value = b"large".to_vec();

    let tree = prolly.create();
    let tree = prolly
        .put_large_value(&blob_store, &tree, b"k".to_vec(), value.clone(), config)
        .unwrap();
    let ValueRef::Blob(reference) = prolly.get_value_ref(&tree, b"k").unwrap().unwrap() else {
        panic!("value should be offloaded");
    };
    blob_store.delete_blob(&reference).unwrap();

    let err = prolly
        .get_large_value(&blob_store, &tree, b"k")
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(cid) if cid == reference.cid));
}

#[test]
fn repeated_large_value_put_is_idempotent_and_deduplicates_blob() {
    let blob_store = MemBlobStore::new();
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let config = LargeValueConfig::new(2);
    let value = b"same-large-value".to_vec();

    let tree = prolly.create();
    let first = prolly
        .put_large_value(
            &blob_store,
            &tree,
            b"k".to_vec(),
            value.clone(),
            config.clone(),
        )
        .unwrap();
    let second = prolly
        .put_large_value(&blob_store, &first, b"k".to_vec(), value, config)
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(blob_store.len().unwrap(), 1);
}

#[test]
fn blob_gc_plans_reclaimable_offloaded_values_without_deleting() {
    let blob_store = MemBlobStore::new();
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let config = LargeValueConfig::new(1);
    let old_value = b"old large payload".to_vec();
    let new_value = b"new large payload".to_vec();

    let base = prolly.create();
    let base = prolly
        .put_large_value(
            &blob_store,
            &base,
            b"k".to_vec(),
            old_value.clone(),
            config.clone(),
        )
        .unwrap();
    let ValueRef::Blob(old_ref) = prolly.get_value_ref(&base, b"k").unwrap().unwrap() else {
        panic!("old value should be offloaded");
    };

    let current = prolly
        .put_large_value(
            &blob_store,
            &base,
            b"k".to_vec(),
            new_value.clone(),
            config.clone(),
        )
        .unwrap();
    let current = prolly
        .put_large_value(
            &blob_store,
            &current,
            b"k2".to_vec(),
            new_value.clone(),
            config,
        )
        .unwrap();
    let ValueRef::Blob(new_ref) = prolly.get_value_ref(&current, b"k").unwrap().unwrap() else {
        panic!("new value should be offloaded");
    };
    let missing_ref = BlobRef::from_bytes(b"missing blob");
    let candidates = vec![
        old_ref.clone(),
        new_ref.clone(),
        new_ref.clone(),
        missing_ref,
    ];

    let reachable = prolly
        .mark_reachable_blobs(std::slice::from_ref(&current))
        .unwrap();
    assert_eq!(reachable.live_blob_count, 1);
    assert_eq!(reachable.live_blob_bytes, new_value.len() as u64);
    assert_eq!(reachable.scanned_values, 2);
    assert!(reachable.contains(&new_ref));
    assert!(!reachable.contains(&old_ref));

    let plan = prolly
        .plan_blob_gc(&blob_store, std::slice::from_ref(&current), &candidates)
        .unwrap();

    assert_eq!(plan.candidate_blobs, 3);
    assert_eq!(plan.retained_candidate_blobs(), 1);
    assert_eq!(plan.missing_candidates, 1);
    assert_eq!(plan.reclaimable_blob_count, 1);
    assert_eq!(plan.reclaimable_blob_bytes, old_value.len() as u64);
    assert_eq!(plan.reclaimable_blobs(), std::slice::from_ref(&old_ref));
    assert_eq!(blob_store.get_blob(&old_ref).unwrap(), Some(old_value));
    assert_eq!(blob_store.get_blob(&new_ref).unwrap(), Some(new_value));
}

#[test]
fn sweep_blob_gc_deletes_only_unreachable_offloaded_values() {
    let blob_store = MemBlobStore::new();
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let config = LargeValueConfig::new(1);
    let old_value = b"old large payload".to_vec();
    let new_value = b"new large payload".to_vec();

    let base = prolly.create();
    let base = prolly
        .put_large_value(
            &blob_store,
            &base,
            b"k".to_vec(),
            old_value.clone(),
            config.clone(),
        )
        .unwrap();
    let ValueRef::Blob(old_ref) = prolly.get_value_ref(&base, b"k").unwrap().unwrap() else {
        panic!("old value should be offloaded");
    };

    let current = prolly
        .put_large_value(&blob_store, &base, b"k".to_vec(), new_value.clone(), config)
        .unwrap();
    let ValueRef::Blob(new_ref) = prolly.get_value_ref(&current, b"k").unwrap().unwrap() else {
        panic!("new value should be offloaded");
    };

    let candidates = vec![old_ref.clone(), new_ref.clone()];
    let sweep = prolly
        .sweep_blob_gc(&blob_store, std::slice::from_ref(&current), &candidates)
        .unwrap();

    assert_eq!(sweep.deleted_blobs, 1);
    assert_eq!(sweep.deleted_blob_bytes, old_value.len() as u64);
    assert_eq!(
        sweep.plan.reclaimable_blobs(),
        std::slice::from_ref(&old_ref)
    );
    assert_eq!(blob_store.get_blob(&old_ref).unwrap(), None);
    assert_eq!(
        blob_store.get_blob(&new_ref).unwrap(),
        Some(new_value.clone())
    );
    assert_eq!(
        prolly.get_large_value(&blob_store, &current, b"k").unwrap(),
        Some(new_value)
    );
    assert!(matches!(
        prolly.get_large_value(&blob_store, &base, b"k"),
        Err(Error::NotFound(cid)) if cid == old_ref.cid
    ));
}

#[test]
fn file_blob_store_persists_lists_and_deletes_blobs() {
    let temp = TempBlobDir::new("file-blob-store");
    let store = FileBlobStore::open(temp.path()).unwrap();

    let first = store.put_blob(b"first payload").unwrap();
    let duplicate = store.put_blob(b"first payload").unwrap();
    let second = store.put_blob(b"second payload").unwrap();

    assert_eq!(first, duplicate);
    assert!(store.path_for_ref(&first).exists());
    assert_eq!(
        store.get_blob(&first).unwrap(),
        Some(b"first payload".to_vec())
    );

    let listed = store.list_blob_refs().unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.contains(&first));
    assert!(listed.contains(&second));

    let reopened = FileBlobStore::open(temp.path()).unwrap();
    assert_eq!(
        reopened.get_blob(&second).unwrap(),
        Some(b"second payload".to_vec())
    );
    assert_eq!(reopened.list_blob_refs().unwrap(), listed);

    reopened.delete_blob(&first).unwrap();
    assert_eq!(reopened.get_blob(&first).unwrap(), None);
    assert_eq!(reopened.list_blob_refs().unwrap(), vec![second]);
}

#[test]
fn file_blob_store_gc_uses_backend_listing_for_candidates() {
    let temp = TempBlobDir::new("file-blob-store-gc");
    let blob_store = FileBlobStore::open(temp.path()).unwrap();
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let config = LargeValueConfig::new(1);
    let old_value = b"old durable payload".to_vec();
    let new_value = b"new durable payload".to_vec();

    let base = prolly.create();
    let base = prolly
        .put_large_value(
            &blob_store,
            &base,
            b"k".to_vec(),
            old_value.clone(),
            config.clone(),
        )
        .unwrap();
    let ValueRef::Blob(old_ref) = prolly.get_value_ref(&base, b"k").unwrap().unwrap() else {
        panic!("old value should be offloaded");
    };

    let current = prolly
        .put_large_value(&blob_store, &base, b"k".to_vec(), new_value.clone(), config)
        .unwrap();
    let ValueRef::Blob(new_ref) = prolly.get_value_ref(&current, b"k").unwrap().unwrap() else {
        panic!("new value should be offloaded");
    };

    assert_eq!(blob_store.list_blob_refs().unwrap().len(), 2);
    let plan = prolly
        .plan_blob_store_gc(&blob_store, std::slice::from_ref(&current))
        .unwrap();
    assert_eq!(plan.reclaimable_blobs(), std::slice::from_ref(&old_ref));

    let sweep = prolly
        .sweep_blob_store_gc(&blob_store, std::slice::from_ref(&current))
        .unwrap();

    assert_eq!(sweep.deleted_blobs, 1);
    assert_eq!(sweep.deleted_blob_bytes, old_value.len() as u64);
    assert_eq!(blob_store.get_blob(&old_ref).unwrap(), None);
    assert_eq!(
        blob_store.get_blob(&new_ref).unwrap(),
        Some(new_value.clone())
    );
    assert_eq!(blob_store.list_blob_refs().unwrap(), vec![new_ref.clone()]);
    assert_eq!(
        prolly.get_large_value(&blob_store, &current, b"k").unwrap(),
        Some(new_value)
    );
}
