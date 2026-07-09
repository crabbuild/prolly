use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{
    Config, Error, FileNodeStore, ManifestStore, MemStore, NamedRootUpdate, NodeStoreScan, Prolly,
    Store, TransactionUpdate, TransactionalStore,
};

#[cfg(feature = "sqlite")]
use prolly::SqliteStore;

fn memory_prolly() -> Prolly<Arc<MemStore>> {
    Prolly::new(Arc::new(MemStore::new()), Config::default())
}

fn temp_store_path(prefix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
}

fn with_file_prolly(test: impl FnOnce(&Prolly<Arc<FileNodeStore>>)) {
    let path = temp_store_path("prolly-transaction-test");
    let store = Arc::new(FileNodeStore::open(&path).unwrap());
    let prolly = Prolly::new(store, Config::default());
    test(&prolly);
    let _ = std::fs::remove_dir_all(path);
}

#[cfg(feature = "sqlite")]
fn sqlite_prolly() -> Prolly<Arc<SqliteStore>> {
    Prolly::new(
        Arc::new(SqliteStore::open_in_memory().unwrap()),
        Config::default(),
    )
}

fn assert_transaction_rolls_back_staged_writes_when_closure_fails<S>(prolly: &Prolly<S>)
where
    S: Store + ManifestStore + TransactionalStore + NodeStoreScan,
{
    let result: Result<(), Error> = prolly.transaction(|tx| {
        let tree = tx.put(
            &tx.create(),
            b"ticket/123/status".to_vec(),
            b"open".to_vec(),
        )?;
        tx.publish_named_root(b"tickets/source/current", &tree)?;
        Err(Error::Serialize("forced failure".to_string()))
    });

    assert!(matches!(result, Err(Error::Serialize(_))));
    assert_eq!(
        prolly.load_named_root(b"tickets/source/current").unwrap(),
        None
    );
    assert!(prolly.store().list_node_cids().unwrap().is_empty());
}

fn assert_transaction_commits_multiple_named_roots_atomically<S>(prolly: &Prolly<S>)
where
    S: Store + ManifestStore + TransactionalStore,
{
    let (source, by_status) = prolly
        .transaction(|tx| {
            let source = tx.put(
                &tx.create(),
                b"ticket/123/status".to_vec(),
                b"open".to_vec(),
            )?;
            let by_status = tx.put(
                &tx.create(),
                b"by_status/open/123".to_vec(),
                b"ticket/123".to_vec(),
            )?;
            tx.publish_named_root(b"tickets/source/current", &source)?;
            tx.publish_named_root(b"tickets/view/by-status/current", &by_status)?;
            Ok((source, by_status))
        })
        .unwrap();

    assert_eq!(
        prolly.load_named_root(b"tickets/source/current").unwrap(),
        Some(source.clone())
    );
    assert_eq!(
        prolly
            .load_named_root(b"tickets/view/by-status/current")
            .unwrap(),
        Some(by_status.clone())
    );
    assert_eq!(
        prolly.get(&source, b"ticket/123/status").unwrap(),
        Some(b"open".to_vec())
    );
    assert_eq!(
        prolly.get(&by_status, b"by_status/open/123").unwrap(),
        Some(b"ticket/123".to_vec())
    );
}

fn assert_transaction_reads_its_own_staged_tree_writes<S>(prolly: &Prolly<S>)
where
    S: Store + ManifestStore + TransactionalStore,
{
    let committed = prolly
        .transaction(|tx| {
            let tree = tx.put(&tx.create(), b"name".to_vec(), b"crabdb".to_vec())?;
            assert_eq!(tx.get(&tree, b"name")?, Some(b"crabdb".to_vec()));
            assert_eq!(prolly.load_named_root(b"main")?, None);
            tx.publish_named_root(b"main", &tree)?;
            Ok(tree)
        })
        .unwrap();

    assert_eq!(
        prolly.load_named_root(b"main").unwrap(),
        Some(committed.clone())
    );
    assert_eq!(
        prolly.get(&committed, b"name").unwrap(),
        Some(b"crabdb".to_vec())
    );
}

fn assert_transaction_detects_concurrent_writer_from_root_read_set<S>(prolly: &Prolly<S>)
where
    S: Store + ManifestStore + TransactionalStore,
{
    let base = prolly
        .put(
            &prolly.create(),
            b"ticket/123/status".to_vec(),
            b"open".to_vec(),
        )
        .unwrap();
    prolly
        .publish_named_root(b"tickets/current", &base)
        .unwrap();

    let result: Result<(), Error> = prolly.transaction(|tx| {
        let current = tx.load_named_root(b"tickets/current")?.unwrap();
        let tx_tree = tx.put(
            &current,
            b"ticket/123/status".to_vec(),
            b"in-progress".to_vec(),
        )?;
        tx.publish_named_root(b"tickets/current", &tx_tree)?;

        let outside = prolly.put(&current, b"ticket/123/status".to_vec(), b"closed".to_vec())?;
        let update = prolly.compare_and_swap_named_root(
            b"tickets/current",
            Some(&current),
            Some(&outside),
        )?;
        assert!(matches!(update, NamedRootUpdate::Applied));
        Ok(())
    });

    match result {
        Err(Error::TransactionConflict(conflict)) => {
            assert_eq!(conflict.name, b"tickets/current".to_vec());
        }
        Err(err) => panic!("unexpected error: {err}"),
        Ok(_) => panic!("transaction should conflict with the concurrent writer"),
    }

    let loaded = prolly.load_named_root(b"tickets/current").unwrap().unwrap();
    assert_eq!(
        prolly.get(&loaded, b"ticket/123/status").unwrap(),
        Some(b"closed".to_vec())
    );
}

fn assert_manual_commit_reports_applied_counts<S>(prolly: &Prolly<S>)
where
    S: Store + ManifestStore + TransactionalStore,
{
    let tx = prolly.begin_transaction().unwrap();
    let tree = tx.put(&tx.create(), b"k".to_vec(), b"v".to_vec()).unwrap();
    tx.publish_named_root(b"main", &tree).unwrap();

    let update = tx.commit().unwrap();
    match update {
        TransactionUpdate::Applied {
            nodes_written,
            roots_written,
        } => {
            assert!(nodes_written > 0);
            assert_eq!(roots_written, 1);
        }
        TransactionUpdate::Conflict(conflict) => {
            panic!("unexpected transaction conflict: {conflict:?}");
        }
    }
}

fn assert_manual_commit_reports_conflict_without_applying_writes<S>(prolly: &Prolly<S>)
where
    S: Store + ManifestStore + TransactionalStore,
{
    let base = prolly
        .put(&prolly.create(), b"status".to_vec(), b"open".to_vec())
        .unwrap();
    prolly.publish_named_root(b"main", &base).unwrap();

    let tx = prolly.begin_transaction().unwrap();
    let current = tx.load_named_root(b"main").unwrap().unwrap();
    let staged = tx
        .put(&current, b"status".to_vec(), b"in-progress".to_vec())
        .unwrap();
    tx.publish_named_root(b"main", &staged).unwrap();

    let outside = prolly
        .put(&current, b"status".to_vec(), b"closed".to_vec())
        .unwrap();
    let update = prolly
        .compare_and_swap_named_root(b"main", Some(&current), Some(&outside))
        .unwrap();
    assert!(update.is_applied());

    let commit = tx.commit().unwrap();
    assert!(commit.is_conflict());

    let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
    assert_eq!(
        prolly.get(&loaded, b"status").unwrap(),
        Some(b"closed".to_vec())
    );
}

fn assert_owned_transaction_commits<S>(prolly: &Prolly<S>)
where
    S: Store + ManifestStore + TransactionalStore + Clone,
{
    let tx = prolly.begin_owned_transaction().unwrap();
    let tree = tx
        .put(&tx.create(), b"owned".to_vec(), b"transaction".to_vec())
        .unwrap();
    tx.publish_named_root(b"owned/current", &tree).unwrap();
    assert!(tx.commit().unwrap().is_applied());

    let loaded = prolly.load_named_root(b"owned/current").unwrap().unwrap();
    assert_eq!(
        prolly.get(&loaded, b"owned").unwrap(),
        Some(b"transaction".to_vec())
    );
}

#[test]
fn memstore_supports_strict_transactions() {
    assert_transaction_rolls_back_staged_writes_when_closure_fails(&memory_prolly());
    assert_transaction_commits_multiple_named_roots_atomically(&memory_prolly());
    assert_transaction_reads_its_own_staged_tree_writes(&memory_prolly());
    assert_transaction_detects_concurrent_writer_from_root_read_set(&memory_prolly());
    assert_manual_commit_reports_applied_counts(&memory_prolly());
    assert_manual_commit_reports_conflict_without_applying_writes(&memory_prolly());
    assert_owned_transaction_commits(&memory_prolly());
}

#[test]
fn file_store_supports_strict_root_transactions() {
    with_file_prolly(assert_transaction_rolls_back_staged_writes_when_closure_fails);
    with_file_prolly(assert_transaction_commits_multiple_named_roots_atomically);
    with_file_prolly(assert_transaction_reads_its_own_staged_tree_writes);
    with_file_prolly(assert_transaction_detects_concurrent_writer_from_root_read_set);
    with_file_prolly(assert_manual_commit_reports_applied_counts);
    with_file_prolly(assert_manual_commit_reports_conflict_without_applying_writes);
    with_file_prolly(assert_owned_transaction_commits);
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_store_supports_strict_transactions() {
    assert_transaction_rolls_back_staged_writes_when_closure_fails(&sqlite_prolly());
    assert_transaction_commits_multiple_named_roots_atomically(&sqlite_prolly());
    assert_transaction_reads_its_own_staged_tree_writes(&sqlite_prolly());
    assert_transaction_detects_concurrent_writer_from_root_read_set(&sqlite_prolly());
    assert_manual_commit_reports_applied_counts(&sqlite_prolly());
    assert_manual_commit_reports_conflict_without_applying_writes(&sqlite_prolly());
    assert_owned_transaction_commits(&sqlite_prolly());
}
