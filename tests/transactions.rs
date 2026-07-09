use prolly::{Config, Error, MemStore, Prolly};

#[cfg(feature = "sqlite")]
use {
    prolly::{NamedRootUpdate, NodeStoreScan, SqliteStore, TransactionUpdate},
    std::sync::Arc,
};

#[test]
fn unsupported_store_reports_clear_transaction_error() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let result = prolly.begin_transaction();

    match result {
        Err(Error::UnsupportedTransactions { store }) => {
            assert!(store.contains("MemStore"));
        }
        Err(err) => panic!("unexpected error: {err}"),
        Ok(_) => panic!("MemStore should not advertise strict transactions"),
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_prolly() -> Prolly<Arc<SqliteStore>> {
    Prolly::new(
        Arc::new(SqliteStore::open_in_memory().unwrap()),
        Config::default(),
    )
}

#[cfg(feature = "sqlite")]
#[test]
fn transaction_rolls_back_staged_writes_when_closure_fails() {
    let prolly = sqlite_prolly();

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

#[cfg(feature = "sqlite")]
#[test]
fn transaction_rolls_back_staged_cas_update_when_commit_conflicts() {
    let prolly = sqlite_prolly();
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

    let tx = prolly.begin_transaction().unwrap();
    let current = tx.load_named_root(b"tickets/current").unwrap().unwrap();
    let tx_tree = tx
        .put(
            &current,
            b"ticket/123/status".to_vec(),
            b"in-progress".to_vec(),
        )
        .unwrap();
    let staged = tx
        .compare_and_swap_named_root(b"tickets/current", Some(&current), Some(&tx_tree))
        .unwrap();
    assert!(staged.is_applied());

    let concurrent = prolly
        .put(&base, b"ticket/123/status".to_vec(), b"closed".to_vec())
        .unwrap();
    let update = prolly
        .compare_and_swap_named_root(b"tickets/current", Some(&base), Some(&concurrent))
        .unwrap();
    assert!(update.is_applied());

    let commit = tx.commit().unwrap();
    assert!(commit.is_conflict());

    let loaded = prolly.load_named_root(b"tickets/current").unwrap().unwrap();
    assert_eq!(
        prolly.get(&loaded, b"ticket/123/status").unwrap(),
        Some(b"closed".to_vec())
    );
    assert_eq!(
        prolly.get(&loaded, b"ticket/123/status").unwrap(),
        prolly.get(&concurrent, b"ticket/123/status").unwrap()
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn transaction_commits_multiple_named_roots_atomically() {
    let prolly = sqlite_prolly();

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

#[cfg(feature = "sqlite")]
#[test]
fn transaction_reads_its_own_staged_tree_writes() {
    let prolly = sqlite_prolly();

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

#[cfg(feature = "sqlite")]
#[test]
fn transaction_detects_concurrent_writer_from_root_read_set() {
    let prolly = sqlite_prolly();
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

#[cfg(feature = "sqlite")]
#[test]
fn manual_commit_reports_applied_counts() {
    let prolly = sqlite_prolly();
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
