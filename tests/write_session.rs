use prolly::{Config, Error, MemStore, Mutation, Prolly};
use std::ops::ControlFlow;
use std::sync::Arc;

fn base() -> (Prolly<Arc<MemStore>>, prolly::Tree) {
    let manager = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let tree = manager
        .batch(
            &manager.create(),
            vec![
                Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"1".to_vec(),
                },
                Mutation::Upsert {
                    key: b"c".to_vec(),
                    val: b"3".to_vec(),
                },
            ],
        )
        .unwrap();
    (manager, tree)
}

#[test]
fn overlay_reads_ranges_and_flush_match_direct_batch() {
    let (manager, tree) = base();
    let mut session = manager.write_session(tree.clone(), 1024);
    session.put(b"b".to_vec(), b"2".to_vec()).unwrap();
    session.put(b"c".to_vec(), b"changed".to_vec()).unwrap();
    session.delete(b"a".to_vec()).unwrap();

    assert_eq!(session.get(b"a").unwrap(), None);
    assert_eq!(session.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(
        session.get_with(b"b", |value| value[0]).unwrap(),
        Some(b'2')
    );
    assert_eq!(
        session.range(b"a", Some(b"d")).unwrap(),
        vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"changed".to_vec()),
        ]
    );
    let mut borrowed = Vec::new();
    let outcome = session
        .scan_range_until(b"a", Some(b"d"), |entry| {
            borrowed.push(entry.to_owned());
            if borrowed.len() == 2 {
                ControlFlow::Break(entry.key().to_vec())
            } else {
                ControlFlow::Continue(())
            }
        })
        .unwrap();
    assert_eq!(outcome.visited, 2);
    assert_eq!(outcome.break_value, Some(b"c".to_vec()));
    assert_eq!(borrowed, session.range(b"a", Some(b"d")).unwrap());

    let direct = manager
        .batch(
            &tree,
            vec![
                Mutation::Delete { key: b"a".to_vec() },
                Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"2".to_vec(),
                },
                Mutation::Upsert {
                    key: b"c".to_vec(),
                    val: b"changed".to_vec(),
                },
            ],
        )
        .unwrap();
    let flushed = session.flush().unwrap();
    assert_eq!(flushed.root, direct.root);
    assert!(session.is_empty());
}

#[test]
fn savepoint_revert_restores_overlay_and_stales_after_flush() {
    let (manager, tree) = base();
    let mut session = manager.write_session(tree, 1024);
    session.put(b"b".to_vec(), b"2".to_vec()).unwrap();
    let savepoint = session.savepoint();
    session.put(b"b".to_vec(), b"changed".to_vec()).unwrap();
    session.delete(b"c".to_vec()).unwrap();
    session.revert(savepoint).unwrap();
    assert_eq!(session.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(session.get(b"c").unwrap(), Some(b"3".to_vec()));

    session.flush().unwrap();
    assert!(matches!(
        session.revert(savepoint),
        Err(Error::InvalidSavepoint)
    ));
}

#[test]
fn byte_budget_is_checked_before_mutation() {
    let (manager, tree) = base();
    let mut session = manager.write_session(tree, 4);
    session.put(b"a".to_vec(), b"123".to_vec()).unwrap();
    assert!(matches!(
        session.put(b"b".to_vec(), b"x".to_vec()),
        Err(Error::BufferFull)
    ));
    assert_eq!(session.get(b"b").unwrap(), None);
}
