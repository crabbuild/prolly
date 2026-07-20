use prolly::{chunking, AsyncProlly, Config, MemStore, Mutation, Prolly, SyncStoreAsAsync};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn tree() -> (Prolly<Arc<MemStore>>, prolly::Tree) {
    let mut policy = chunking::entry_count_key_hash();
    policy.min = 2;
    policy.target = 4;
    policy.max = 8;
    policy.rule = prolly::BoundaryRule::HashThreshold { factor: 4 };
    let manager = Prolly::new(
        Arc::new(MemStore::new()),
        Config::builder().chunking(policy).build(),
    );
    let tree = manager
        .batch(
            &manager.create(),
            (0..100)
                .map(|index| Mutation::Upsert {
                    key: format!("k{index:03}").into_bytes(),
                    val: format!("v{index:03}").into_bytes(),
                })
                .collect(),
        )
        .unwrap();
    (manager, tree)
}

#[test]
fn len_rank_and_select_use_persisted_subtree_counts() {
    let (manager, tree) = tree();

    assert_eq!(manager.len(&tree).unwrap(), 100);
    assert_eq!(manager.rank(&tree, b"k000").unwrap(), 0);
    assert_eq!(manager.rank(&tree, b"k050").unwrap(), 50);
    assert_eq!(manager.rank(&tree, b"k050x").unwrap(), 51);
    assert_eq!(manager.rank(&tree, b"z").unwrap(), 100);
    assert_eq!(
        manager.select(&tree, 50).unwrap(),
        Some((b"k050".to_vec(), b"v050".to_vec()))
    );
    assert_eq!(manager.select(&tree, 100).unwrap(), None);
}

#[test]
fn cardinalities_follow_canonical_updates() {
    let (manager, tree) = tree();
    let deleted = manager.delete(&tree, b"k050").unwrap();
    assert_eq!(manager.len(&deleted).unwrap(), 99);
    assert_eq!(manager.rank(&deleted, b"k051").unwrap(), 50);
    assert_eq!(
        manager.select(&deleted, 50).unwrap(),
        Some((b"k051".to_vec(), b"v051".to_vec()))
    );
}

#[test]
fn native_async_cardinality_and_bounds_match_the_sync_facade() {
    let (sync, tree) = tree();
    let asynchronous = AsyncProlly::new(
        SyncStoreAsAsync::new(sync.store().clone()),
        sync.config().clone(),
    );

    block_on(async {
        assert_eq!(asynchronous.len(&tree).await.unwrap(), 100);
        assert_eq!(asynchronous.rank(&tree, b"k050x").await.unwrap(), 51);
        assert_eq!(
            asynchronous.select(&tree, 50).await.unwrap(),
            Some((b"k050".to_vec(), b"v050".to_vec()))
        );
        assert_eq!(
            asynchronous.lower_bound(&tree, b"k050x").await.unwrap(),
            Some((b"k051".to_vec(), b"v051".to_vec()))
        );
        assert_eq!(
            asynchronous.upper_bound(&tree, b"k050").await.unwrap(),
            Some((b"k051".to_vec(), b"v051".to_vec()))
        );
        assert_eq!(
            asynchronous.first_entry(&tree).await.unwrap(),
            Some((b"k000".to_vec(), b"v000".to_vec()))
        );
        assert_eq!(
            asynchronous.last_entry(&tree).await.unwrap(),
            Some((b"k099".to_vec(), b"v099".to_vec()))
        );

        let mut session = asynchronous.read(&tree).await.unwrap();
        assert_eq!(session.len().await.unwrap(), 100);
        assert_eq!(session.rank(b"k075").await.unwrap(), 75);
        assert_eq!(
            session
                .select_with(75, |entry| entry.to_owned())
                .await
                .unwrap(),
            Some((b"k075".to_vec(), b"v075".to_vec()))
        );
    });
}
