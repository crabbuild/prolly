use prolly::{Config, Diff, Error, MemStore, Prolly};

fn small_node_config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(231)
        .build()
}

fn leaf_config() -> Config {
    Config::builder()
        .min_chunk_size(32)
        .max_chunk_size(64)
        .chunking_factor(64)
        .hash_seed(231)
        .build()
}

fn build_versions<S>(prolly: &Prolly<S>) -> (prolly::Tree, prolly::Tree)
where
    S: prolly::Store,
{
    let mut base = prolly.create();
    for idx in 0..40 {
        base = prolly
            .put(
                &base,
                format!("k{idx:03}").into_bytes(),
                format!("base-{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = base.clone();
    for idx in [2, 4, 7, 15, 21, 32] {
        other = prolly
            .put(
                &other,
                format!("k{idx:03}").into_bytes(),
                format!("changed-{idx:03}").into_bytes(),
            )
            .unwrap();
    }
    for idx in [11, 17, 29] {
        other = prolly
            .delete(&other, format!("k{idx:03}").as_bytes())
            .unwrap();
    }
    for idx in [41, 42, 43] {
        other = prolly
            .put(
                &other,
                format!("k{idx:03}").into_bytes(),
                format!("added-{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    (base, other)
}

fn collect_stream_diff<S>(
    prolly: &Prolly<S>,
    base: &prolly::Tree,
    other: &prolly::Tree,
) -> Vec<Diff>
where
    S: prolly::Store,
{
    prolly
        .stream_diff(base, other)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn structural_diff_pages_resume_to_reconstruct_stream_diff() {
    let prolly = Prolly::new(MemStore::new(), small_node_config());
    let (base, other) = build_versions(&prolly);
    let expected = collect_stream_diff(&prolly, &base, &other);

    let mut cursor = None;
    let mut actual = Vec::new();
    let mut page_count = 0usize;

    loop {
        let page = prolly
            .structural_diff_page(&base, &other, cursor.as_ref(), 3)
            .unwrap();
        assert!(page.diffs.len() <= 3);
        assert_eq!(page.stats.emitted_diffs, page.diffs.len());
        page_count += 1;
        actual.extend(page.diffs);

        let Some(next) = page.next_cursor else {
            break;
        };
        assert_eq!(next.base_root, base.root);
        assert_eq!(next.other_root, other.root);
        assert!(!next.is_empty());
        cursor = Some(next);
    }

    assert_eq!(actual, expected);
    assert!(page_count > 1);
}

#[test]
fn zero_limit_structural_diff_page_returns_start_checkpoint() {
    let prolly = Prolly::new(MemStore::new(), small_node_config());
    let (base, other) = build_versions(&prolly);

    let page = prolly.structural_diff_page(&base, &other, None, 0).unwrap();

    assert!(page.diffs.is_empty());
    assert_eq!(page.stats.emitted_diffs, 0);
    let cursor = page.next_cursor.unwrap();
    assert_eq!(cursor.base_root, base.root);
    assert_eq!(cursor.other_root, other.root);
    assert!(!cursor.is_empty());
}

#[test]
fn structural_diff_cursor_rejects_different_roots() {
    let prolly = Prolly::new(MemStore::new(), small_node_config());
    let (base, other) = build_versions(&prolly);
    let page = prolly.structural_diff_page(&base, &other, None, 1).unwrap();
    let cursor = page.next_cursor.unwrap();
    let different_other = prolly
        .put(&other, b"k999".to_vec(), b"different".to_vec())
        .unwrap();

    let err = prolly
        .structural_diff_page(&base, &different_other, Some(&cursor), 1)
        .unwrap_err();

    assert!(matches!(err, Error::InvalidNode));
}

#[test]
fn structural_diff_cursor_preserves_pending_leaf_diffs() {
    let prolly = Prolly::new(MemStore::new(), leaf_config());
    let mut base = prolly.create();
    for idx in 0..6 {
        base = prolly
            .put(
                &base,
                format!("k{idx:03}").into_bytes(),
                format!("base-{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = base.clone();
    for idx in 0..4 {
        other = prolly
            .put(
                &other,
                format!("k{idx:03}").into_bytes(),
                format!("changed-{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let first = prolly.structural_diff_page(&base, &other, None, 1).unwrap();
    assert_eq!(first.diffs.len(), 1);
    let cursor = first.next_cursor.unwrap();
    assert!(
        !cursor.pending.is_empty(),
        "leaf compare should checkpoint already-expanded pending diffs"
    );

    let second = prolly
        .structural_diff_page(&base, &other, Some(&cursor), 16)
        .unwrap();
    let mut actual = first.diffs;
    actual.extend(second.diffs);

    assert_eq!(actual, collect_stream_diff(&prolly, &base, &other));
    assert!(second.next_cursor.is_none());
}

#[cfg(feature = "async-store")]
mod async_tests {
    use super::*;
    use prolly::{AsyncProlly, SyncStoreAsAsync};
    use std::{
        future::Future,
        sync::Arc,
        task::{Context, Poll},
    };

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn async_structural_diff_pages_match_sync_stream_diff() {
        let store = Arc::new(MemStore::new());
        let config = small_node_config();
        let sync_prolly = Prolly::new(store.clone(), config.clone());
        let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
        let (base, other) = build_versions(&sync_prolly);
        let expected = collect_stream_diff(&sync_prolly, &base, &other);

        let mut cursor = None;
        let mut actual = Vec::new();

        loop {
            let page =
                block_on(async_prolly.structural_diff_page(&base, &other, cursor.as_ref(), 3))
                    .unwrap();
            assert!(page.diffs.len() <= 3);
            assert_eq!(page.stats.emitted_diffs, page.diffs.len());
            actual.extend(page.diffs);

            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }

        assert_eq!(actual, expected);
    }
}
