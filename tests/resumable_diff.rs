use prolly::{Config, Diff, MemStore, Prolly, RangeCursor};

fn small_node_config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(188)
        .build()
}

fn build_versions<S>(prolly: &Prolly<S>) -> (prolly::Tree, prolly::Tree)
where
    S: prolly::Store,
{
    let mut base = prolly.create();
    for idx in 0..20 {
        base = prolly
            .put(
                &base,
                format!("k{idx:03}").into_bytes(),
                format!("base-{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut other = base.clone();
    other = prolly.delete(&other, b"k002").unwrap();
    other = prolly
        .put(&other, b"k004".to_vec(), b"changed-004".to_vec())
        .unwrap();
    other = prolly
        .put(&other, b"k007".to_vec(), b"changed-007".to_vec())
        .unwrap();
    other = prolly.delete(&other, b"k012").unwrap();
    other = prolly
        .put(&other, b"k015".to_vec(), b"changed-015".to_vec())
        .unwrap();
    other = prolly
        .put(&other, b"k021".to_vec(), b"added-021".to_vec())
        .unwrap();

    (base, other)
}

fn diff_keys(diffs: &[Diff]) -> Vec<Vec<u8>> {
    diffs.iter().map(|diff| diff.key().to_vec()).collect()
}

#[test]
fn diff_from_cursor_resumes_strictly_after_key() {
    let prolly = Prolly::new(MemStore::new(), small_node_config());
    let (base, other) = build_versions(&prolly);

    let diffs = prolly
        .diff_from_cursor(
            &base,
            &other,
            &RangeCursor::after_key(b"k004".to_vec()),
            Some(b"k016"),
        )
        .unwrap();

    assert_eq!(
        diff_keys(&diffs),
        vec![b"k007".to_vec(), b"k012".to_vec(), b"k015".to_vec()]
    );
}

#[test]
fn diff_pages_resume_to_reconstruct_bounded_diff() {
    let prolly = Prolly::new(MemStore::new(), small_node_config());
    let (base, other) = build_versions(&prolly);

    let expected = prolly
        .diff_from_cursor(
            &base,
            &other,
            &RangeCursor::after_key(b"k003".to_vec()),
            Some(b"k022"),
        )
        .unwrap();

    let mut cursor = RangeCursor::after_key(b"k003".to_vec());
    let mut actual = Vec::new();
    let mut page_count = 0usize;

    loop {
        let page = prolly
            .diff_page(&base, &other, &cursor, Some(b"k022"), 2)
            .unwrap();
        page_count += 1;
        actual.extend(page.diffs);

        let Some(next) = page.next_cursor else {
            break;
        };
        assert!(!next.is_start());
        cursor = next;
    }

    assert_eq!(actual, expected);
    assert!(page_count > 1);
}

#[test]
fn zero_limit_diff_page_is_noop() {
    let prolly = Prolly::new(MemStore::new(), small_node_config());
    let (base, other) = build_versions(&prolly);
    let cursor = RangeCursor::after_key(b"k004".to_vec());

    let page = prolly.diff_page(&base, &other, &cursor, None, 0).unwrap();

    assert!(page.diffs.is_empty());
    assert_eq!(page.next_cursor, Some(cursor));
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
    fn async_diff_pages_match_sync_diff_pages() {
        let store = Arc::new(MemStore::new());
        let config = small_node_config();
        let sync_prolly = Prolly::new(store.clone(), config.clone());
        let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);
        let (base, other) = build_versions(&sync_prolly);

        let expected = sync_prolly
            .diff_from_cursor(
                &base,
                &other,
                &RangeCursor::after_key(b"k003".to_vec()),
                Some(b"k022"),
            )
            .unwrap();

        let mut cursor = RangeCursor::after_key(b"k003".to_vec());
        let mut actual = Vec::new();

        loop {
            let page =
                block_on(async_prolly.diff_page(&base, &other, &cursor, Some(b"k022"), 2)).unwrap();
            actual.extend(page.diffs);

            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = next;
        }

        assert_eq!(actual, expected);
    }
}
