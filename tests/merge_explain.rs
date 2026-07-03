mod common;

use common::configured_prolly;
use prolly::{
    Error, MergeFallbackReason, MergeFastPath, MergeResolutionKind, MergeTraceEvent,
    MergeTraceStage, Resolution,
};

#[test]
fn merge_explain_reports_root_fast_path() {
    let prolly = configured_prolly();

    let base = prolly
        .put(&prolly.create(), b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let explanation = prolly.merge_explain(&base, &base, &right, None);
    let merged = explanation.result.unwrap();

    assert_eq!(merged.root, right.root);
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::FastPath {
            reason: MergeFastPath::LeftUnchanged
        }
    )));
}

#[test]
fn merge_explain_reports_structural_rewrite() {
    let prolly = configured_prolly();

    let mut base = prolly.create();
    base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
    base = prolly.put(&base, b"b".to_vec(), b"1".to_vec()).unwrap();
    base = prolly.put(&base, b"c".to_vec(), b"1".to_vec()).unwrap();

    let left = prolly.put(&base, b"a".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"b".to_vec(), b"right".to_vec()).unwrap();

    let explanation = prolly.merge_explain(&base, &left, &right, None);
    let merged = explanation.result.unwrap();

    assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"left".to_vec()));
    assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"right".to_vec()));
    assert!(explanation
        .trace
        .events
        .iter()
        .any(|event| matches!(event, MergeTraceEvent::StructuralMergeStarted)));
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::RewrittenNode {
            level: 0,
            entries: 3,
            first_key,
            last_key,
            ..
        } if first_key.as_deref() == Some(b"a".as_slice())
            && last_key.as_deref() == Some(b"c".as_slice())
    )));
}

#[test]
fn merge_explain_keeps_trace_when_resolver_leaves_conflict_unresolved() {
    let prolly = configured_prolly();

    let base = prolly
        .put(&prolly.create(), b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let explanation = prolly.merge_explain(
        &base,
        &left,
        &right,
        Some(Box::new(|_| Resolution::unresolved())),
    );

    assert!(matches!(&explanation.result, Err(Error::Conflict(_))));
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::ResolverCalled {
            stage: MergeTraceStage::Structural,
            key,
            resolution: MergeResolutionKind::Unresolved,
        } if key == b"k"
    )));
}

#[test]
fn merge_explain_resolves_safe_leaf_delete_structurally() {
    let prolly = configured_prolly();

    let mut base = prolly.create();
    base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
    base = prolly.put(&base, b"b".to_vec(), b"base".to_vec()).unwrap();
    base = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();

    let left = prolly.put(&base, b"b".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"b".to_vec(), b"right".to_vec()).unwrap();

    let explanation = prolly.merge_explain(
        &base,
        &left,
        &right,
        Some(Box::new(|_| Resolution::delete())),
    );
    let merged = explanation.result.unwrap();

    assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(prolly.get(&merged, b"b").unwrap(), None);
    assert_eq!(prolly.get(&merged, b"c").unwrap(), Some(b"3".to_vec()));
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::ResolverCalled {
            stage: MergeTraceStage::Structural,
            key,
            resolution: MergeResolutionKind::Delete,
        } if key == b"b"
    )));
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::RewrittenNode {
            level: 0,
            entries: 2,
            first_key,
            last_key,
            ..
        } if first_key.as_deref() == Some(b"a".as_slice())
            && last_key.as_deref() == Some(b"c".as_slice())
    )));
    assert!(!explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::Fallback {
            reason: MergeFallbackReason::DeleteResolution | MergeFallbackReason::DiffBatch
        } | MergeTraceEvent::BatchMerge { .. }
    )));
}

#[test]
fn merge_explain_reports_delete_resolution_fallback_to_batch_path() {
    let prolly = configured_prolly();

    let base = prolly
        .put(&prolly.create(), b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let explanation = prolly.merge_explain(
        &base,
        &left,
        &right,
        Some(Box::new(|_| Resolution::delete())),
    );
    let merged = match &explanation.result {
        Ok(tree) => tree.clone(),
        Err(err) => panic!("expected successful delete resolution, got {err:?}"),
    };

    assert_eq!(prolly.get(&merged, b"k").unwrap(), None);
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::ResolverCalled {
            stage: MergeTraceStage::Structural,
            key,
            resolution: MergeResolutionKind::Delete,
        } if key == b"k"
    )));
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::Fallback {
            reason: MergeFallbackReason::DeleteResolution
        }
    )));
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::ResolverCalled {
            stage: MergeTraceStage::Batch,
            key,
            resolution: MergeResolutionKind::Delete,
        } if key == b"k"
    )));
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::BatchMerge {
            right_changes: 1,
            mutations: 1,
            append_only: false,
        }
    )));
    assert!(explanation.trace.events.iter().any(|event| matches!(
        event,
        MergeTraceEvent::DiffTraversal { stats }
            if stats.compared_nodes >= 1 && stats.emitted_diffs == 1
    )));
}

#[cfg(feature = "async-store")]
mod async_tests {
    use super::*;
    use prolly::{AsyncProlly, Config, MemStore, Prolly, SyncStoreAsAsync};
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

    fn async_pair() -> (
        Prolly<Arc<MemStore>>,
        AsyncProlly<SyncStoreAsAsync<Arc<MemStore>>>,
    ) {
        let store = Arc::new(MemStore::new());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(2)
            .build();
        (
            Prolly::new(store.clone(), config.clone()),
            AsyncProlly::new(SyncStoreAsAsync::new(store), config),
        )
    }

    #[test]
    fn async_merge_explain_reports_root_fast_path() {
        let (sync_prolly, async_prolly) = async_pair();

        let base = sync_prolly
            .put(&sync_prolly.create(), b"k".to_vec(), b"base".to_vec())
            .unwrap();
        let right = sync_prolly
            .put(&base, b"k".to_vec(), b"right".to_vec())
            .unwrap();

        let explanation = block_on(async_prolly.merge_explain(&base, &base, &right, None));
        let merged = explanation.result.unwrap();

        assert_eq!(merged.root, right.root);
        assert!(explanation.trace.events.iter().any(|event| matches!(
            event,
            MergeTraceEvent::FastPath {
                reason: MergeFastPath::LeftUnchanged
            }
        )));
    }

    #[test]
    fn async_merge_explain_reports_batch_merge() {
        let (sync_prolly, async_prolly) = async_pair();

        let base = sync_prolly
            .put(&sync_prolly.create(), b"a".to_vec(), b"1".to_vec())
            .unwrap();
        let left = sync_prolly
            .put(&base, b"b".to_vec(), b"2".to_vec())
            .unwrap();
        let right = sync_prolly
            .put(&base, b"c".to_vec(), b"3".to_vec())
            .unwrap();

        let explanation = block_on(async_prolly.merge_explain(&base, &left, &right, None));
        let merged = explanation.result.unwrap();

        assert_eq!(
            block_on(async_prolly.get(&merged, b"c")).unwrap(),
            Some(b"3".to_vec())
        );
        assert!(explanation.trace.events.iter().any(|event| matches!(
            event,
            MergeTraceEvent::Fallback {
                reason: MergeFallbackReason::DiffBatch
            }
        )));
        assert!(explanation.trace.events.iter().any(|event| matches!(
            event,
            MergeTraceEvent::BatchMerge {
                right_changes: 1,
                mutations: 1,
                append_only: false,
            }
        )));
        assert!(explanation.trace.events.iter().any(|event| matches!(
            event,
            MergeTraceEvent::DiffTraversal { stats }
                if stats.compared_nodes >= 1 && stats.emitted_diffs == 1
        )));
    }

    #[test]
    fn async_merge_explain_keeps_trace_when_resolver_leaves_conflict_unresolved() {
        let (sync_prolly, async_prolly) = async_pair();

        let base = sync_prolly
            .put(&sync_prolly.create(), b"k".to_vec(), b"base".to_vec())
            .unwrap();
        let left = sync_prolly
            .put(&base, b"k".to_vec(), b"left".to_vec())
            .unwrap();
        let right = sync_prolly
            .put(&base, b"k".to_vec(), b"right".to_vec())
            .unwrap();

        let explanation = block_on(async_prolly.merge_explain(
            &base,
            &left,
            &right,
            Some(Box::new(|_| Resolution::unresolved())),
        ));

        assert!(matches!(&explanation.result, Err(Error::Conflict(_))));
        assert!(explanation.trace.events.iter().any(|event| matches!(
            event,
            MergeTraceEvent::ResolverCalled {
                stage: MergeTraceStage::Batch,
                key,
                resolution: MergeResolutionKind::Unresolved,
            } if key == b"k"
        )));
    }
}
