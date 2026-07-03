package prolly

import (
	"context"
	"errors"
)

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func contextValue[T any](ctx context.Context, run func() (T, error)) (T, error) {
	var zero T
	if err := checkContext(ctx); err != nil {
		return zero, err
	}
	value, err := run()
	if err != nil {
		return zero, err
	}
	if err := checkContext(ctx); err != nil {
		return zero, err
	}
	return value, nil
}

func contextValues[A, B any](ctx context.Context, run func() (A, B, error)) (A, B, error) {
	var zeroA A
	var zeroB B
	if err := checkContext(ctx); err != nil {
		return zeroA, zeroB, err
	}
	a, b, err := run()
	if err != nil {
		return zeroA, zeroB, err
	}
	if err := checkContext(ctx); err != nil {
		return zeroA, zeroB, err
	}
	return a, b, nil
}

func contextVoid(ctx context.Context, run func() error) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := run(); err != nil {
		return err
	}
	return checkContext(ctx)
}

func (s *BlobStore) PutBlobContext(ctx context.Context, data []byte) (BlobRef, error) {
	return contextValue(ctx, func() (BlobRef, error) {
		return s.PutBlob(data)
	})
}

func (s *BlobStore) GetBlobContext(ctx context.Context, ref BlobRef) ([]byte, bool, error) {
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	value, found, err := s.GetBlob(ref)
	if err != nil {
		return nil, false, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	return value, found, nil
}

func (s *BlobStore) DeleteBlobContext(ctx context.Context, ref BlobRef) error {
	return contextVoid(ctx, func() error {
		return s.DeleteBlob(ref)
	})
}

func (s *BlobStore) ListBlobRefsContext(ctx context.Context) ([]BlobRef, error) {
	return contextValue(ctx, s.ListBlobRefs)
}

func (s *BlobStore) BlobCountContext(ctx context.Context) (uint64, error) {
	return contextValue(ctx, s.BlobCount)
}

func (e *Engine) CreateContext(ctx context.Context) (Tree, error) {
	return contextValue(ctx, e.Create)
}

func (e *Engine) PutContext(ctx context.Context, tree Tree, key []byte, value []byte) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.Put(tree, key, value)
	})
}

func (e *Engine) DeleteContext(ctx context.Context, tree Tree, key []byte) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.Delete(tree, key)
	})
}

func (e *Engine) GetContext(ctx context.Context, tree Tree, key []byte) ([]byte, bool, error) {
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	value, found, err := e.Get(tree, key)
	if err != nil {
		return nil, false, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	return value, found, nil
}

func (e *Engine) GetValueRefContext(ctx context.Context, tree Tree, key []byte) (*ValueRef, error) {
	return contextValue(ctx, func() (*ValueRef, error) {
		return e.GetValueRef(tree, key)
	})
}

func (e *Engine) GetLargeValueContext(ctx context.Context, blobStore *BlobStore, tree Tree, key []byte) ([]byte, bool, error) {
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	value, found, err := e.GetLargeValue(blobStore, tree, key)
	if err != nil {
		return nil, false, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, false, err
	}
	return value, found, nil
}

func (e *Engine) PutLargeValueContext(ctx context.Context, blobStore *BlobStore, tree Tree, key []byte, value []byte, config LargeValueConfig) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.PutLargeValue(blobStore, tree, key, value, config)
	})
}

func (e *Engine) GetManyContext(ctx context.Context, tree Tree, keys [][]byte) ([][]byte, []bool, error) {
	return contextValues(ctx, func() ([][]byte, []bool, error) {
		return e.GetMany(tree, keys)
	})
}

func (e *Engine) ProveKeyContext(ctx context.Context, tree Tree, key []byte) (KeyProof, error) {
	return contextValue(ctx, func() (KeyProof, error) {
		return e.ProveKey(tree, key)
	})
}

func (e *Engine) ProveKeysContext(ctx context.Context, tree Tree, keys [][]byte) (MultiKeyProof, error) {
	return contextValue(ctx, func() (MultiKeyProof, error) {
		return e.ProveKeys(tree, keys)
	})
}

func (e *Engine) ProveRangeContext(ctx context.Context, tree Tree, start []byte, end []byte) (RangeProof, error) {
	return contextValue(ctx, func() (RangeProof, error) {
		return e.ProveRange(tree, start, end)
	})
}

func (e *Engine) ProvePrefixContext(ctx context.Context, tree Tree, prefix []byte) (RangeProof, error) {
	return contextValue(ctx, func() (RangeProof, error) {
		return e.ProvePrefix(tree, prefix)
	})
}

func (e *Engine) ProveRangePageContext(ctx context.Context, tree Tree, cursor *RangeCursor, end []byte, limit uint64) (ProvedRangePage, error) {
	return contextValue(ctx, func() (ProvedRangePage, error) {
		return e.ProveRangePage(tree, cursor, end, limit)
	})
}

func (e *Engine) BatchContext(ctx context.Context, tree Tree, mutations []Mutation) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.Batch(tree, mutations)
	})
}

func (e *Engine) BatchWithStatsContext(ctx context.Context, tree Tree, mutations []Mutation) (BatchApplyResult, error) {
	return contextValue(ctx, func() (BatchApplyResult, error) {
		return e.BatchWithStats(tree, mutations)
	})
}

func (e *Engine) BuildFromEntriesContext(ctx context.Context, entries []Entry) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.BuildFromEntries(entries)
	})
}

func (e *Engine) BuildFromSortedEntriesContext(ctx context.Context, entries []Entry) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.BuildFromSortedEntries(entries)
	})
}

func (e *Engine) AppendBatchContext(ctx context.Context, tree Tree, mutations []Mutation) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.AppendBatch(tree, mutations)
	})
}

func (e *Engine) AppendBatchWithStatsContext(ctx context.Context, tree Tree, mutations []Mutation) (BatchApplyResult, error) {
	return contextValue(ctx, func() (BatchApplyResult, error) {
		return e.AppendBatchWithStats(tree, mutations)
	})
}

func (e *Engine) ParallelBatchContext(ctx context.Context, tree Tree, mutations []Mutation, config ParallelConfig) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.ParallelBatch(tree, mutations, config)
	})
}

func (e *Engine) ParallelBatchWithStatsContext(ctx context.Context, tree Tree, mutations []Mutation, config ParallelConfig) (BatchApplyResult, error) {
	return contextValue(ctx, func() (BatchApplyResult, error) {
		return e.ParallelBatchWithStats(tree, mutations, config)
	})
}

func (e *Engine) FirstEntryContext(ctx context.Context, tree Tree) (*Entry, error) {
	return contextValue(ctx, func() (*Entry, error) {
		return e.FirstEntry(tree)
	})
}

func (e *Engine) LastEntryContext(ctx context.Context, tree Tree) (*Entry, error) {
	return contextValue(ctx, func() (*Entry, error) {
		return e.LastEntry(tree)
	})
}

func (e *Engine) LowerBoundContext(ctx context.Context, tree Tree, key []byte) (*Entry, error) {
	return contextValue(ctx, func() (*Entry, error) {
		return e.LowerBound(tree, key)
	})
}

func (e *Engine) UpperBoundContext(ctx context.Context, tree Tree, key []byte) (*Entry, error) {
	return contextValue(ctx, func() (*Entry, error) {
		return e.UpperBound(tree, key)
	})
}

func (e *Engine) RangeContext(ctx context.Context, tree Tree, start []byte, end []byte) ([]Entry, error) {
	return contextValue(ctx, func() ([]Entry, error) {
		return e.Range(tree, start, end)
	})
}

func (e *Engine) PrefixContext(ctx context.Context, tree Tree, prefix []byte) ([]Entry, error) {
	return contextValue(ctx, func() ([]Entry, error) {
		return e.Prefix(tree, prefix)
	})
}

func (e *Engine) PrefixPageContext(ctx context.Context, tree Tree, prefix []byte, cursor *RangeCursor, limit uint64) (RangePage, error) {
	return contextValue(ctx, func() (RangePage, error) {
		return e.PrefixPage(tree, prefix, cursor, limit)
	})
}

func (e *Engine) PrefixReversePageContext(ctx context.Context, tree Tree, prefix []byte, cursor *ReverseCursor, limit uint64) (ReversePage, error) {
	return contextValue(ctx, func() (ReversePage, error) {
		return e.PrefixReversePage(tree, prefix, cursor, limit)
	})
}

func (e *Engine) RangeAfterContext(ctx context.Context, tree Tree, afterKey []byte, end []byte) ([]Entry, error) {
	return contextValue(ctx, func() ([]Entry, error) {
		return e.RangeAfter(tree, afterKey, end)
	})
}

func (e *Engine) RangeFromCursorContext(ctx context.Context, tree Tree, cursor *RangeCursor, end []byte) ([]Entry, error) {
	return contextValue(ctx, func() ([]Entry, error) {
		return e.RangeFromCursor(tree, cursor, end)
	})
}

func (e *Engine) RangePageContext(ctx context.Context, tree Tree, cursor *RangeCursor, end []byte, limit uint64) (RangePage, error) {
	return contextValue(ctx, func() (RangePage, error) {
		return e.RangePage(tree, cursor, end, limit)
	})
}

func (e *Engine) ReversePageContext(ctx context.Context, tree Tree, cursor *ReverseCursor, start []byte, limit uint64) (ReversePage, error) {
	return contextValue(ctx, func() (ReversePage, error) {
		return e.ReversePage(tree, cursor, start, limit)
	})
}

func (e *Engine) CursorWindowContext(ctx context.Context, tree Tree, key []byte, end []byte, limit uint64) (CursorWindow, error) {
	return contextValue(ctx, func() (CursorWindow, error) {
		return e.CursorWindow(tree, key, end, limit)
	})
}

func (e *Engine) DiffContext(ctx context.Context, base Tree, other Tree) ([]Diff, error) {
	return contextValue(ctx, func() ([]Diff, error) {
		return e.Diff(base, other)
	})
}

func (e *Engine) RangeDiffContext(ctx context.Context, base Tree, other Tree, start []byte, end []byte) ([]Diff, error) {
	return contextValue(ctx, func() ([]Diff, error) {
		return e.RangeDiff(base, other, start, end)
	})
}

func (e *Engine) DiffFromCursorContext(ctx context.Context, base Tree, other Tree, cursor *RangeCursor, end []byte) ([]Diff, error) {
	return contextValue(ctx, func() ([]Diff, error) {
		return e.DiffFromCursor(base, other, cursor, end)
	})
}

func (e *Engine) DiffPageContext(ctx context.Context, base Tree, other Tree, cursor *RangeCursor, end []byte, limit uint64) (DiffPage, error) {
	return contextValue(ctx, func() (DiffPage, error) {
		return e.DiffPage(base, other, cursor, end, limit)
	})
}

func (e *Engine) ConflictPageContext(ctx context.Context, base Tree, left Tree, right Tree, cursor *RangeCursor, limit uint64) (ConflictPage, error) {
	return contextValue(ctx, func() (ConflictPage, error) {
		return e.ConflictPage(base, left, right, cursor, limit)
	})
}

func (e *Engine) MergeContext(ctx context.Context, base Tree, left Tree, right Tree, resolver string) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.Merge(base, left, right, resolver)
	})
}

func (e *Engine) MergeWithResolverContext(ctx context.Context, base Tree, left Tree, right Tree, resolver Resolver) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.MergeWithResolver(base, left, right, resolver)
	})
}

func (e *Engine) MergeWithPolicyContext(ctx context.Context, base Tree, left Tree, right Tree, policy *MergePolicyRegistry) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.MergeWithPolicy(base, left, right, policy)
	})
}

func (e *Engine) MergeRangeContext(ctx context.Context, base Tree, left Tree, right Tree, start []byte, end []byte, resolver string) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.MergeRange(base, left, right, start, end, resolver)
	})
}

func (e *Engine) MergeRangeWithResolverContext(ctx context.Context, base Tree, left Tree, right Tree, start []byte, end []byte, resolver Resolver) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.MergeRangeWithResolver(base, left, right, start, end, resolver)
	})
}

func (e *Engine) MergeRangeWithPolicyContext(ctx context.Context, base Tree, left Tree, right Tree, start []byte, end []byte, policy *MergePolicyRegistry) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.MergeRangeWithPolicy(base, left, right, start, end, policy)
	})
}

func (e *Engine) MergePrefixContext(ctx context.Context, base Tree, left Tree, right Tree, prefix []byte, resolver string) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.MergePrefix(base, left, right, prefix, resolver)
	})
}

func (e *Engine) MergePrefixWithResolverContext(ctx context.Context, base Tree, left Tree, right Tree, prefix []byte, resolver Resolver) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.MergePrefixWithResolver(base, left, right, prefix, resolver)
	})
}

func (e *Engine) MergePrefixWithPolicyContext(ctx context.Context, base Tree, left Tree, right Tree, prefix []byte, policy *MergePolicyRegistry) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.MergePrefixWithPolicy(base, left, right, prefix, policy)
	})
}

func (e *Engine) MergeExplainContext(ctx context.Context, base Tree, left Tree, right Tree, resolver string) (MergeExplanation, error) {
	return contextValue(ctx, func() (MergeExplanation, error) {
		return e.MergeExplain(base, left, right, resolver)
	})
}

func (e *Engine) MergeExplainWithResolverContext(ctx context.Context, base Tree, left Tree, right Tree, resolver Resolver) (MergeExplanation, error) {
	return contextValue(ctx, func() (MergeExplanation, error) {
		return e.MergeExplainWithResolver(base, left, right, resolver)
	})
}

func (e *Engine) MergeExplainWithPolicyContext(ctx context.Context, base Tree, left Tree, right Tree, policy *MergePolicyRegistry) (MergeExplanation, error) {
	return contextValue(ctx, func() (MergeExplanation, error) {
		return e.MergeExplainWithPolicy(base, left, right, policy)
	})
}

func (e *Engine) CrdtMergeContext(ctx context.Context, base Tree, left Tree, right Tree, config CrdtConfig) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.CrdtMerge(base, left, right, config)
	})
}

func (e *Engine) CrdtMergeWithResolverContext(ctx context.Context, base Tree, left Tree, right Tree, deletePolicy string, resolver CrdtResolver) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.CrdtMergeWithResolver(base, left, right, deletePolicy, resolver)
	})
}

func (e *Engine) PublishNamedRootContext(ctx context.Context, name []byte, tree Tree) error {
	return contextVoid(ctx, func() error {
		return e.PublishNamedRoot(name, tree)
	})
}

func (e *Engine) PublishNamedRootAtMillisContext(ctx context.Context, name []byte, tree Tree, timestampMillis uint64) error {
	return contextVoid(ctx, func() error {
		return e.PublishNamedRootAtMillis(name, tree, timestampMillis)
	})
}

func (e *Engine) LoadNamedRootContext(ctx context.Context, name []byte) (*Tree, error) {
	return contextValue(ctx, func() (*Tree, error) {
		return e.LoadNamedRoot(name)
	})
}

func (e *Engine) LoadNamedRootsContext(ctx context.Context, names [][]byte) (NamedRootSelection, error) {
	return contextValue(ctx, func() (NamedRootSelection, error) {
		return e.LoadNamedRoots(names)
	})
}

func (e *Engine) LoadRetainedNamedRootsContext(ctx context.Context, retention NamedRootRetention) (NamedRootSelection, error) {
	return contextValue(ctx, func() (NamedRootSelection, error) {
		return e.LoadRetainedNamedRoots(retention)
	})
}

func (e *Engine) ListNamedRootsContext(ctx context.Context) ([]NamedRoot, error) {
	return contextValue(ctx, e.ListNamedRoots)
}

func (e *Engine) ListNamedRootManifestsContext(ctx context.Context) ([]NamedRootManifestRecord, error) {
	return contextValue(ctx, e.ListNamedRootManifests)
}

func (e *Engine) CompareAndSwapNamedRootContext(ctx context.Context, name []byte, expected *Tree, replacement *Tree) (NamedRootUpdate, error) {
	return contextValue(ctx, func() (NamedRootUpdate, error) {
		return e.CompareAndSwapNamedRoot(name, expected, replacement)
	})
}

func (e *Engine) CompareAndSwapNamedRootAtMillisContext(ctx context.Context, name []byte, expected *Tree, replacement *Tree, timestampMillis uint64) (NamedRootUpdate, error) {
	return contextValue(ctx, func() (NamedRootUpdate, error) {
		return e.CompareAndSwapNamedRootAtMillis(name, expected, replacement, timestampMillis)
	})
}

func (e *Engine) DeleteNamedRootContext(ctx context.Context, name []byte) error {
	return contextVoid(ctx, func() error {
		return e.DeleteNamedRoot(name)
	})
}

func (e *Engine) PublishSnapshotContext(ctx context.Context, namespace SnapshotNamespace, id []byte, tree Tree) error {
	return contextVoid(ctx, func() error {
		return e.PublishSnapshot(namespace, id, tree)
	})
}

func (e *Engine) PublishSnapshotAtMillisContext(ctx context.Context, namespace SnapshotNamespace, id []byte, tree Tree, timestampMillis uint64) error {
	return contextVoid(ctx, func() error {
		return e.PublishSnapshotAtMillis(namespace, id, tree, timestampMillis)
	})
}

func (e *Engine) LoadSnapshotContext(ctx context.Context, namespace SnapshotNamespace, id []byte) (*Tree, error) {
	return contextValue(ctx, func() (*Tree, error) {
		return e.LoadSnapshot(namespace, id)
	})
}

func (e *Engine) LoadSnapshotsContext(ctx context.Context, namespace SnapshotNamespace, ids [][]byte) (SnapshotSelection, error) {
	return contextValue(ctx, func() (SnapshotSelection, error) {
		return e.LoadSnapshots(namespace, ids)
	})
}

func (e *Engine) ListSnapshotsContext(ctx context.Context, namespace SnapshotNamespace) ([]SnapshotRoot, error) {
	return contextValue(ctx, func() ([]SnapshotRoot, error) {
		return e.ListSnapshots(namespace)
	})
}

func (e *Engine) CompareAndSwapSnapshotContext(ctx context.Context, namespace SnapshotNamespace, id []byte, expected *Tree, replacement *Tree) (NamedRootUpdate, error) {
	return contextValue(ctx, func() (NamedRootUpdate, error) {
		return e.CompareAndSwapSnapshot(namespace, id, expected, replacement)
	})
}

func (e *Engine) CompareAndSwapSnapshotAtMillisContext(ctx context.Context, namespace SnapshotNamespace, id []byte, expected *Tree, replacement *Tree, timestampMillis uint64) (NamedRootUpdate, error) {
	return contextValue(ctx, func() (NamedRootUpdate, error) {
		return e.CompareAndSwapSnapshotAtMillis(namespace, id, expected, replacement, timestampMillis)
	})
}

func (e *Engine) DeleteSnapshotContext(ctx context.Context, namespace SnapshotNamespace, id []byte) error {
	return contextVoid(ctx, func() error {
		return e.DeleteSnapshot(namespace, id)
	})
}

func (e *Engine) CollectStatsJSONContext(ctx context.Context, tree Tree) (string, error) {
	return contextValue(ctx, func() (string, error) {
		return e.CollectStatsJSON(tree)
	})
}

func (e *Engine) CollectStatsContext(ctx context.Context, tree Tree) (TreeStats, error) {
	return contextValue(ctx, func() (TreeStats, error) {
		return e.CollectStats(tree)
	})
}

func (e *Engine) StatsDiffJSONContext(ctx context.Context, before Tree, after Tree) (string, error) {
	return contextValue(ctx, func() (string, error) {
		return e.StatsDiffJSON(before, after)
	})
}

func (e *Engine) StatsDiffContext(ctx context.Context, before Tree, after Tree) (StatsComparison, error) {
	return contextValue(ctx, func() (StatsComparison, error) {
		return e.StatsDiff(before, after)
	})
}

func (e *Engine) DebugTreeJSONContext(ctx context.Context, tree Tree) (string, error) {
	return contextValue(ctx, func() (string, error) {
		return e.DebugTreeJSON(tree)
	})
}

func (e *Engine) DebugTreeContext(ctx context.Context, tree Tree) (TreeDebugView, error) {
	return contextValue(ctx, func() (TreeDebugView, error) {
		return e.DebugTree(tree)
	})
}

func (e *Engine) DebugTreeTextContext(ctx context.Context, tree Tree) (string, error) {
	return contextValue(ctx, func() (string, error) {
		return e.DebugTreeText(tree)
	})
}

func (e *Engine) DebugCompareTreesJSONContext(ctx context.Context, left Tree, right Tree) (string, error) {
	return contextValue(ctx, func() (string, error) {
		return e.DebugCompareTreesJSON(left, right)
	})
}

func (e *Engine) DebugCompareTreesContext(ctx context.Context, left Tree, right Tree) (TreeDebugComparison, error) {
	return contextValue(ctx, func() (TreeDebugComparison, error) {
		return e.DebugCompareTrees(left, right)
	})
}

func (e *Engine) DebugCompareTreesTextContext(ctx context.Context, left Tree, right Tree) (string, error) {
	return contextValue(ctx, func() (string, error) {
		return e.DebugCompareTreesText(left, right)
	})
}

func (e *Engine) CacheStatsContext(ctx context.Context) (CacheStats, error) {
	return contextValue(ctx, e.CacheStats)
}

func (e *Engine) ClearCacheContext(ctx context.Context) error {
	return contextVoid(ctx, e.ClearCache)
}

func (e *Engine) PinTreeRootContext(ctx context.Context, tree Tree) (uint64, error) {
	return contextValue(ctx, func() (uint64, error) {
		return e.PinTreeRoot(tree)
	})
}

func (e *Engine) PinTreePathContext(ctx context.Context, tree Tree, key []byte) (uint64, error) {
	return contextValue(ctx, func() (uint64, error) {
		return e.PinTreePath(tree, key)
	})
}

func (e *Engine) UnpinAllCacheNodesContext(ctx context.Context) (uint64, error) {
	return contextValue(ctx, e.UnpinAllCacheNodes)
}

func (e *Engine) MetricsContext(ctx context.Context) (Metrics, error) {
	return contextValue(ctx, e.Metrics)
}

func (e *Engine) ResetMetricsContext(ctx context.Context) error {
	return contextVoid(ctx, e.ResetMetrics)
}

func (e *Engine) PublishPrefixPathHintContext(ctx context.Context, tree Tree, prefix []byte) (bool, error) {
	return contextValue(ctx, func() (bool, error) {
		return e.PublishPrefixPathHint(tree, prefix)
	})
}

func (e *Engine) HydratePrefixPathHintContext(ctx context.Context, tree Tree, prefix []byte) (bool, error) {
	return contextValue(ctx, func() (bool, error) {
		return e.HydratePrefixPathHint(tree, prefix)
	})
}

func (e *Engine) PublishChangedSpansHintContext(ctx context.Context, base Tree, changed Tree, spans []ChangedSpan) (bool, error) {
	return contextValue(ctx, func() (bool, error) {
		return e.PublishChangedSpansHint(base, changed, spans)
	})
}

func (e *Engine) LoadChangedSpansHintContext(ctx context.Context, base Tree, changed Tree) (*ChangedSpanHint, error) {
	return contextValue(ctx, func() (*ChangedSpanHint, error) {
		return e.LoadChangedSpansHint(base, changed)
	})
}

func (e *Engine) StructuralDiffPageContext(ctx context.Context, base Tree, other Tree, cursorJSON *string, limit uint64) (StructuralDiffPage, error) {
	return contextValue(ctx, func() (StructuralDiffPage, error) {
		return e.StructuralDiffPage(base, other, cursorJSON, limit)
	})
}

func (e *Engine) StructuralDiffPageWithCursorContext(ctx context.Context, base Tree, other Tree, cursor *StructuralDiffCursor, limit uint64) (StructuralDiffPage, error) {
	return contextValue(ctx, func() (StructuralDiffPage, error) {
		return e.StructuralDiffPageWithCursor(base, other, cursor, limit)
	})
}

func (e *Engine) MarkReachableContext(ctx context.Context, roots []Tree) (GcReachability, error) {
	return contextValue(ctx, func() (GcReachability, error) {
		return e.MarkReachable(roots)
	})
}

func (e *Engine) PlanGCContext(ctx context.Context, roots []Tree, candidateCids [][]byte) (GcPlan, error) {
	return contextValue(ctx, func() (GcPlan, error) {
		return e.PlanGC(roots, candidateCids)
	})
}

func (e *Engine) SweepGCContext(ctx context.Context, roots []Tree, candidateCids [][]byte) (GcSweep, error) {
	return contextValue(ctx, func() (GcSweep, error) {
		return e.SweepGC(roots, candidateCids)
	})
}

func (e *Engine) ListNodeCidsContext(ctx context.Context) ([][]byte, error) {
	return contextValue(ctx, e.ListNodeCids)
}

func (e *Engine) PlanStoreGCContext(ctx context.Context, roots []Tree) (GcPlan, error) {
	return contextValue(ctx, func() (GcPlan, error) {
		return e.PlanStoreGC(roots)
	})
}

func (e *Engine) SweepStoreGCContext(ctx context.Context, roots []Tree) (GcSweep, error) {
	return contextValue(ctx, func() (GcSweep, error) {
		return e.SweepStoreGC(roots)
	})
}

func (e *Engine) PlanStoreGCForRetentionContext(ctx context.Context, retention NamedRootRetention) (GcPlan, error) {
	return contextValue(ctx, func() (GcPlan, error) {
		return e.PlanStoreGCForRetention(retention)
	})
}

func (e *Engine) SweepStoreGCForRetentionContext(ctx context.Context, retention NamedRootRetention) (GcSweep, error) {
	return contextValue(ctx, func() (GcSweep, error) {
		return e.SweepStoreGCForRetention(retention)
	})
}

func (e *Engine) MarkReachableBlobsContext(ctx context.Context, roots []Tree) (BlobGcReachability, error) {
	return contextValue(ctx, func() (BlobGcReachability, error) {
		return e.MarkReachableBlobs(roots)
	})
}

func (e *Engine) PlanBlobGCContext(ctx context.Context, blobStore *BlobStore, roots []Tree, candidates []BlobRef) (BlobGcPlan, error) {
	return contextValue(ctx, func() (BlobGcPlan, error) {
		return e.PlanBlobGC(blobStore, roots, candidates)
	})
}

func (e *Engine) SweepBlobGCContext(ctx context.Context, blobStore *BlobStore, roots []Tree, candidates []BlobRef) (BlobGcSweep, error) {
	return contextValue(ctx, func() (BlobGcSweep, error) {
		return e.SweepBlobGC(blobStore, roots, candidates)
	})
}

func (e *Engine) PlanBlobStoreGCContext(ctx context.Context, blobStore *BlobStore, roots []Tree) (BlobGcPlan, error) {
	return contextValue(ctx, func() (BlobGcPlan, error) {
		return e.PlanBlobStoreGC(blobStore, roots)
	})
}

func (e *Engine) SweepBlobStoreGCContext(ctx context.Context, blobStore *BlobStore, roots []Tree) (BlobGcSweep, error) {
	return contextValue(ctx, func() (BlobGcSweep, error) {
		return e.SweepBlobStoreGC(blobStore, roots)
	})
}

func (e *Engine) PlanMissingNodesContext(ctx context.Context, tree Tree, destination *Engine) (MissingNodePlan, error) {
	return contextValue(ctx, func() (MissingNodePlan, error) {
		return e.PlanMissingNodes(tree, destination)
	})
}

func (e *Engine) CopyMissingNodesContext(ctx context.Context, tree Tree, destination *Engine) (MissingNodeCopy, error) {
	return contextValue(ctx, func() (MissingNodeCopy, error) {
		return e.CopyMissingNodes(tree, destination)
	})
}

func (e *Engine) ExportSnapshotContext(ctx context.Context, tree Tree) (SnapshotBundle, error) {
	return contextValue(ctx, func() (SnapshotBundle, error) {
		return e.ExportSnapshot(tree)
	})
}

func (e *Engine) ImportSnapshotContext(ctx context.Context, bundle SnapshotBundle) (Tree, error) {
	return contextValue(ctx, func() (Tree, error) {
		return e.ImportSnapshot(bundle)
	})
}
