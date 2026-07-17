package build.crab.prolly.javaapi;

public record BatchApplyStats(
        long inputMutations,
        long effectiveMutations,
        boolean preprocessInputSorted,
        long affectedLeaves,
        long changedLeaves,
        long sparseLeafApplies,
        long writtenNodes,
        long writtenBytes,
        boolean usedAppendFastPath,
        boolean usedBatchedRoute,
        boolean usedCoalescedRebuild,
        boolean usedDeferredRebalancing,
        boolean usedBottomUpRebuild,
        boolean cacheWrittenNodes) {
    static BatchApplyStats fromBridge(build.crab.prolly.api.JavaBatchApplyStats value) {
        return new BatchApplyStats(
                value.getInputMutations(), value.getEffectiveMutations(), value.getPreprocessInputSorted(),
                value.getAffectedLeaves(), value.getChangedLeaves(), value.getSparseLeafApplies(),
                value.getWrittenNodes(), value.getWrittenBytes(), value.getUsedAppendFastPath(),
                value.getUsedBatchedRoute(), value.getUsedCoalescedRebuild(),
                value.getUsedDeferredRebalancing(), value.getUsedBottomUpRebuild(),
                value.getCacheWrittenNodes());
    }
}
