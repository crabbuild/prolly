package build.crab.prolly;

public final class BatchApplyStats {
    private final long inputMutations;
    private final long effectiveMutations;
    private final boolean preprocessInputSorted;
    private final long affectedLeaves;
    private final long changedLeaves;
    private final long sparseLeafApplies;
    private final long writtenNodes;
    private final long writtenBytes;
    private final boolean usedAppendFastPath;
    private final boolean usedBatchedRoute;
    private final boolean usedCoalescedRebuild;
    private final boolean usedDeferredRebalancing;
    private final boolean usedBottomUpRebuild;
    private final boolean cacheWrittenNodes;

    BatchApplyStats(BatchApplyStatsRecord record) {
        this.inputMutations = ProllyJavaAdapters.batchStatsInputMutations(record);
        this.effectiveMutations = ProllyJavaAdapters.batchStatsEffectiveMutations(record);
        this.preprocessInputSorted = record.getPreprocessInputSorted();
        this.affectedLeaves = ProllyJavaAdapters.batchStatsAffectedLeaves(record);
        this.changedLeaves = ProllyJavaAdapters.batchStatsChangedLeaves(record);
        this.sparseLeafApplies = ProllyJavaAdapters.batchStatsSparseLeafApplies(record);
        this.writtenNodes = ProllyJavaAdapters.batchStatsWrittenNodes(record);
        this.writtenBytes = ProllyJavaAdapters.batchStatsWrittenBytes(record);
        this.usedAppendFastPath = record.getUsedAppendFastPath();
        this.usedBatchedRoute = record.getUsedBatchedRoute();
        this.usedCoalescedRebuild = record.getUsedCoalescedRebuild();
        this.usedDeferredRebalancing = record.getUsedDeferredRebalancing();
        this.usedBottomUpRebuild = record.getUsedBottomUpRebuild();
        this.cacheWrittenNodes = record.getCacheWrittenNodes();
    }

    public long inputMutations() {
        return inputMutations;
    }

    public long effectiveMutations() {
        return effectiveMutations;
    }

    public boolean preprocessInputSorted() {
        return preprocessInputSorted;
    }

    public long affectedLeaves() {
        return affectedLeaves;
    }

    public long changedLeaves() {
        return changedLeaves;
    }

    public long sparseLeafApplies() {
        return sparseLeafApplies;
    }

    public long writtenNodes() {
        return writtenNodes;
    }

    public long writtenBytes() {
        return writtenBytes;
    }

    public boolean usedAppendFastPath() {
        return usedAppendFastPath;
    }

    public boolean usedBatchedRoute() {
        return usedBatchedRoute;
    }

    public boolean usedCoalescedRebuild() {
        return usedCoalescedRebuild;
    }

    public boolean usedDeferredRebalancing() {
        return usedDeferredRebalancing;
    }

    public boolean usedBottomUpRebuild() {
        return usedBottomUpRebuild;
    }

    public boolean cacheWrittenNodes() {
        return cacheWrittenNodes;
    }
}
