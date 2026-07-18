package build.crab.prolly;

public final class BatchApplyStats {
    private final long inputMutations;
    private final long effectiveMutations;
    private final boolean preprocessInputSorted;
    private final long entriesStreamed;
    private final long nodesRead;
    private final long writtenNodes;
    private final long nodesReused;
    private final long bytesRead;
    private final long writtenBytes;
    private final long resyncDistanceEntries;
    private final long resyncDistanceNodes;
    private final boolean usedKeyStableFastPath;
    private final boolean usedBatchedValueUpdatePath;
    private final long parallelWidth;
    private final long parallelTasks;
    private final long structuralIslands;
    private final long coalescedIslands;

    BatchApplyStats(BatchApplyStatsRecord record) {
        this.inputMutations = ProllyJavaAdapters.batchStatsInputMutations(record);
        this.effectiveMutations = ProllyJavaAdapters.batchStatsEffectiveMutations(record);
        this.preprocessInputSorted = record.getPreprocessInputSorted();
        this.entriesStreamed = ProllyJavaAdapters.batchStatsEntriesStreamed(record);
        this.nodesRead = ProllyJavaAdapters.batchStatsNodesRead(record);
        this.writtenNodes = ProllyJavaAdapters.batchStatsWrittenNodes(record);
        this.nodesReused = ProllyJavaAdapters.batchStatsNodesReused(record);
        this.bytesRead = ProllyJavaAdapters.batchStatsBytesRead(record);
        this.writtenBytes = ProllyJavaAdapters.batchStatsWrittenBytes(record);
        this.resyncDistanceEntries = ProllyJavaAdapters.batchStatsResyncDistanceEntries(record);
        this.resyncDistanceNodes = ProllyJavaAdapters.batchStatsResyncDistanceNodes(record);
        this.usedKeyStableFastPath = record.getUsedKeyStableFastPath();
        this.usedBatchedValueUpdatePath = record.getUsedBatchedValueUpdatePath();
        this.parallelWidth = ProllyJavaAdapters.batchStatsParallelWidth(record);
        this.parallelTasks = ProllyJavaAdapters.batchStatsParallelTasks(record);
        this.structuralIslands = ProllyJavaAdapters.batchStatsStructuralIslands(record);
        this.coalescedIslands = ProllyJavaAdapters.batchStatsCoalescedIslands(record);
    }

    public long inputMutations() { return inputMutations; }

    public long effectiveMutations() { return effectiveMutations; }

    public boolean preprocessInputSorted() { return preprocessInputSorted; }

    public long entriesStreamed() { return entriesStreamed; }

    public long nodesRead() { return nodesRead; }

    public long writtenNodes() { return writtenNodes; }

    public long nodesReused() { return nodesReused; }

    public long bytesRead() { return bytesRead; }

    public long writtenBytes() { return writtenBytes; }

    public long resyncDistanceEntries() { return resyncDistanceEntries; }

    public long resyncDistanceNodes() { return resyncDistanceNodes; }

    public boolean usedKeyStableFastPath() { return usedKeyStableFastPath; }

    public boolean usedBatchedValueUpdatePath() { return usedBatchedValueUpdatePath; }

    public long parallelWidth() { return parallelWidth; }

    public long parallelTasks() { return parallelTasks; }

    public long structuralIslands() { return structuralIslands; }

    public long coalescedIslands() { return coalescedIslands; }
}
