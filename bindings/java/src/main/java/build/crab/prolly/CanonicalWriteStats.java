package build.crab.prolly;

public final class CanonicalWriteStats {
    private final long inputMutations;
    private final long effectiveMutations;
    private final long entriesStreamed;
    private final long nodesRead;
    private final long nodesWritten;
    private final long nodesReused;
    private final long bytesRead;
    private final long bytesWritten;
    private final long resyncDistanceEntries;
    private final long resyncDistanceNodes;
    private final boolean usedKeyStableFastPath;
    private final boolean usedBatchedValueUpdatePath;

    CanonicalWriteStats(CanonicalWriteStatsRecord record) {
        this.inputMutations = ProllyJavaAdapters.canonicalWriteStatsInputMutations(record);
        this.effectiveMutations = ProllyJavaAdapters.canonicalWriteStatsEffectiveMutations(record);
        this.entriesStreamed = ProllyJavaAdapters.canonicalWriteStatsEntriesStreamed(record);
        this.nodesRead = ProllyJavaAdapters.canonicalWriteStatsNodesRead(record);
        this.nodesWritten = ProllyJavaAdapters.canonicalWriteStatsNodesWritten(record);
        this.nodesReused = ProllyJavaAdapters.canonicalWriteStatsNodesReused(record);
        this.bytesRead = ProllyJavaAdapters.canonicalWriteStatsBytesRead(record);
        this.bytesWritten = ProllyJavaAdapters.canonicalWriteStatsBytesWritten(record);
        this.resyncDistanceEntries = ProllyJavaAdapters.canonicalWriteStatsResyncDistanceEntries(record);
        this.resyncDistanceNodes = ProllyJavaAdapters.canonicalWriteStatsResyncDistanceNodes(record);
        this.usedKeyStableFastPath = ProllyJavaAdapters.canonicalWriteStatsUsedKeyStableFastPath(record);
        this.usedBatchedValueUpdatePath = ProllyJavaAdapters.canonicalWriteStatsUsedBatchedValueUpdatePath(record);
    }

    public long inputMutations() { return inputMutations; }
    public long effectiveMutations() { return effectiveMutations; }
    public long entriesStreamed() { return entriesStreamed; }
    public long nodesRead() { return nodesRead; }
    public long nodesWritten() { return nodesWritten; }
    public long nodesReused() { return nodesReused; }
    public long bytesRead() { return bytesRead; }
    public long bytesWritten() { return bytesWritten; }
    public long resyncDistanceEntries() { return resyncDistanceEntries; }
    public long resyncDistanceNodes() { return resyncDistanceNodes; }
    public boolean usedKeyStableFastPath() { return usedKeyStableFastPath; }
    public boolean usedBatchedValueUpdatePath() { return usedBatchedValueUpdatePath; }
}
