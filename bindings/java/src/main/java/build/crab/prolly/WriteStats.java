package build.crab.prolly;

public final class WriteStats {
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

    WriteStats(WriteStatsRecord record) {
        this.inputMutations = ProllyJavaAdapters.writeStatsInputMutations(record);
        this.effectiveMutations = ProllyJavaAdapters.writeStatsEffectiveMutations(record);
        this.entriesStreamed = ProllyJavaAdapters.writeStatsEntriesStreamed(record);
        this.nodesRead = ProllyJavaAdapters.writeStatsNodesRead(record);
        this.nodesWritten = ProllyJavaAdapters.writeStatsNodesWritten(record);
        this.nodesReused = ProllyJavaAdapters.writeStatsNodesReused(record);
        this.bytesRead = ProllyJavaAdapters.writeStatsBytesRead(record);
        this.bytesWritten = ProllyJavaAdapters.writeStatsBytesWritten(record);
        this.resyncDistanceEntries = ProllyJavaAdapters.writeStatsResyncDistanceEntries(record);
        this.resyncDistanceNodes = ProllyJavaAdapters.writeStatsResyncDistanceNodes(record);
        this.usedKeyStableFastPath = ProllyJavaAdapters.writeStatsUsedKeyStableFastPath(record);
        this.usedBatchedValueUpdatePath = ProllyJavaAdapters.writeStatsUsedBatchedValueUpdatePath(record);
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
