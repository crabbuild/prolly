package build.crab.prolly.javaapi;

public record BatchApplyStats(
        long inputMutations,
        long effectiveMutations,
        boolean preprocessInputSorted,
        long entriesStreamed,
        long nodesRead,
        long writtenNodes,
        long nodesReused,
        long bytesRead,
        long writtenBytes,
        long resyncDistanceEntries,
        long resyncDistanceNodes,
        boolean usedKeyStableFastPath,
        boolean usedBatchedValueUpdatePath,
        long parallelWidth,
        long parallelTasks,
        long structuralIslands,
        long coalescedIslands) {
    static BatchApplyStats fromBridge(build.crab.prolly.api.JavaBatchApplyStats value) {
        return new BatchApplyStats(
                value.getInputMutations(), value.getEffectiveMutations(), value.getPreprocessInputSorted(),
                value.getEntriesStreamed(), value.getNodesRead(), value.getWrittenNodes(),
                value.getNodesReused(), value.getBytesRead(), value.getWrittenBytes(),
                value.getResyncDistanceEntries(), value.getResyncDistanceNodes(),
                value.getUsedKeyStableFastPath(), value.getUsedBatchedValueUpdatePath(),
                value.getParallelWidth(), value.getParallelTasks(), value.getStructuralIslands(),
                value.getCoalescedIslands());
    }
}
