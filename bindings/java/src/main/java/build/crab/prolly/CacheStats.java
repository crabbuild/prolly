package build.crab.prolly;

public final class CacheStats {
    private final long cachedNodes;
    private final long cachedBytes;
    private final long pinnedNodes;
    private final long pinnedBytes;

    CacheStats(CacheStatsRecord record) {
        this.cachedNodes = ProllyJavaAdapters.cacheStatsCachedNodes(record);
        this.cachedBytes = ProllyJavaAdapters.cacheStatsCachedBytes(record);
        this.pinnedNodes = ProllyJavaAdapters.cacheStatsPinnedNodes(record);
        this.pinnedBytes = ProllyJavaAdapters.cacheStatsPinnedBytes(record);
    }

    public long cachedNodes() {
        return cachedNodes;
    }

    public long cachedBytes() {
        return cachedBytes;
    }

    public long pinnedNodes() {
        return pinnedNodes;
    }

    public long pinnedBytes() {
        return pinnedBytes;
    }
}
