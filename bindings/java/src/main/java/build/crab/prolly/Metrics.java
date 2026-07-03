package build.crab.prolly;

public final class Metrics {
    private final long nodeCacheHits;
    private final long nodeCacheMisses;
    private final long nodeCacheEvictions;
    private final long nodesRead;
    private final long bytesRead;
    private final long nodesWritten;
    private final long bytesWritten;
    private final long storeGetCalls;
    private final long storeBatchGetCalls;
    private final long storeBatchGetKeys;
    private final long storePutCalls;
    private final long storeBatchPutCalls;
    private final long storeBatchPutNodes;

    Metrics(MetricsRecord record) {
        this.nodeCacheHits = ProllyJavaAdapters.metricsNodeCacheHits(record);
        this.nodeCacheMisses = ProllyJavaAdapters.metricsNodeCacheMisses(record);
        this.nodeCacheEvictions = ProllyJavaAdapters.metricsNodeCacheEvictions(record);
        this.nodesRead = ProllyJavaAdapters.metricsNodesRead(record);
        this.bytesRead = ProllyJavaAdapters.metricsBytesRead(record);
        this.nodesWritten = ProllyJavaAdapters.metricsNodesWritten(record);
        this.bytesWritten = ProllyJavaAdapters.metricsBytesWritten(record);
        this.storeGetCalls = ProllyJavaAdapters.metricsStoreGetCalls(record);
        this.storeBatchGetCalls = ProllyJavaAdapters.metricsStoreBatchGetCalls(record);
        this.storeBatchGetKeys = ProllyJavaAdapters.metricsStoreBatchGetKeys(record);
        this.storePutCalls = ProllyJavaAdapters.metricsStorePutCalls(record);
        this.storeBatchPutCalls = ProllyJavaAdapters.metricsStoreBatchPutCalls(record);
        this.storeBatchPutNodes = ProllyJavaAdapters.metricsStoreBatchPutNodes(record);
    }

    public long nodeCacheHits() {
        return nodeCacheHits;
    }

    public long nodeCacheMisses() {
        return nodeCacheMisses;
    }

    public long nodeCacheEvictions() {
        return nodeCacheEvictions;
    }

    public long nodesRead() {
        return nodesRead;
    }

    public long bytesRead() {
        return bytesRead;
    }

    public long nodesWritten() {
        return nodesWritten;
    }

    public long bytesWritten() {
        return bytesWritten;
    }

    public long storeGetCalls() {
        return storeGetCalls;
    }

    public long storeBatchGetCalls() {
        return storeBatchGetCalls;
    }

    public long storeBatchGetKeys() {
        return storeBatchGetKeys;
    }

    public long storePutCalls() {
        return storePutCalls;
    }

    public long storeBatchPutCalls() {
        return storeBatchPutCalls;
    }

    public long storeBatchPutNodes() {
        return storeBatchPutNodes;
    }
}
