package build.crab.prolly.javaapi;

public final class ProximityReadSession implements AutoCloseable {
    private build.crab.prolly.api.ProximityReadSession nativeSession;
    ProximityReadSession(ProximityMap owner, build.crab.prolly.api.ProximityReadSession nativeSession) {
        this.nativeSession = nativeSession;
    }
    public SearchResult search(SearchRequest request) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        var query = new java.util.ArrayList<Float>(request.vector().length);
        for (float value : request.vector()) query.add(value);
        return ProximityMap.fromNative(
                build.crab.prolly.api.JavaPortableBridge.searchExact(
                        nativeSession, query, request.topK()));
    }
    public build.crab.prolly.ExactProximityRecordRecord get(byte[] key) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        return nativeSession.get(key.clone());
    }
    public boolean contains(byte[] key) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        return nativeSession.containsKey(key.clone());
    }
    @Override public void close() {
        if (nativeSession != null) { nativeSession.close(); nativeSession = null; }
    }
}
