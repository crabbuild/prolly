package build.crab.prolly.javaapi;

public final class ProximityReadSession implements AutoCloseable {
    private final ProximityMap owner;
    private build.crab.prolly.api.ProximityReadSession nativeSession;
    ProximityReadSession(ProximityMap owner, build.crab.prolly.api.ProximityReadSession nativeSession) {
        this.owner = owner; this.nativeSession = nativeSession;
    }
    public SearchResult search(SearchRequest request) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        return owner.search(request);
    }
    @Override public void close() {
        if (nativeSession != null) { nativeSession.close(); nativeSession = null; }
    }
}
