package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.function.Predicate;

public final class ProximityReadSession implements AutoCloseable {
    private build.crab.prolly.api.ProximityReadSession nativeSession;
    ProximityReadSession(ProximityMap owner, build.crab.prolly.api.ProximityReadSession nativeSession) {
        this.nativeSession = nativeSession;
    }
    public SearchResult search(SearchRequest request) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        return ProximityMap.fromNative(
                build.crab.prolly.api.JavaPortableBridge.search(nativeSession, request.toNative()));
    }
    public SearchResult searchWithRuntime(
            SearchRequest request, ProximitySearchRuntime runtime) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        return ProximityMap.fromNative(JavaPortableBridge.searchWithRuntime(
                nativeSession, request.toNative(), runtime.open()));
    }
    public build.crab.prolly.ExactProximityRecordRecord get(byte[] key) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        return nativeSession.get(key.clone());
    }
    public boolean contains(byte[] key) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        return nativeSession.containsKey(key.clone());
    }
    public long scanRecords(Predicate<ProximityRecord> visitor) {
        if (nativeSession == null) throw new IllegalStateException("proximity session is closed");
        return JavaPortableBridge.scanRecords(nativeSession, record -> {
            var vector = new float[record.getVector().size()];
            for (int index = 0; index < vector.length; index++) vector[index] = record.getVector().get(index);
            return visitor.test(new ProximityRecord(
                    record.getKey().clone(), vector, record.getValue().clone()));
        });
    }
    @Override public void close() {
        if (nativeSession != null) { nativeSession.close(); nativeSession = null; }
    }
}
