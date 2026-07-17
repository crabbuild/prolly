package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;
import java.util.List;

public final class SecondaryIndex implements AutoCloseable {
    private build.crab.prolly.api.SecondaryIndex nativeIndex;
    SecondaryIndex(build.crab.prolly.api.SecondaryIndex nativeIndex) { this.nativeIndex = nativeIndex; }
    private build.crab.prolly.api.SecondaryIndex open() {
        if (nativeIndex == null) throw new IllegalStateException("secondary index is closed");
        return nativeIndex;
    }
    public List<IndexMatch> exact(byte[] term) {
        return open().exact(term.clone()).stream().map(value -> new IndexMatch(
                value.getTerm().clone(), value.getPrimaryKey().clone(),
                value.getProjection() == null ? null : value.getProjection().clone())).toList();
    }
    public IndexPage exactPage(byte[] term, int limit) {
        return new IndexPage(JavaPortableBridge.openIndexExact(open(), term.clone(), limit));
    }
    @Override public void close() {
        if (nativeIndex != null) { nativeIndex.close(); nativeIndex = null; }
    }
}
