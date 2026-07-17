package build.crab.prolly.javaapi;

import build.crab.prolly.ConflictPageRecord;
import build.crab.prolly.RangeCursorRecord;
import build.crab.prolly.TreeRecord;

/** Three-way merge pinned to a concrete base, head, and candidate. */
public final class MapMerge implements AutoCloseable {
    private build.crab.prolly.api.MapMerge nativeMerge;
    MapMerge(build.crab.prolly.api.MapMerge nativeMerge) { this.nativeMerge = nativeMerge; }
    private build.crab.prolly.api.MapMerge open() {
        if (nativeMerge == null) throw new IllegalStateException("map merge is closed");
        return nativeMerge;
    }
    public MapVersion base() { return MapVersion.fromNative(open().base()); }
    public MapVersion head() { return MapVersion.fromNative(open().head()); }
    public MapVersion candidate() { return MapVersion.fromNative(open().candidate()); }
    public TreeRecord merge(String resolver) { return open().merge(resolver); }
    public TreeRecord merge() { return merge(null); }
    public ConflictPageRecord conflictPage(RangeCursorRecord cursor, long limit) {
        return open().conflictPage(cursor, limit);
    }
    public MapUpdate publish(String resolver) { return MapUpdate.fromNative(open().publish(resolver)); }
    public MapUpdate publish() { return publish(null); }
    @Override public void close() { if (nativeMerge != null) { nativeMerge.close(); nativeMerge = null; } }
}
