package build.crab.prolly.javaapi;

import build.crab.prolly.DiffPageRecord;
import build.crab.prolly.DiffRecord;
import build.crab.prolly.RangeCursorRecord;
import build.crab.prolly.api.JavaPortableBridge;
import java.util.List;

/** Version-pinned map comparison; later head changes do not affect its results. */
public final class MapComparison implements AutoCloseable {
    private build.crab.prolly.api.MapComparison nativeComparison;

    MapComparison(build.crab.prolly.api.MapComparison nativeComparison) {
        this.nativeComparison = nativeComparison;
    }

    private build.crab.prolly.api.MapComparison open() {
        if (nativeComparison == null) throw new IllegalStateException("map comparison is closed");
        return nativeComparison;
    }

    public MapVersion base() { return MapVersion.fromNative(open().base()); }
    public MapVersion target() { return MapVersion.fromNative(open().target()); }
    public List<DiffRecord> diff() { return open().diff(); }
    public DiffPageRecord diffPage(RangeCursorRecord cursor, byte[] end, long limit) {
        return JavaPortableBridge.diffPage(open(), cursor, end == null ? null : end.clone(), limit);
    }
    public DiffPageRecord diffPage(long limit) { return diffPage(null, null, limit); }

    @Override
    public void close() {
        if (nativeComparison != null) {
            nativeComparison.close();
            nativeComparison = null;
        }
    }
}
