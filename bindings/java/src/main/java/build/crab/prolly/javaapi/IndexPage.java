package build.crab.prolly.javaapi;

import build.crab.prolly.api.PackedIndexPage;
import java.util.List;

public final class IndexPage implements AutoCloseable {
    public record Row(ScopedBytes term, ScopedBytes primaryKey, ScopedBytes projection) {}

    private PackedIndexPage nativePage;
    private final List<Row> rows;

    IndexPage(PackedIndexPage nativePage) {
        this.nativePage = nativePage;
        this.rows = nativePage.getRows().stream().map(row -> new Row(
                new ScopedBytes(row.getTerm()),
                new ScopedBytes(row.getPrimaryKey()),
                row.getProjection() == null ? null : new ScopedBytes(row.getProjection()))).toList();
    }

    public List<Row> rows() {
        if (nativePage == null) throw new IllegalStateException("index page is closed");
        return rows;
    }

    @Override public void close() {
        if (nativePage != null) { nativePage.close(); nativePage = null; }
    }
}
