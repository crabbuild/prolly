package build.crab.prolly.javaapi;

import build.crab.prolly.api.PackedIndexPage;
import java.nio.ByteBuffer;
import java.util.List;

public final class IndexPage implements AutoCloseable {
    public record Row(ByteBuffer term, ByteBuffer primaryKey, ByteBuffer projection) {}

    private PackedIndexPage nativePage;
    private final List<Row> rows;

    IndexPage(PackedIndexPage nativePage) {
        this.nativePage = nativePage;
        this.rows = nativePage.getRows().stream().map(row -> new Row(
                row.getTerm().buffer(),
                row.getPrimaryKey().buffer(),
                row.getProjection() == null ? null : row.getProjection().buffer())).toList();
    }

    public List<Row> rows() {
        if (nativePage == null) throw new IllegalStateException("index page is closed");
        return rows;
    }

    @Override public void close() {
        if (nativePage != null) { nativePage.close(); nativePage = null; }
    }
}
