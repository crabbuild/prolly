package build.crab.prolly.javaapi;

public record IndexEntry(byte[] term, byte[] projection) {
    public IndexEntry {
        term = term.clone();
        projection = projection == null ? null : projection.clone();
    }

    @Override public byte[] term() { return term.clone(); }
    @Override public byte[] projection() { return projection == null ? null : projection.clone(); }
}
