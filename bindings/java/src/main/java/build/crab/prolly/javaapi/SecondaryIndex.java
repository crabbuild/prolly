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

    public byte[] name() { return JavaPortableBridge.indexName(open()).clone(); }

    public List<IndexMatch> exact(byte[] term) {
        return JavaPortableBridge.indexExact(open(), term.clone()).stream().map(value -> new IndexMatch(
                value.getTerm().clone(), value.getPrimaryKey().clone(),
                value.getProjection() == null ? null : value.getProjection().clone())).toList();
    }

    public List<IndexMatch> prefix(byte[] prefix) {
        return JavaPortableBridge.indexPrefix(open(), prefix.clone()).stream().map(value -> new IndexMatch(
                value.getTerm().clone(), value.getPrimaryKey().clone(),
                value.getProjection() == null ? null : value.getProjection().clone())).toList();
    }

    public List<IndexMatch> range(byte[] start, byte[] end) {
        return JavaPortableBridge.indexRange(
                open(), start.clone(), end == null ? null : end.clone()).stream().map(value -> new IndexMatch(
                value.getTerm().clone(), value.getPrimaryKey().clone(),
                value.getProjection() == null ? null : value.getProjection().clone())).toList();
    }

    public List<IndexedSource> records(byte[] term) {
        return JavaPortableBridge.indexRecords(open(), term.clone()).stream()
                .map(IndexedSource::fromNative).toList();
    }

    /** Retained direct-buffer page for the hot exact-query path. */
    public IndexPage exactPage(byte[] term, int limit) {
        return new IndexPage(JavaPortableBridge.openIndexExact(open(), term.clone(), limit));
    }

    public OwnedIndexPage exactPage(byte[] term, byte[] cursor, long limit) {
        return OwnedIndexPage.fromNative(JavaPortableBridge.indexExactPage(
                open(), term.clone(), cursor == null ? null : cursor.clone(), limit));
    }

    public OwnedIndexPage exactReversePage(byte[] term, byte[] cursor, long limit) {
        return OwnedIndexPage.fromNative(JavaPortableBridge.indexExactReversePage(
                open(), term.clone(), cursor == null ? null : cursor.clone(), limit));
    }

    public OwnedIndexPage prefixPage(byte[] prefix, byte[] cursor, long limit) {
        return OwnedIndexPage.fromNative(JavaPortableBridge.indexPrefixPage(
                open(), prefix.clone(), cursor == null ? null : cursor.clone(), limit));
    }

    public OwnedIndexPage prefixReversePage(byte[] prefix, byte[] cursor, long limit) {
        return OwnedIndexPage.fromNative(JavaPortableBridge.indexPrefixReversePage(
                open(), prefix.clone(), cursor == null ? null : cursor.clone(), limit));
    }

    public OwnedIndexPage rangePage(byte[] start, byte[] end, byte[] cursor, long limit) {
        return OwnedIndexPage.fromNative(JavaPortableBridge.indexRangePage(
                open(), start.clone(), end == null ? null : end.clone(),
                cursor == null ? null : cursor.clone(), limit));
    }

    public OwnedIndexPage rangeReversePage(byte[] start, byte[] end, byte[] cursor, long limit) {
        return OwnedIndexPage.fromNative(JavaPortableBridge.indexRangeReversePage(
                open(), start.clone(), end == null ? null : end.clone(),
                cursor == null ? null : cursor.clone(), limit));
    }

    @Override public void close() {
        if (nativeIndex != null) { nativeIndex.close(); nativeIndex = null; }
    }
}
