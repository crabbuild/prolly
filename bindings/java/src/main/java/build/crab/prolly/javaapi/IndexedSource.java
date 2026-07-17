package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedSource;

public record IndexedSource(
        byte[] term,
        byte[] primaryKey,
        byte[] projection,
        byte[] sourceValue) {
    static IndexedSource fromNative(JavaIndexedSource value) {
        return new IndexedSource(
                value.getTerm().clone(), value.getPrimaryKey().clone(),
                value.getProjection() == null ? null : value.getProjection().clone(),
                value.getSourceValue().clone());
    }
}
