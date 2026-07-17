package build.crab.prolly.javaapi;

import build.crab.prolly.IndexBuildResultRecord;

public record IndexBuildResult(
        byte[] sourceVersion,
        byte[] indexVersion,
        byte[] catalogVersion,
        boolean activated) {
    static IndexBuildResult fromNative(IndexBuildResultRecord value) {
        return new IndexBuildResult(
                value.getSourceVersion().clone(), value.getIndexVersion().clone(),
                value.getCatalogVersion().clone(), value.getActivated());
    }
}
