package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexBuildResult;

public record IndexBuildResult(
        byte[] sourceVersion,
        byte[] indexVersion,
        byte[] catalogVersion,
        long generation,
        long entries,
        long attempts,
        boolean activated) {
    static IndexBuildResult fromNative(JavaIndexBuildResult value) {
        return new IndexBuildResult(
                value.getSourceVersion().clone(), value.getIndexVersion().clone(),
                value.getCatalogVersion().clone(), value.getGeneration(), value.getEntries(),
                value.getAttempts(), value.getActivated());
    }
}
