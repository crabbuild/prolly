package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexBuildResult;

public record IndexBuildResult(
        byte[] sourceVersion,
        byte[] indexVersion,
        byte[] stateVersion,
        long generation,
        long entries,
        long attempts,
        boolean activated) {
    static IndexBuildResult fromNative(JavaIndexBuildResult value) {
        return new IndexBuildResult(
                value.getSourceVersion().clone(), value.getIndexVersion().clone(),
                value.getStateVersion().clone(), value.getGeneration(), value.getEntries(),
                value.getAttempts(), value.getActivated());
    }
}
