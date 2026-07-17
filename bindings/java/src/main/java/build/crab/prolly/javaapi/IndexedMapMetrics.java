package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexedMapMetrics;

public record IndexedMapMetrics(
        long normalizedSourceMutations,
        long recordsExtracted,
        long termsEmitted,
        long projectedBytes,
        long physicalUpserts,
        long physicalDeletes,
        long unchangedEmissionsSkipped,
        long sourceNodesWritten,
        long indexNodesWritten,
        long catalogNodesWritten,
        long retries,
        long buildAttempts,
        long verificationOutcomes,
        long retainedRoots) {
    static IndexedMapMetrics fromNative(JavaIndexedMapMetrics value) {
        return new IndexedMapMetrics(
                value.getNormalizedSourceMutations(), value.getRecordsExtracted(),
                value.getTermsEmitted(), value.getProjectedBytes(), value.getPhysicalUpserts(),
                value.getPhysicalDeletes(), value.getUnchangedEmissionsSkipped(),
                value.getSourceNodesWritten(), value.getIndexNodesWritten(),
                value.getCatalogNodesWritten(), value.getRetries(), value.getBuildAttempts(),
                value.getVerificationOutcomes(), value.getRetainedRoots());
    }
}
