package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaCompositeBuildStats;

public record CompositeBuildStats(
        long diffEntries,
        long insertedRecords,
        long vectorUpdatedRecords,
        long valueOnlyRecords,
        long deletedRecords,
        long deltaRecords,
        long shadowRecords,
        long ownedBytesPeak,
        long encodedOutputBytes,
        long distanceEvaluations) {
    static CompositeBuildStats fromNative(JavaCompositeBuildStats value) {
        return new CompositeBuildStats(
                value.getDiffEntries(), value.getInsertedRecords(), value.getVectorUpdatedRecords(),
                value.getValueOnlyRecords(), value.getDeletedRecords(), value.getDeltaRecords(),
                value.getShadowRecords(), value.getOwnedBytesPeak(), value.getEncodedOutputBytes(),
                value.getDistanceEvaluations());
    }
}
