package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaIndexVerification;

public record IndexVerification(
        byte[] name,
        byte[] sourceVersion,
        byte[] expectedIndexVersion,
        byte[] actualIndexVersion,
        long expectedEntries,
        long actualEntries,
        long semanticDifferences,
        boolean valid,
        boolean canonical) {
    static IndexVerification fromNative(JavaIndexVerification value) {
        return new IndexVerification(
                value.getName().clone(), value.getSourceVersion().clone(),
                value.getExpectedIndexVersion().clone(), value.getActualIndexVersion().clone(),
                value.getExpectedEntries(), value.getActualEntries(), value.getSemanticDifferences(),
                value.getValid(), value.getCanonical());
    }
}
