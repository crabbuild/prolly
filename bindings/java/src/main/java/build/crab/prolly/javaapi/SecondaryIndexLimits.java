package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaSecondaryIndexLimits;
import build.crab.prolly.SecondaryIndexLimitsRecord;
import build.crab.prolly.api.JavaPortableBridge;

public record SecondaryIndexLimits(
        long maxTermBytes,
        long maxProjectionBytes,
        long maxAllValueBytes,
        long maxTermsPerRecord,
        long maxProjectedBytesPerRecord,
        long maxDerivedMutationsPerTransaction,
        long maxProjectedBytesPerTransaction,
        long maxIndexes,
        long buildPageSize,
        long maxTemporarySortBytes,
        long maxBundleNodes,
        long maxBundleBytes,
        long maxVerificationEntries,
        long maxWriteRetries,
        long maxBuildRetries) {

    public static SecondaryIndexLimits defaults() {
        return fromNative(JavaPortableBridge.defaultSecondaryIndexLimits());
    }

    public SecondaryIndexLimits withMaxTermBytes(long value) {
        return new SecondaryIndexLimits(
                value, maxProjectionBytes, maxAllValueBytes, maxTermsPerRecord,
                maxProjectedBytesPerRecord, maxDerivedMutationsPerTransaction,
                maxProjectedBytesPerTransaction, maxIndexes, buildPageSize,
                maxTemporarySortBytes, maxBundleNodes, maxBundleBytes,
                maxVerificationEntries, maxWriteRetries, maxBuildRetries);
    }

    static SecondaryIndexLimits fromNative(JavaSecondaryIndexLimits value) {
        return new SecondaryIndexLimits(
                value.getMaxTermBytes(), value.getMaxProjectionBytes(),
                value.getMaxAllValueBytes(), value.getMaxTermsPerRecord(),
                value.getMaxProjectedBytesPerRecord(),
                value.getMaxDerivedMutationsPerTransaction(),
                value.getMaxProjectedBytesPerTransaction(), value.getMaxIndexes(),
                value.getBuildPageSize(), value.getMaxTemporarySortBytes(),
                value.getMaxBundleNodes(), value.getMaxBundleBytes(),
                value.getMaxVerificationEntries(), value.getMaxWriteRetries(),
                value.getMaxBuildRetries());
    }

    SecondaryIndexLimitsRecord toNative() {
        return JavaPortableBridge.secondaryIndexLimits(
                maxTermBytes, maxProjectionBytes, maxAllValueBytes, maxTermsPerRecord,
                maxProjectedBytesPerRecord, maxDerivedMutationsPerTransaction,
                maxProjectedBytesPerTransaction, maxIndexes, buildPageSize,
                maxTemporarySortBytes, maxBundleNodes, maxBundleBytes,
                maxVerificationEntries, maxWriteRetries, maxBuildRetries);
    }
}
