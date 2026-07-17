package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaPortableBridge;

public record ProximitySearchRuntimePolicy(
        long maxEntries,
        long maxBytes,
        long authoritativeMaxBytes,
        long hnswMaxBytes,
        long pqMaxBytes) {
    public ProximitySearchRuntimePolicy {
        if (maxEntries <= 0 || maxBytes <= 0 || authoritativeMaxBytes < 0
                || hnswMaxBytes < 0 || pqMaxBytes < 0) {
            throw new IllegalArgumentException("search runtime limits must be non-negative and totals positive");
        }
    }

    public static ProximitySearchRuntimePolicy defaults() {
        return fromNative(JavaPortableBridge.defaultProximitySearchRuntimePolicy());
    }

    static ProximitySearchRuntimePolicy fromNative(
            build.crab.prolly.api.JavaProximitySearchRuntimePolicy value) {
        return new ProximitySearchRuntimePolicy(
                value.getMaxEntries(),
                value.getMaxBytes(),
                value.getAuthoritativeMaxBytes(),
                value.getHnswMaxBytes(),
                value.getPqMaxBytes());
    }

    build.crab.prolly.api.JavaProximitySearchRuntimePolicy toNative() {
        return new build.crab.prolly.api.JavaProximitySearchRuntimePolicy(
                maxEntries, maxBytes, authoritativeMaxBytes, hnswMaxBytes, pqMaxBytes);
    }
}
