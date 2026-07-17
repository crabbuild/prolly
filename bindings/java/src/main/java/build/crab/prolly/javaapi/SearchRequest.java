package build.crab.prolly.javaapi;

import build.crab.prolly.api.JavaProximitySearchRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Complete, owned proximity-search request corresponding to Rust's SearchRequest. */
public record SearchRequest(
        float[] vector,
        int topK,
        Policy policy,
        AdaptiveQuality adaptiveQuality,
        SearchBudget budget,
        SearchFilter filter,
        Kernel kernel,
        Backend backend,
        Integer hnswEfSearch,
        Integer pqRerankMultiplier) {

    public enum Policy { EXACT, FIXED_BUDGET, ADAPTIVE }
    public enum AdaptiveQuality { FAST, BALANCED, HIGH_RECALL }
    public enum FilterKind { ALL, KEY_RANGE, PREFIX, ELIGIBLE_KEYS }
    public enum Kernel { SCALAR_DETERMINISTIC, SIMD_DETERMINISTIC, AUTO_DETERMINISTIC }
    public enum Backend { NATIVE, PRODUCT_QUANTIZED, HNSW, COMPOSITE, AUTO }

    public record SearchBudget(
            Long maxNodes,
            Long maxCommittedBytes,
            Long maxDistanceEvaluations,
            Long maxFrontierEntries) {
        public SearchBudget {
            requireNonNegative(maxNodes, "maxNodes");
            requireNonNegative(maxCommittedBytes, "maxCommittedBytes");
            requireNonNegative(maxDistanceEvaluations, "maxDistanceEvaluations");
            requireNonNegative(maxFrontierEntries, "maxFrontierEntries");
        }
        public static SearchBudget unlimited() { return new SearchBudget(null, null, null, null); }
        private static void requireNonNegative(Long value, String name) {
            if (value != null && value < 0) throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    public record SearchFilter(
            FilterKind kind,
            byte[] start,
            byte[] rangeEnd,
            byte[] prefix,
            List<byte[]> eligibleKeys) {
        public SearchFilter {
            Objects.requireNonNull(kind, "kind");
            start = copy(start);
            rangeEnd = copy(rangeEnd);
            prefix = copy(prefix);
            eligibleKeys = copyKeys(eligibleKeys);
            if (kind == FilterKind.PREFIX && prefix == null) {
                throw new IllegalArgumentException("prefix filter requires a prefix");
            }
        }
        public static SearchFilter all() {
            return new SearchFilter(FilterKind.ALL, null, null, null, List.of());
        }
        public static SearchFilter keyRange(byte[] start, byte[] rangeEnd) {
            return new SearchFilter(FilterKind.KEY_RANGE, start, rangeEnd, null, List.of());
        }
        public static SearchFilter prefix(byte[] prefix) {
            return new SearchFilter(FilterKind.PREFIX, null, null, prefix, List.of());
        }
        public static SearchFilter eligibleKeys(List<byte[]> keys) {
            return new SearchFilter(FilterKind.ELIGIBLE_KEYS, null, null, null, keys);
        }
        @Override public byte[] start() { return copy(start); }
        @Override public byte[] rangeEnd() { return copy(rangeEnd); }
        @Override public byte[] prefix() { return copy(prefix); }
        @Override public List<byte[]> eligibleKeys() { return copyKeys(eligibleKeys); }
        private static byte[] copy(byte[] value) { return value == null ? null : value.clone(); }
        private static List<byte[]> copyKeys(List<byte[]> keys) {
            if (keys == null) return List.of();
            return keys.stream().map(key -> Objects.requireNonNull(key, "eligible key").clone()).toList();
        }
    }

    public SearchRequest {
        vector = Objects.requireNonNull(vector, "vector").clone();
        policy = Objects.requireNonNull(policy, "policy");
        budget = Objects.requireNonNull(budget, "budget");
        filter = Objects.requireNonNull(filter, "filter");
        kernel = Objects.requireNonNull(kernel, "kernel");
        backend = Objects.requireNonNull(backend, "backend");
        if (vector.length == 0 || topK <= 0) throw new IllegalArgumentException("invalid search request");
        if (policy == Policy.ADAPTIVE && adaptiveQuality == null) {
            throw new IllegalArgumentException("adaptive search requires adaptiveQuality");
        }
        if (hnswEfSearch != null && hnswEfSearch < 0) {
            throw new IllegalArgumentException("hnswEfSearch must be non-negative");
        }
        if (pqRerankMultiplier != null && (pqRerankMultiplier < 0 || pqRerankMultiplier > 65_535)) {
            throw new IllegalArgumentException("pqRerankMultiplier must fit an unsigned 16-bit value");
        }
    }

    @Override public float[] vector() { return vector.clone(); }

    public static SearchRequest exact(float[] vector, int topK) {
        return new SearchRequest(
                vector, topK, Policy.EXACT, null, SearchBudget.unlimited(), SearchFilter.all(),
                Kernel.AUTO_DETERMINISTIC, Backend.NATIVE, null, null);
    }

    public static SearchRequest fixedBudget(
            float[] vector,
            int topK,
            SearchBudget budget,
            SearchFilter filter,
            Kernel kernel,
            Backend backend) {
        return new SearchRequest(
                vector, topK, Policy.FIXED_BUDGET, null, budget, filter, kernel, backend, null, null);
    }

    public static SearchRequest adaptive(
            float[] vector,
            int topK,
            AdaptiveQuality quality,
            SearchBudget budget,
            SearchFilter filter,
            Kernel kernel,
            Backend backend) {
        return new SearchRequest(
                vector, topK, Policy.ADAPTIVE, quality, budget, filter, kernel, backend, null, null);
    }

    SearchRequest ownedCopy() {
        return new SearchRequest(
                vector, topK, policy, adaptiveQuality, budget, filter, kernel, backend,
                hnswEfSearch, pqRerankMultiplier);
    }

    JavaProximitySearchRequest toNative() {
        var query = new ArrayList<Float>(vector.length);
        for (float value : vector) query.add(value);
        return new JavaProximitySearchRequest(
                query,
                topK,
                policy.name(),
                adaptiveQuality == null ? null : adaptiveQuality.name(),
                budget.maxNodes(),
                budget.maxCommittedBytes(),
                budget.maxDistanceEvaluations(),
                budget.maxFrontierEntries(),
                filter.kind().name(),
                filter.start(),
                filter.rangeEnd(),
                filter.prefix(),
                filter.eligibleKeys(),
                kernel.name(),
                backend.name(),
                hnswEfSearch,
                pqRerankMultiplier);
    }
}
