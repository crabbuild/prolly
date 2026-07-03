package build.crab.prolly;

public record ProofBundleSummary(
        long version,
        String kind,
        byte[] root,
        byte[] otherRoot,
        long keyCount,
        long pathNodeCount,
        byte[] start,
        byte[] end,
        byte[] after,
        byte[] requestedEnd,
        Long limit,
        boolean hasLookahead) {
    public ProofBundleSummary {
        root = root == null ? null : root.clone();
        otherRoot = otherRoot == null ? null : otherRoot.clone();
        start = start == null ? null : start.clone();
        end = end == null ? null : end.clone();
        after = after == null ? null : after.clone();
        requestedEnd = requestedEnd == null ? null : requestedEnd.clone();
    }

    static ProofBundleSummary fromRecord(ProofBundleSummaryRecord record) {
        return new ProofBundleSummary(
                ProllyJavaAdapters.proofBundleSummaryVersion(record),
                record.getKind(),
                record.getRoot(),
                record.getOtherRoot(),
                ProllyJavaAdapters.proofBundleSummaryKeyCount(record),
                ProllyJavaAdapters.proofBundleSummaryPathNodeCount(record),
                record.getStart(),
                record.getEnd(),
                record.getAfter(),
                record.getRequestedEnd(),
                ProllyJavaAdapters.proofBundleSummaryLimit(record),
                record.getHasLookahead());
    }

    @Override
    public byte[] root() {
        return root == null ? null : root.clone();
    }

    @Override
    public byte[] otherRoot() {
        return otherRoot == null ? null : otherRoot.clone();
    }

    @Override
    public byte[] start() {
        return start == null ? null : start.clone();
    }

    @Override
    public byte[] end() {
        return end == null ? null : end.clone();
    }

    @Override
    public byte[] after() {
        return after == null ? null : after.clone();
    }

    @Override
    public byte[] requestedEnd() {
        return requestedEnd == null ? null : requestedEnd.clone();
    }
}
