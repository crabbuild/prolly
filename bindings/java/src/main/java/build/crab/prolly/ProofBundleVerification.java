package build.crab.prolly;

public record ProofBundleVerification(
        ProofBundleSummary summary,
        boolean valid,
        long existsCount,
        long absenceCount,
        long entryCount,
        long diffCount,
        RangeCursorRecord nextCursor) {
    public ProofBundleVerification {
        nextCursor = cloneCursor(nextCursor);
    }

    static ProofBundleVerification fromRecord(ProofBundleVerificationRecord record) {
        return new ProofBundleVerification(
                ProofBundleSummary.fromRecord(record.getSummary()),
                record.getValid(),
                ProllyJavaAdapters.proofBundleVerificationExistsCount(record),
                ProllyJavaAdapters.proofBundleVerificationAbsenceCount(record),
                ProllyJavaAdapters.proofBundleVerificationEntryCount(record),
                ProllyJavaAdapters.proofBundleVerificationDiffCount(record),
                record.getNextCursor());
    }

    @Override
    public RangeCursorRecord nextCursor() {
        return cloneCursor(nextCursor);
    }

    private static RangeCursorRecord cloneCursor(RangeCursorRecord cursor) {
        if (cursor == null) {
            return null;
        }
        byte[] afterKey = cursor.getAfterKey();
        return new RangeCursorRecord(afterKey == null ? null : afterKey.clone());
    }
}
