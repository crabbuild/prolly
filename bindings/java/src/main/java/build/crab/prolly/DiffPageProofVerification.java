package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record DiffPageProofVerification(
        boolean valid,
        boolean baseValid,
        boolean otherValid,
        boolean lookaheadValid,
        byte[] baseRoot,
        byte[] otherRoot,
        byte[] after,
        byte[] requestedEnd,
        byte[] proofEnd,
        long limit,
        List<DiffRecord> diffs,
        RangeCursorRecord nextCursor) {
    public DiffPageProofVerification {
        baseRoot = baseRoot == null ? null : baseRoot.clone();
        otherRoot = otherRoot == null ? null : otherRoot.clone();
        after = after == null ? null : after.clone();
        requestedEnd = requestedEnd == null ? null : requestedEnd.clone();
        proofEnd = proofEnd == null ? null : proofEnd.clone();
        diffs = cloneDiffs(diffs);
        nextCursor = cloneCursor(nextCursor);
    }

    static DiffPageProofVerification fromRecord(DiffPageProofVerificationRecord record) {
        return new DiffPageProofVerification(
                record.getValid(),
                record.getBaseValid(),
                record.getOtherValid(),
                record.getLookaheadValid(),
                record.getBaseRoot(),
                record.getOtherRoot(),
                record.getAfter(),
                record.getRequestedEnd(),
                record.getProofEnd(),
                ProllyJavaAdapters.diffPageProofVerificationLimit(record),
                record.getDiffs(),
                record.getNextCursor());
    }

    @Override
    public byte[] baseRoot() {
        return baseRoot == null ? null : baseRoot.clone();
    }

    @Override
    public byte[] otherRoot() {
        return otherRoot == null ? null : otherRoot.clone();
    }

    @Override
    public byte[] after() {
        return after == null ? null : after.clone();
    }

    @Override
    public byte[] requestedEnd() {
        return requestedEnd == null ? null : requestedEnd.clone();
    }

    @Override
    public byte[] proofEnd() {
        return proofEnd == null ? null : proofEnd.clone();
    }

    @Override
    public List<DiffRecord> diffs() {
        return cloneDiffs(diffs);
    }

    @Override
    public RangeCursorRecord nextCursor() {
        return cloneCursor(nextCursor);
    }

    private static List<DiffRecord> cloneDiffs(List<DiffRecord> diffs) {
        List<DiffRecord> cloned = new ArrayList<>(diffs.size());
        for (DiffRecord diff : diffs) {
            byte[] value = diff.getValue();
            byte[] oldValue = diff.getOldValue();
            byte[] newValue = diff.getNewValue();
            cloned.add(new DiffRecord(
                    diff.getKind(),
                    diff.getKey().clone(),
                    value == null ? null : value.clone(),
                    oldValue == null ? null : oldValue.clone(),
                    newValue == null ? null : newValue.clone()));
        }
        return List.copyOf(cloned);
    }

    private static RangeCursorRecord cloneCursor(RangeCursorRecord cursor) {
        return cursor == null ? null : new RangeCursorRecord(cursor.getAfterKey().clone());
    }
}
