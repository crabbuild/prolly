package build.crab.prolly;

public final class TransactionUpdate {
    private final boolean applied;
    private final boolean conflict;
    private final long nodesWritten;
    private final long rootsWritten;
    private final TransactionConflict conflictDetail;

    TransactionUpdate(TransactionUpdateRecord record) {
        this.applied = record.getApplied();
        this.conflict = record.getConflict();
        this.nodesWritten = ProllyJavaAdapters.transactionUpdateNodesWritten(record);
        this.rootsWritten = ProllyJavaAdapters.transactionUpdateRootsWritten(record);
        TransactionConflictRecord detail = record.getConflictDetail();
        this.conflictDetail = detail == null ? null : TransactionConflict.fromRecord(detail);
    }

    public boolean applied() {
        return applied;
    }

    public boolean conflict() {
        return conflict;
    }

    public long nodesWritten() {
        return nodesWritten;
    }

    public long rootsWritten() {
        return rootsWritten;
    }

    public TransactionConflict conflictDetail() {
        return conflictDetail;
    }
}
