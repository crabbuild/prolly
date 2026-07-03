package build.crab.prolly;

public record DiffPageProof(
        RangePageProof base,
        RangePageProof other,
        KeyProof lookaheadBase,
        KeyProof lookaheadOther,
        byte[] requestedEnd,
        long limit) {
    public DiffPageProof {
        requestedEnd = requestedEnd == null ? null : requestedEnd.clone();
    }

    static DiffPageProof fromRecord(DiffPageProofRecord record) throws ProllyBindingException {
        KeyProofRecord baseLookahead = record.getLookaheadBase();
        KeyProofRecord otherLookahead = record.getLookaheadOther();
        return new DiffPageProof(
                RangePageProof.fromRecord(record.getBase()),
                RangePageProof.fromRecord(record.getOther()),
                baseLookahead == null ? null : KeyProof.fromRecord(baseLookahead),
                otherLookahead == null ? null : KeyProof.fromRecord(otherLookahead),
                record.getRequestedEnd(),
                ProllyJavaAdapters.diffPageProofLimit(record));
    }

    DiffPageProofRecord toRecord() throws ProllyBindingException {
        return ProllyJavaAdapters.diffPageProofRecord(
                base.toRecord(),
                other.toRecord(),
                lookaheadBase == null ? null : lookaheadBase.toRecord(),
                lookaheadOther == null ? null : lookaheadOther.toRecord(),
                requestedEnd == null ? null : requestedEnd.clone(),
                limit);
    }

    @Override
    public byte[] requestedEnd() {
        return requestedEnd == null ? null : requestedEnd.clone();
    }
}
