package build.crab.prolly;

public record TransactionConflict(
        byte[] name,
        RootManifest expected,
        RootManifest current) {
    public TransactionConflict {
        name = name.clone();
    }

    static TransactionConflict fromRecord(TransactionConflictRecord record) {
        return new TransactionConflict(
                record.getName(),
                manifest(record.getExpected()),
                manifest(record.getCurrent()));
    }

    @Override
    public byte[] name() {
        return name.clone();
    }

    private static RootManifest manifest(RootManifestRecord record) {
        if (record == null) {
            return null;
        }
        return new RootManifest(
                record.getTree(),
                ProllyJavaAdapters.rootManifestCreatedAtMillis(record),
                ProllyJavaAdapters.rootManifestUpdatedAtMillis(record));
    }
}
