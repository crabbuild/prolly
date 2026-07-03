package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record RangePageProofVerification(
        boolean valid,
        byte[] root,
        byte[] after,
        byte[] end,
        List<Entry> entries) {
    public RangePageProofVerification {
        root = root == null ? null : root.clone();
        after = after == null ? null : after.clone();
        end = end == null ? null : end.clone();
        entries = List.copyOf(entries);
    }

    static RangePageProofVerification fromRecord(RangePageProofVerificationRecord record) {
        List<Entry> entries = new ArrayList<>(record.getEntries().size());
        for (EntryRecord entry : record.getEntries()) {
            entries.add(new Entry(entry.getKey(), entry.getValue()));
        }
        return new RangePageProofVerification(
                record.getValid(),
                record.getRoot(),
                record.getAfter(),
                record.getEnd(),
                entries);
    }

    @Override
    public byte[] root() {
        return root == null ? null : root.clone();
    }

    @Override
    public byte[] after() {
        return after == null ? null : after.clone();
    }

    @Override
    public byte[] end() {
        return end == null ? null : end.clone();
    }

    @Override
    public List<Entry> entries() {
        return entries;
    }
}
