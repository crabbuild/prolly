package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record RangeProofVerification(
        boolean valid,
        byte[] root,
        byte[] start,
        byte[] end,
        List<Entry> entries) {
    public RangeProofVerification {
        root = root == null ? null : root.clone();
        start = start.clone();
        end = end == null ? null : end.clone();
        entries = List.copyOf(entries);
    }

    static RangeProofVerification fromRecord(RangeProofVerificationRecord record) {
        List<Entry> entries = new ArrayList<>(record.getEntries().size());
        for (EntryRecord entry : record.getEntries()) {
            entries.add(new Entry(entry.getKey(), entry.getValue()));
        }
        return new RangeProofVerification(
                record.getValid(),
                record.getRoot(),
                record.getStart(),
                record.getEnd(),
                entries);
    }

    @Override
    public byte[] root() {
        return root == null ? null : root.clone();
    }

    @Override
    public byte[] start() {
        return start.clone();
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
