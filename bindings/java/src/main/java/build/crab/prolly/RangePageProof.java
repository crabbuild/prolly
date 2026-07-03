package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record RangePageProof(byte[] root, byte[] after, byte[] end, List<byte[]> pathNodeBytes) {
    public RangePageProof {
        root = root == null ? null : root.clone();
        after = after == null ? null : after.clone();
        end = end == null ? null : end.clone();
        pathNodeBytes = cloneByteArrays(pathNodeBytes);
    }

    static RangePageProof fromRecord(RangePageProofRecord record) throws ProllyBindingException {
        return new RangePageProof(
                record.getRoot(),
                record.getAfter(),
                record.getEnd(),
                ProllyJavaAdapters.rangePageProofPathNodeBytes(record));
    }

    RangePageProofRecord toRecord() throws ProllyBindingException {
        return ProllyJavaAdapters.rangePageProofFromNodeBytes(root, after, end, pathNodeBytes);
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
    public List<byte[]> pathNodeBytes() {
        return cloneByteArrays(pathNodeBytes);
    }

    private static List<byte[]> cloneByteArrays(List<byte[]> values) {
        List<byte[]> clones = new ArrayList<>(values.size());
        for (byte[] value : values) {
            clones.add(value.clone());
        }
        return List.copyOf(clones);
    }
}
