package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record RangeProof(byte[] root, byte[] start, byte[] end, List<byte[]> pathNodeBytes) {
    public RangeProof {
        root = root == null ? null : root.clone();
        start = start.clone();
        end = end == null ? null : end.clone();
        pathNodeBytes = cloneByteArrays(pathNodeBytes);
    }

    static RangeProof fromRecord(RangeProofRecord record) throws ProllyBindingException {
        return new RangeProof(
                record.getRoot(),
                record.getStart(),
                record.getEnd(),
                ProllyJavaAdapters.rangeProofPathNodeBytes(record));
    }

    RangeProofRecord toRecord() throws ProllyBindingException {
        return ProllyJavaAdapters.rangeProofFromNodeBytes(root, start, end, pathNodeBytes);
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
