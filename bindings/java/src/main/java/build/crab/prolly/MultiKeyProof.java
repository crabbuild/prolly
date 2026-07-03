package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record MultiKeyProof(byte[] root, List<byte[]> keys, List<byte[]> pathNodeBytes) {
    public MultiKeyProof {
        root = root == null ? null : root.clone();
        keys = cloneByteArrays(keys);
        pathNodeBytes = cloneByteArrays(pathNodeBytes);
    }

    static MultiKeyProof fromRecord(MultiKeyProofRecord record) throws ProllyBindingException {
        return new MultiKeyProof(
                record.getRoot(),
                record.getKeys(),
                ProllyJavaAdapters.multiKeyProofPathNodeBytes(record));
    }

    MultiKeyProofRecord toRecord() throws ProllyBindingException {
        return ProllyJavaAdapters.multiKeyProofFromNodeBytes(root, keys, pathNodeBytes);
    }

    @Override
    public byte[] root() {
        return root == null ? null : root.clone();
    }

    @Override
    public List<byte[]> keys() {
        return cloneByteArrays(keys);
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
