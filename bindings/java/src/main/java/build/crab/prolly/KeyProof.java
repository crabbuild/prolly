package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record KeyProof(byte[] root, byte[] key, List<byte[]> pathNodeBytes) {
    public KeyProof {
        root = root == null ? null : root.clone();
        key = key.clone();
        pathNodeBytes = cloneByteArrays(pathNodeBytes);
    }

    static KeyProof fromRecord(KeyProofRecord record) throws ProllyBindingException {
        return new KeyProof(
                record.getRoot(),
                record.getKey(),
                ProllyJavaAdapters.keyProofPathNodeBytes(record));
    }

    KeyProofRecord toRecord() throws ProllyBindingException {
        return ProllyJavaAdapters.keyProofFromNodeBytes(root, key, pathNodeBytes);
    }

    @Override
    public byte[] root() {
        return root == null ? null : root.clone();
    }

    @Override
    public byte[] key() {
        return key.clone();
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
