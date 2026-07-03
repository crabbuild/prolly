package build.crab.prolly;

public record KeyProofVerification(
        boolean valid,
        boolean exists,
        boolean absence,
        byte[] root,
        byte[] key,
        byte[] value) {
    public KeyProofVerification {
        root = root == null ? null : root.clone();
        key = key.clone();
        value = value == null ? null : value.clone();
    }

    static KeyProofVerification fromRecord(KeyProofVerificationRecord record) {
        return new KeyProofVerification(
                record.getValid(),
                record.getExists(),
                record.getAbsence(),
                record.getRoot(),
                record.getKey(),
                record.getValue());
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
    public byte[] value() {
        return value == null ? null : value.clone();
    }
}
