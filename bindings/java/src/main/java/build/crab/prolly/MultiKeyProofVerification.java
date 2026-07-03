package build.crab.prolly;

import java.util.ArrayList;
import java.util.List;

public record MultiKeyProofVerification(
        boolean valid,
        byte[] root,
        List<KeyProofVerification> results) {
    public MultiKeyProofVerification {
        root = root == null ? null : root.clone();
        results = List.copyOf(results);
    }

    static MultiKeyProofVerification fromRecord(MultiKeyProofVerificationRecord record) {
        List<KeyProofVerification> results = new ArrayList<>(record.getResults().size());
        for (KeyProofVerificationRecord result : record.getResults()) {
            results.add(KeyProofVerification.fromRecord(result));
        }
        return new MultiKeyProofVerification(record.getValid(), record.getRoot(), results);
    }

    @Override
    public byte[] root() {
        return root == null ? null : root.clone();
    }

    @Override
    public List<KeyProofVerification> results() {
        return results;
    }
}
