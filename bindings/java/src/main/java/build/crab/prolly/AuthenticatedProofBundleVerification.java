package build.crab.prolly;

public record AuthenticatedProofBundleVerification(
        boolean valid,
        AuthenticatedProofEnvelopeVerification envelope,
        ProofBundleVerification proof,
        String proofError) {
    static AuthenticatedProofBundleVerification fromRecord(
            AuthenticatedProofBundleVerificationRecord record) {
        ProofBundleVerification proof = record.getProof() == null
                ? null
                : ProofBundleVerification.fromRecord(record.getProof());
        return new AuthenticatedProofBundleVerification(
                record.getValid(),
                AuthenticatedProofEnvelopeVerification.fromRecord(record.getEnvelope()),
                proof,
                record.getProofError());
    }
}
