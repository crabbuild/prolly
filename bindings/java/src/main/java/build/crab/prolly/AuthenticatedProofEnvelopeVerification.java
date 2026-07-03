package build.crab.prolly;

public record AuthenticatedProofEnvelopeVerification(
        boolean valid,
        boolean signatureValid,
        boolean timeValid,
        boolean notYetValid,
        boolean expired,
        String algorithm,
        byte[] keyId,
        byte[] proofBundle,
        byte[] context,
        Long issuedAtMillis,
        Long expiresAtMillis,
        byte[] nonce) {
    public AuthenticatedProofEnvelopeVerification {
        keyId = keyId.clone();
        proofBundle = proofBundle.clone();
        context = context.clone();
        nonce = nonce.clone();
    }

    static AuthenticatedProofEnvelopeVerification fromRecord(
            AuthenticatedProofEnvelopeVerificationRecord record) {
        return new AuthenticatedProofEnvelopeVerification(
                record.getValid(),
                record.getSignatureValid(),
                record.getTimeValid(),
                record.getNotYetValid(),
                record.getExpired(),
                record.getAlgorithm(),
                record.getKeyId(),
                record.getProofBundle(),
                record.getContext(),
                ProllyJavaAdapters.authenticatedProofEnvelopeVerificationIssuedAtMillis(record),
                ProllyJavaAdapters.authenticatedProofEnvelopeVerificationExpiresAtMillis(record),
                record.getNonce());
    }

    @Override
    public byte[] keyId() {
        return keyId.clone();
    }

    @Override
    public byte[] proofBundle() {
        return proofBundle.clone();
    }

    @Override
    public byte[] context() {
        return context.clone();
    }

    @Override
    public byte[] nonce() {
        return nonce.clone();
    }
}
