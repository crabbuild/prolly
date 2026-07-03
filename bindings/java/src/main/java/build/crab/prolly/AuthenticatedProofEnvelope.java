package build.crab.prolly;

public record AuthenticatedProofEnvelope(
        String algorithm,
        byte[] keyId,
        byte[] proofBundle,
        byte[] context,
        Long issuedAtMillis,
        Long expiresAtMillis,
        byte[] nonce,
        byte[] signature) {
    public AuthenticatedProofEnvelope {
        keyId = keyId.clone();
        proofBundle = proofBundle.clone();
        context = context.clone();
        nonce = nonce.clone();
        signature = signature.clone();
    }

    static AuthenticatedProofEnvelope fromRecord(AuthenticatedProofEnvelopeRecord record) {
        return new AuthenticatedProofEnvelope(
                record.getAlgorithm(),
                record.getKeyId(),
                record.getProofBundle(),
                record.getContext(),
                ProllyJavaAdapters.authenticatedProofEnvelopeIssuedAtMillis(record),
                ProllyJavaAdapters.authenticatedProofEnvelopeExpiresAtMillis(record),
                record.getNonce(),
                record.getSignature());
    }

    AuthenticatedProofEnvelopeRecord toRecord() {
        return ProllyJavaAdapters.authenticatedProofEnvelopeRecord(
                algorithm,
                keyId,
                proofBundle,
                context,
                issuedAtMillis,
                expiresAtMillis,
                nonce,
                signature);
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

    @Override
    public byte[] signature() {
        return signature.clone();
    }
}
