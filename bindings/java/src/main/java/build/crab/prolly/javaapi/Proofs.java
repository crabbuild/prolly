package build.crab.prolly.javaapi;

public final class Proofs {
    private Proofs() {}

    public static build.crab.prolly.KeyProofVerificationRecord verify(
            build.crab.prolly.KeyProofRecord proof) {
        return build.crab.prolly.api.JavaPortableBridge.verify(proof);
    }

    public static build.crab.prolly.ProximityMembershipVerificationRecord verify(
            build.crab.prolly.ProximityMembershipProofRecord proof,
            byte[] expectedDescriptor) {
        return build.crab.prolly.api.JavaPortableBridge.verify(
                proof, expectedDescriptor == null ? null : expectedDescriptor.clone());
    }
}
