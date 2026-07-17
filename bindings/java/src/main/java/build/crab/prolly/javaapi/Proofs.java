package build.crab.prolly.javaapi;

public final class Proofs {
    private Proofs() {}

    public static build.crab.prolly.KeyProofVerificationRecord verify(
            build.crab.prolly.KeyProofRecord proof) {
        return build.crab.prolly.api.JavaPortableBridge.verify(proof);
    }

    public static build.crab.prolly.MultiKeyProofVerificationRecord verify(
            build.crab.prolly.MultiKeyProofRecord proof) {
        return build.crab.prolly.api.JavaPortableBridge.verifyMultiKey(proof);
    }

    public static build.crab.prolly.RangeProofVerificationRecord verify(
            build.crab.prolly.RangeProofRecord proof) {
        return build.crab.prolly.api.JavaPortableBridge.verifyRange(proof);
    }

    public static build.crab.prolly.RangePageProofVerificationRecord verify(
            build.crab.prolly.RangePageProofRecord proof) {
        return build.crab.prolly.api.JavaPortableBridge.verifyRangePage(proof);
    }

    public static build.crab.prolly.ProximityMembershipVerificationRecord verify(
            build.crab.prolly.ProximityMembershipProofRecord proof,
            byte[] expectedDescriptor) {
        return build.crab.prolly.api.JavaPortableBridge.verify(
                proof, expectedDescriptor == null ? null : expectedDescriptor.clone());
    }

    public static ProximityStructuralVerification verify(
            build.crab.prolly.ProximityStructuralProofRecord proof,
            byte[] expectedDescriptor) {
        var value = build.crab.prolly.api.JavaPortableBridge.verifyStructural(
                proof, expectedDescriptor == null ? null : expectedDescriptor.clone());
        var summary = value.getSummary();
        return new ProximityStructuralVerification(
                value.getDescriptor(), value.getObjectCount(), new ProximityVerification(
                        summary.getRecordCount(), summary.getProximityNodeCount(),
                        summary.getExternalVectorCount(), summary.getQuantizedNodeCount(),
                        summary.getScalarQuantizerCount(), summary.getOverflowPageCount(),
                        summary.getOverflowDirectoryCount(), summary.getMaximumLevel(),
                        summary.getMaximumNodeBytes(), summary.getDistanceChecks()));
    }
}
