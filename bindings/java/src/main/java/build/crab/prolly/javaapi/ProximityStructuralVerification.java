package build.crab.prolly.javaapi;

public record ProximityStructuralVerification(
        byte[] descriptor,
        long objectCount,
        ProximityVerification summary) {
    public ProximityStructuralVerification {
        descriptor = descriptor.clone();
    }
    @Override public byte[] descriptor() { return descriptor.clone(); }
}
