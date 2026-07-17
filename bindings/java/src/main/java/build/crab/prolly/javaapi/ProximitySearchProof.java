package build.crab.prolly.javaapi;

import build.crab.prolly.ProximitySearchVerificationRecord;
import build.crab.prolly.api.JavaPortableBridge;

public final class ProximitySearchProof implements AutoCloseable {
    private build.crab.prolly.api.ProximitySearchProof nativeProof;

    ProximitySearchProof(build.crab.prolly.api.ProximitySearchProof nativeProof) {
        this.nativeProof = nativeProof;
    }

    private build.crab.prolly.api.ProximitySearchProof open() {
        if (nativeProof == null) throw new IllegalStateException("proximity search proof is closed");
        return nativeProof;
    }

    public byte[] sourceDescriptor() { return open().getSourceDescriptor().clone(); }

    public ProximitySearchVerificationRecord verify(byte[] expectedDescriptor) {
        return JavaPortableBridge.verify(
                open(), expectedDescriptor == null ? null : expectedDescriptor.clone());
    }

    public long replayedEvents(ProximitySearchVerificationRecord verification) {
        return JavaPortableBridge.replayedEvents(verification);
    }

    @Override public void close() {
        if (nativeProof != null) {
            nativeProof.close();
            nativeProof = null;
        }
    }
}
