package build.crab.prolly.javaapi;

import java.nio.file.Path;

/** A content-addressed large-value store owned by the portable Java API. */
public final class BlobStore implements AutoCloseable {
    private build.crab.prolly.ProllyBlobStore nativeStore;

    private BlobStore(build.crab.prolly.ProllyBlobStore nativeStore) {
        this.nativeStore = nativeStore;
    }

    public static BlobStore memory() {
        return new BlobStore(build.crab.prolly.ProllyBlobStore.Companion.memory());
    }

    public static BlobStore file(Path path) throws build.crab.prolly.ProllyBindingException {
        return new BlobStore(build.crab.prolly.ProllyBlobStore.Companion.file(path.toString()));
    }

    build.crab.prolly.ProllyBlobStore nativeHandle() {
        if (nativeStore == null) throw new IllegalStateException("blob store is closed");
        return nativeStore;
    }

    build.crab.prolly.ProllyBlobStore cloneNativeHandle() {
        return build.crab.prolly.api.JavaPortableBridge.cloneBlobStore(nativeHandle());
    }

    @Override
    public void close() {
        if (nativeStore != null) {
            nativeStore.close();
            nativeStore = null;
        }
    }
}
