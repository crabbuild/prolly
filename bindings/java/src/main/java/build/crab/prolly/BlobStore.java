package build.crab.prolly;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class BlobStore implements AutoCloseable {
    private final ProllyBlobStore store;

    private BlobStore(ProllyBlobStore store) {
        this.store = store;
    }

    public static BlobStore memory() {
        return new BlobStore(ProllyBlobStore.Companion.memory());
    }

    public static BlobStore file(Path path) throws ProllyBindingException {
        return new BlobStore(ProllyBlobStore.Companion.file(path.toString()));
    }

    ProllyBlobStore inner() {
        return store;
    }

    public BlobRef putBlob(byte[] bytes) throws ProllyBindingException {
        return new BlobRef(store.putBlob(bytes.clone()));
    }

    public Optional<byte[]> getBlob(BlobRef reference) throws ProllyBindingException {
        byte[] value = store.getBlob(reference.toRecord());
        return value == null ? Optional.empty() : Optional.of(value.clone());
    }

    public void deleteBlob(BlobRef reference) throws ProllyBindingException {
        store.deleteBlob(reference.toRecord());
    }

    public List<BlobRef> listBlobRefs() throws ProllyBindingException {
        List<BlobRefRecord> records = store.listBlobRefs();
        List<BlobRef> refs = new ArrayList<>(records.size());
        for (BlobRefRecord record : records) {
            refs.add(new BlobRef(record));
        }
        return List.copyOf(refs);
    }

    public long blobCount() throws ProllyBindingException {
        return ProllyJavaAdapters.blobStoreBlobCount(store);
    }

    @Override
    public void close() {
        store.close();
    }
}
