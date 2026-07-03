package build.crab.prolly;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public final class AsyncBlobStore implements AutoCloseable {
    private final BlobStore inner;
    private final Executor executor;

    private AsyncBlobStore(BlobStore inner, Executor executor) {
        this.inner = inner;
        this.executor = executor;
    }

    public static CompletableFuture<AsyncBlobStore> memory() {
        return memory(ForkJoinPool.commonPool());
    }

    public static CompletableFuture<AsyncBlobStore> memory(Executor executor) {
        return CompletableFuture.supplyAsync(() -> new AsyncBlobStore(BlobStore.memory(), executor), executor);
    }

    public static CompletableFuture<AsyncBlobStore> file(Path path) {
        return file(path, ForkJoinPool.commonPool());
    }

    public static CompletableFuture<AsyncBlobStore> file(Path path, Executor executor) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return new AsyncBlobStore(BlobStore.file(path), executor);
            } catch (ProllyBindingException exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    public static AsyncBlobStore wrap(BlobStore blobStore) {
        return wrap(blobStore, ForkJoinPool.commonPool());
    }

    public static AsyncBlobStore wrap(BlobStore blobStore, Executor executor) {
        return new AsyncBlobStore(blobStore, executor);
    }

    BlobStore inner() {
        return inner;
    }

    public CompletableFuture<BlobRef> putBlob(byte[] bytes) {
        return supply(() -> inner.putBlob(bytes));
    }

    public CompletableFuture<Optional<byte[]>> getBlob(BlobRef reference) {
        return supply(() -> inner.getBlob(reference));
    }

    public CompletableFuture<Void> deleteBlob(BlobRef reference) {
        return run(() -> inner.deleteBlob(reference));
    }

    public CompletableFuture<List<BlobRef>> listBlobRefs() {
        return supply(inner::listBlobRefs);
    }

    public CompletableFuture<Long> blobCount() {
        return supply(inner::blobCount);
    }

    @Override
    public void close() {
        inner.close();
    }

    private CompletableFuture<Void> run(ThrowingRunnable runnable) {
        return CompletableFuture.runAsync(() -> {
            try {
                runnable.run();
            } catch (Exception exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    private <T> CompletableFuture<T> supply(ThrowingSupplier<T> supplier) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return supplier.get();
            } catch (Exception exception) {
                throw new CompletionException(exception);
            }
        }, executor);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
