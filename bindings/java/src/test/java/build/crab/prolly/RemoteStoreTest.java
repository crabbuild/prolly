package build.crab.prolly;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import build.crab.prolly.javaapi.remote.RemoteProlly;
import build.crab.prolly.storetest.MemoryRemoteStore;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RemoteStoreTest {
    @Test
    void futuresDriveTheSharedKotlinStoreAndPropagateCancellation() throws Exception {
        Prolly.useLocalDebugLibrary();
        MemoryRemoteStore store = new MemoryRemoteStore();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        TreeRecord tree;
        try (RemoteProlly writer = RemoteProlly.open(store, executor).get(10, TimeUnit.SECONDS)) {
            tree = writer.put(writer.create(), bytes("key"), bytes("value")).get(10, TimeUnit.SECONDS);
        }
        try (RemoteProlly engine = RemoteProlly.open(store, executor).get(10, TimeUnit.SECONDS)) {
            store.blockReads();
            CompletableFuture<byte[]> pending = engine.get(tree, bytes("key"));
            store.readStarted().get(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
            assertTrue(pending.cancel(true));
            assertThrows(CancellationException.class, pending::join);
            assertTrue(store.readCancelled().get(10, TimeUnit.SECONDS));
            assertArrayEquals(bytes("value"), engine.get(tree, bytes("key")).get(10, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
