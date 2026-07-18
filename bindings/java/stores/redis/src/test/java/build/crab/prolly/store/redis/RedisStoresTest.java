package build.crab.prolly.store.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import build.crab.prolly.Prolly;
import build.crab.prolly.javaapi.remote.RemoteProlly;
import build.crab.prolly.remote.RemoteStore;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class RedisStoresTest {
    @Test
    void javaFactoryDrivesTheBorrowedLettuceAdapter() throws Exception {
        var url = System.getenv("PROLLY_REDIS_URL");
        assumeTrue(url != null && !url.isBlank(), "PROLLY_REDIS_URL is not set");
        Prolly.useLocalDebugLibrary();
        var client = RedisClient.create(url);
        var connection = client.connect(ByteArrayCodec.INSTANCE);
        var commands = connection.async();
        var executor = Executors.newFixedThreadPool(4);
        var prefix = ("prolly:test:java:" + UUID.randomUUID() + ":").getBytes(StandardCharsets.UTF_8);
        var store = RedisStores.from(commands, prefix);
        try {
            try (var engine = RemoteProlly.open((RemoteStore) store, executor).get(10, TimeUnit.SECONDS)) {
                var tree = engine.create();
                tree = engine.put(tree, "key".getBytes(), "value".getBytes()).get(10, TimeUnit.SECONDS);
                assertEquals("value", new String(engine.get(tree, "key".getBytes()).get(10, TimeUnit.SECONDS)));
            }
            store.close();
            assertEquals("PONG", commands.ping().get(10, TimeUnit.SECONDS));
        } finally {
            store.close(); connection.close(); client.shutdown(); executor.shutdownNow();
        }
    }
}
