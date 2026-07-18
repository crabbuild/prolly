package build.crab.prolly.store.redis;

import io.lettuce.core.api.async.RedisAsyncCommands;
import java.util.Objects;

/** Java entry points for the shared Kotlin/Lettuce Redis adapter. */
public final class RedisStores {
    private RedisStores() {}

    /** Creates an adapter over caller-owned binary Lettuce commands. */
    public static RedisStore from(RedisAsyncCommands<byte[], byte[]> commands) {
        return new RedisStore(Objects.requireNonNull(commands, "commands"));
    }

    /** Creates a namespaced adapter over caller-owned binary Lettuce commands. */
    public static RedisStore from(RedisAsyncCommands<byte[], byte[]> commands, byte[] keyPrefix) {
        Objects.requireNonNull(commands, "commands");
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        return new RedisStore(commands, keyPrefix.clone());
    }
}
