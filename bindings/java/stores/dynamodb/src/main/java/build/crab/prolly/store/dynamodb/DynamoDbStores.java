package build.crab.prolly.store.dynamodb;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

/** Java entry points for the shared Kotlin/AWS SDK DynamoDB adapter. */
public final class DynamoDbStores {
    private DynamoDbStores() {}

    /** Creates an adapter over a caller-owned asynchronous AWS client. */
    public static DynamoDbStore from(DynamoDbAsyncClient client, String tableName, byte[] keyPrefix) {
        return new DynamoDbStore(Objects.requireNonNull(client, "client"), Objects.requireNonNull(tableName, "tableName"), Objects.requireNonNull(keyPrefix, "keyPrefix").clone());
    }

    /** Creates or validates the required binary-key table. */
    public static CompletableFuture<Void> initializeTable(DynamoDbStore store) {
        Objects.requireNonNull(store, "store");
        var source = store.initializeTableAsync();
        var result = new CompletableFuture<Void>();
        source.whenComplete((ignored, failure) -> { if (failure == null) result.complete(null); else result.completeExceptionally(failure); });
        result.whenComplete((ignored, failure) -> { if (result.isCancelled()) source.cancel(true); });
        return result;
    }
}
