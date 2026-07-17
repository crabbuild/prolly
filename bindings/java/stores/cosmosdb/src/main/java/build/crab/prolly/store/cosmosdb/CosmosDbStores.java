package build.crab.prolly.store.cosmosdb;

import com.azure.cosmos.CosmosAsyncContainer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Java entry points for the shared Kotlin/Azure SDK Cosmos DB adapter. */
public final class CosmosDbStores {
    private CosmosDbStores() {}

    /** Creates an adapter over a caller-owned container using the required {@code /kind} partition key. */
    public static CosmosDbStore from(CosmosAsyncContainer container, String partitionKey, byte[] keyPrefix) {
        return new CosmosDbStore(
                Objects.requireNonNull(container, "container"),
                Objects.requireNonNull(partitionKey, "partitionKey"),
                Objects.requireNonNull(keyPrefix, "keyPrefix").clone());
    }

    /** Validates that the caller-owned container is partitioned by {@code /kind}. */
    public static CompletableFuture<Void> validateContainer(CosmosDbStore store) {
        Objects.requireNonNull(store, "store");
        var source = store.validateContainerAsync();
        var result = new CompletableFuture<Void>();
        source.whenComplete((ignored, failure) -> {
            if (failure == null) result.complete(null);
            else result.completeExceptionally(failure);
        });
        result.whenComplete((ignored, failure) -> {
            if (result.isCancelled()) source.cancel(true);
        });
        return result;
    }
}
