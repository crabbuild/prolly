package build.crab.prolly.store.postgres;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.sql.DataSource;

/** Java entry points for the shared Kotlin/JDBC PostgreSQL adapter. */
public final class PostgresStores {
    private PostgresStores() {}

    /** Creates an adapter over caller-owned inputs. Closing it does not close either input. */
    public static PostgresStore from(DataSource dataSource, Executor executor) {
        return new PostgresStore(
                Objects.requireNonNull(dataSource, "dataSource"),
                Objects.requireNonNull(executor, "executor"));
    }

    /** Initializes the three protocol tables asynchronously. */
    public static CompletableFuture<Void> initializeSchema(PostgresStore store) {
        Objects.requireNonNull(store, "store");
        var source = store.initializeSchemaAsync();
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
