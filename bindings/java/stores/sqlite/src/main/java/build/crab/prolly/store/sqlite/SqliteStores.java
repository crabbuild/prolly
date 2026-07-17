package build.crab.prolly.store.sqlite;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.sql.DataSource;

/** Java entry points for the shared Kotlin SQLite adapter. */
public final class SqliteStores {
    private SqliteStores() {}

    public static SqliteStore from(DataSource dataSource, Executor executor) {
        return new SqliteStore(
                Objects.requireNonNull(dataSource, "dataSource"),
                Objects.requireNonNull(executor, "executor"));
    }

    public static CompletableFuture<Void> initializeSchema(SqliteStore store) {
        Objects.requireNonNull(store, "store");
        var source = store.initializeSchemaAsync();
        var result = new CompletableFuture<Void>();
        source.whenComplete((ignored, error) -> {
            if (error == null) result.complete(null);
            else result.completeExceptionally(error);
        });
        result.whenComplete((ignored, error) -> {
            if (result.isCancelled()) source.cancel(true);
        });
        return result;
    }
}
