package build.crab.prolly.store.mysql;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.sql.DataSource;

/** Java entry points for the shared Kotlin/JDBC MySQL adapter. */
public final class MysqlStores {
    private MysqlStores() {}

    /** Creates an adapter over caller-owned inputs. */
    public static MysqlStore from(DataSource dataSource, Executor executor) {
        return new MysqlStore(Objects.requireNonNull(dataSource, "dataSource"), Objects.requireNonNull(executor, "executor"));
    }

    /** Initializes the three protocol tables asynchronously with cancellation propagation. */
    public static CompletableFuture<Void> initializeSchema(MysqlStore store) {
        Objects.requireNonNull(store, "store");
        var source = store.initializeSchemaAsync();
        var result = new CompletableFuture<Void>();
        source.whenComplete((ignored, failure) -> { if (failure == null) result.complete(null); else result.completeExceptionally(failure); });
        result.whenComplete((ignored, failure) -> { if (result.isCancelled()) source.cancel(true); });
        return result;
    }
}
