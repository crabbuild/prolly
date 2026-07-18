package build.crab.prolly.store.spanner;

import com.google.cloud.spanner.DatabaseClient;
import java.util.Objects;
import java.util.concurrent.Executor;

/** Java entry points for the shared Kotlin/Google Cloud Spanner adapter. */
public final class SpannerStores {
    private SpannerStores() {}

    /** Creates an adapter over a caller-owned database client and executor. */
    public static SpannerStore from(DatabaseClient database, Executor executor) {
        return SpannerStore.fromExecutor(
                Objects.requireNonNull(database, "database"),
                Objects.requireNonNull(executor, "executor"));
    }
}
