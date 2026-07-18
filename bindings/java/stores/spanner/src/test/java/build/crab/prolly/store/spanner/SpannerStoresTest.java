package build.crab.prolly.store.spanner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.cloud.spanner.DatabaseClient;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class SpannerStoresTest {
    @Test
    void javaFactoryBorrowsDatabaseAndExecutor() throws Exception {
        var database = mock(DatabaseClient.class);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var store = SpannerStores.from(database, executor);
            assertEquals("SpannerStore", store.getClass().getSimpleName());
            store.close();
            assertEquals("still-owned", executor.submit(() -> "still-owned").get(5, TimeUnit.SECONDS));
            verifyNoInteractions(database);
        } finally {
            executor.shutdownNow();
        }
    }
}
