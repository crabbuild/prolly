package build.crab.prolly.store.sqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import build.crab.prolly.Prolly;
import build.crab.prolly.javaapi.remote.RemoteProlly;
import build.crab.prolly.remote.RemoteStore;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.sqlite.SQLiteDataSource;

final class SqliteStoresTest {
    @Test
    void javaFactoryInitializesAndDrivesTheKotlinAdapterWithoutOwningInputs() throws Exception {
        Prolly.useLocalDebugLibrary();
        var path = Files.createTempFile("prolly-java-sqlite-", ".db");
        var dataSource = new SQLiteDataSource();
        dataSource.setUrl("jdbc:sqlite:" + path);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        var store = SqliteStores.from(dataSource, executor);
        try {
            SqliteStores.initializeSchema(store).get(10, TimeUnit.SECONDS);
            try (var engine = RemoteProlly.open((RemoteStore) store, executor).get(10, TimeUnit.SECONDS)) {
                var tree = engine.create();
                tree = engine.put(tree, "key".getBytes(), "value".getBytes()).get(10, TimeUnit.SECONDS);
                assertEquals("value", new String(engine.get(tree, "key".getBytes()).get(10, TimeUnit.SECONDS)));
            }
            store.close();
            try (var connection = dataSource.getConnection();
                 var statement = connection.createStatement();
                 var rows = statement.executeQuery("SELECT 1")) {
                assertFalse(rows.isClosed());
                rows.next();
                assertEquals(1, rows.getInt(1));
            }
        } finally {
            executor.shutdownNow();
            Files.deleteIfExists(path);
        }
    }
}
