package build.crab.prolly.store.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import build.crab.prolly.Prolly;
import build.crab.prolly.javaapi.remote.RemoteProlly;
import build.crab.prolly.remote.RemoteStore;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

final class PostgresStoresTest {
    @Test
    void javaFactoryInitializesAndDrivesTheBorrowedKotlinAdapter() throws Exception {
        var url = System.getenv("PROLLY_POSTGRES_URL");
        assumeTrue(url != null && !url.isBlank(), "PROLLY_POSTGRES_URL is not set");
        Prolly.useLocalDebugLibrary();
        var dataSource = dataSource(url);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        var store = PostgresStores.from(dataSource, executor);
        try {
            PostgresStores.initializeSchema(store).get(10, TimeUnit.SECONDS);
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("TRUNCATE prolly_nodes, prolly_hints, prolly_roots");
            }
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
        }
    }

    private static PGSimpleDataSource dataSource(String url) throws Exception {
        var uri = new URI(url);
        var credentials = uri.getUserInfo().split(":", 2);
        var dataSource = new PGSimpleDataSource();
        dataSource.setURL("jdbc:postgresql://" + uri.getHost() + ":" + uri.getPort() + uri.getPath() + "?" + (uri.getQuery() == null ? "" : uri.getQuery()));
        dataSource.setUser(credentials[0]);
        dataSource.setPassword(credentials.length == 2 ? credentials[1] : "");
        return dataSource;
    }
}
