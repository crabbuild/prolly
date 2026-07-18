package build.crab.prolly.store.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import build.crab.prolly.Prolly;
import build.crab.prolly.javaapi.remote.RemoteProlly;
import build.crab.prolly.remote.RemoteStore;
import com.mysql.cj.jdbc.MysqlDataSource;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class MysqlStoresTest {
    @Test
    void javaFactoryInitializesAndDrivesTheBorrowedKotlinAdapter() throws Exception {
        var url = System.getenv("PROLLY_MYSQL_URL");
        assumeTrue(url != null && !url.isBlank(), "PROLLY_MYSQL_URL is not set");
        Prolly.useLocalDebugLibrary();
        var dataSource = dataSource(url);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        var store = MysqlStores.from(dataSource, executor);
        try {
            MysqlStores.initializeSchema(store).get(10, TimeUnit.SECONDS);
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.executeUpdate("TRUNCATE prolly_nodes"); statement.executeUpdate("TRUNCATE prolly_hints"); statement.executeUpdate("TRUNCATE prolly_roots");
            }
            try (var engine = RemoteProlly.open((RemoteStore) store, executor).get(10, TimeUnit.SECONDS)) {
                var tree = engine.create(); tree = engine.put(tree, "key".getBytes(), "value".getBytes()).get(10, TimeUnit.SECONDS);
                assertEquals("value", new String(engine.get(tree, "key".getBytes()).get(10, TimeUnit.SECONDS)));
            }
            store.close();
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT 1")) { assertFalse(rows.isClosed()); rows.next(); assertEquals(1, rows.getInt(1)); }
        } finally { executor.shutdownNow(); }
    }

    private static MysqlDataSource dataSource(String url) throws Exception {
        var uri = new URI(url); var credentials = uri.getUserInfo().split(":", 2); var result = new MysqlDataSource();
        result.setURL("jdbc:mysql://" + uri.getHost() + ":" + uri.getPort() + uri.getPath() + "?" + (uri.getQuery() == null ? "" : uri.getQuery()));
        result.setUser(credentials[0]); result.setPassword(credentials.length == 2 ? credentials[1] : ""); return result;
    }
}
