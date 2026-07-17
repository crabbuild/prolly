package build.crab.prolly.store.dynamodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import build.crab.prolly.Prolly;
import build.crab.prolly.javaapi.remote.RemoteProlly;
import build.crab.prolly.remote.RemoteStore;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;

final class DynamoDbStoresTest {
    @Test
    void javaFactoryDrivesTheBorrowedAsyncClient() throws Exception {
        var endpoint = System.getenv("PROLLY_DYNAMODB_ENDPOINT");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "PROLLY_DYNAMODB_ENDPOINT is not set");
        Prolly.useLocalDebugLibrary();
        var client = DynamoDbAsyncClient.builder().endpointOverride(URI.create(endpoint)).region(Region.US_WEST_2)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local"))).build();
        var table = "prolly_java_" + UUID.randomUUID().toString().replace("-", "");
        var prefix = "prolly:test:java:".getBytes(StandardCharsets.UTF_8);
        var store = DynamoDbStores.from(client, table, prefix);
        var executor = Executors.newFixedThreadPool(4);
        try {
            DynamoDbStores.initializeTable(store).get(10, TimeUnit.SECONDS);
            try (var engine = RemoteProlly.open((RemoteStore) store, executor).get(10, TimeUnit.SECONDS)) {
                var tree = engine.create();
                tree = engine.put(tree, "key".getBytes(), "value".getBytes()).get(10, TimeUnit.SECONDS);
                assertEquals("value", new String(engine.get(tree, "key".getBytes()).get(10, TimeUnit.SECONDS)));
            }
            store.close();
            assertTrue(client.listTables(ListTablesRequest.builder().build()).get(10, TimeUnit.SECONDS).sdkHttpResponse().isSuccessful());
        } finally {
            store.close(); client.deleteTable(DeleteTableRequest.builder().tableName(table).build()).exceptionally(ignored -> null).get(10, TimeUnit.SECONDS); client.close(); executor.shutdownNow();
        }
    }
}
