package build.crab.prolly.store.cosmosdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.azure.cosmos.CosmosAsyncContainer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

final class CosmosDbStoresTest {
    @Test
    void javaFactoryBorrowsTheAzureContainer() {
        var container = mock(CosmosAsyncContainer.class);
        var prefix = "prolly:test:java:".getBytes(StandardCharsets.UTF_8);
        var store = CosmosDbStores.from(container, "tenant-a", prefix);

        assertEquals("CosmosDbStore", store.getClass().getSimpleName());
        store.close();

        verifyNoInteractions(container);
    }
}
