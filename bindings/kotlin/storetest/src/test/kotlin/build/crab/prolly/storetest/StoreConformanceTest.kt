package build.crab.prolly.storetest

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class StoreConformanceTest {
    @Test
    fun protocolV1MemoryStorePassesSharedConformance() = runBlocking {
        StoreConformance.run { MemoryRemoteStore() }
    }
}
