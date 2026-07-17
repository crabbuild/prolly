package build.crab.prolly.store.mysql

import build.crab.prolly.ProllyNative
import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RemoteProlly
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.remote.StoreException
import build.crab.prolly.storetest.StoreConformance
import com.mysql.cj.jdbc.MysqlDataSource
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MysqlStoreTest {
    private val databaseUrl = System.getenv("PROLLY_MYSQL_URL")

    @BeforeEach fun requireMysql() = assumeTrue(!databaseUrl.isNullOrBlank(), "PROLLY_MYSQL_URL is not set")

    @Test
    fun `MySQL satisfies conformance binary ordering and pre-driver limits`() = withStore { _, store ->
        StoreConformance.run { store }
        val binaryKeys = listOf(byteArrayOf(0x00), byteArrayOf(0x7f), byteArrayOf(0x80.toByte()), byteArrayOf(0xff.toByte()))
        binaryKeys.forEach { store.putNode(it, it) }
        assertEquals(listOf("00", "7f", "80", "ff"), store.listNodeCids().filter { it.size == 1 }.map(ByteArray::hex))

        val unreachable = MysqlDataSource().apply { setURL("jdbc:mysql://127.0.0.1:1/missing?connectTimeout=50") }
        val invalid = MysqlStore(unreachable)
        val error = runCatching { invalid.getNode(ByteArray(33)) }.exceptionOrNull()
        assertTrue(error is StoreException)
        assertEquals("invalid_argument", (error as StoreException).error.code)
        invalid.close()
    }

    @Test
    fun `MySQL uses the Rust physical layout in both directions`() = withStore { dataSource, store ->
        dataSource.connection.use { connection ->
            val actual = connection.prepareStatement(
                """SELECT table_name, column_name, data_type, character_maximum_length
                   FROM information_schema.columns
                   WHERE table_schema = DATABASE() AND table_name IN ('prolly_nodes','prolly_hints','prolly_roots')
                   ORDER BY table_name, ordinal_position""".trimIndent(),
            ).use { statement -> statement.executeQuery().use { rows -> buildList { while (rows.next()) add(listOf(rows.getString(1), rows.getString(2), rows.getString(3), rows.getLong(4))) } } }
            assertEquals(
                listOf(
                    listOf("prolly_hints", "namespace", "varbinary", 255L), listOf("prolly_hints", "key", "varbinary", 255L), listOf("prolly_hints", "value", "longblob", 4294967295L),
                    listOf("prolly_nodes", "cid", "varbinary", 32L), listOf("prolly_nodes", "node", "longblob", 4294967295L),
                    listOf("prolly_roots", "name", "varbinary", 255L), listOf("prolly_roots", "manifest", "longblob", 4294967295L),
                ),
                actual,
            )
            connection.execute("INSERT INTO prolly_nodes (cid,node) VALUES (?,?)", "rust-cid".bytes(), "rust-node".bytes())
            connection.execute("INSERT INTO prolly_hints (namespace,`key`,value) VALUES (?,?,?)", "rust-ns".bytes(), "rust-key".bytes(), "rust-hint".bytes())
            connection.execute("INSERT INTO prolly_roots (name,manifest) VALUES (?,?)", "rust-root".bytes(), "rust-manifest".bytes())
        }
        assertOptional(store.getNode("rust-cid".bytes()), "rust-node")
        assertOptional(store.getHint("rust-ns".bytes(), "rust-key".bytes()), "rust-hint")
        assertOptional(store.getRootManifest("rust-root".bytes()), "rust-manifest")
        store.putNode("kotlin-cid".bytes(), "kotlin-node".bytes())
        store.putHint("kotlin-ns".bytes(), "kotlin-key".bytes(), "kotlin-hint".bytes())
        store.putRootManifest("kotlin-root".bytes(), "kotlin-manifest".bytes())
        dataSource.connection.use { connection ->
            assertEquals("kotlin-node", connection.queryBytes("SELECT node FROM prolly_nodes WHERE cid=?", "kotlin-cid".bytes()).decodeToString())
            assertEquals("kotlin-hint", connection.queryBytes("SELECT value FROM prolly_hints WHERE namespace=? AND `key`=?", "kotlin-ns".bytes(), "kotlin-key".bytes()).decodeToString())
            assertEquals("kotlin-manifest", connection.queryBytes("SELECT manifest FROM prolly_roots WHERE name=?", "kotlin-root".bytes()).decodeToString())
        }
    }

    @Test
    fun `MySQL serializes missing-root CAS and rolls conflicts back`() = withStore { _, store ->
        val results = coroutineScope { (0 until 32).map { index -> async { store.compareAndSwapRootManifest("race".bytes(), OptionalBytes.missing(), OptionalBytes.present("winner-$index".bytes())) } }.awaitAll() }
        assertEquals(1, results.count { it.applied })
        val conflict = store.commitTransaction(
            listOf(NodeMutation.Upsert("rollback-node".bytes(), "must-not-write".bytes())),
            listOf(RootCondition("race".bytes(), OptionalBytes.missing())),
            listOf(RootWrite.Put("rollback-root".bytes(), "must-not-publish".bytes())),
        )
        assertFalse(conflict.applied)
        assertFalse(store.getNode("rollback-node".bytes()).present)
        assertFalse(store.getRootManifest("rollback-root".bytes()).present)
    }

    @Test
    fun `cancellation before JDBC dispatch prevents writes and close keeps inputs open`() = runBlocking {
        val dataSource = mysqlDataSource(checkNotNull(databaseUrl))
        val executor = Executors.newSingleThreadExecutor()
        val dispatcher = executor.asCoroutineDispatcher()
        val store = MysqlStore(dataSource, dispatcher)
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        try {
            store.initializeSchema(); clear(dataSource)
            executor.submit { started.countDown(); release.await() }; started.await()
            val write = async { store.putNode("cancelled".bytes(), "must-not-write".bytes()) }
            yield(); write.cancel(); release.countDown(); write.join(); executor.submit {}.get()
            dataSource.connection.use { assertEquals(0, it.queryInt("SELECT count(*) FROM prolly_nodes WHERE cid=?", "cancelled".bytes())) }
            store.close()
            dataSource.connection.use { assertEquals(1, it.queryInt("SELECT 1")) }
        } finally {
            release.countDown(); store.close(); dispatcher.close(); executor.shutdownNow()
        }
    }

    @Test
    fun `MySQL reads Rust trees and Rust reads Kotlin trees`() = withStore { _, store ->
        ProllyNative.useLocalDebugLibrary()
        runRustInterop("write", "rust-main", "rust-key", "rust-value")
        RemoteProlly.open(store).use { engine ->
            val rustTree = checkNotNull(engine.loadNamedRoot("rust-main".bytes()))
            assertEquals("rust-value", checkNotNull(engine.get(rustTree, "rust-key".bytes())).decodeToString())
            val tree = engine.put(engine.create(), "kotlin-key".bytes(), "kotlin-value".bytes())
            engine.publishNamedRoot("kotlin-main".bytes(), tree)
        }
        runRustInterop("verify", "kotlin-main", "kotlin-key", "kotlin-value")
    }

    private fun withStore(block: suspend (MysqlDataSource, MysqlStore) -> Unit) = runBlocking {
        val dataSource = mysqlDataSource(checkNotNull(databaseUrl))
        val executor = Executors.newFixedThreadPool(40) { runnable -> Thread(runnable, "mysql-test-jdbc") }
        val dispatcher = executor.asCoroutineDispatcher()
        val store = MysqlStore(dataSource, dispatcher)
        try { store.initializeSchema(); clear(dataSource); block(dataSource, store) }
        finally { store.close(); dispatcher.close(); executor.shutdownNow() }
    }

    private fun runRustInterop(operation: String, root: String, key: String, value: String) {
        val repository = generateSequence(java.nio.file.Path.of(System.getProperty("user.dir"))) { it.parent }.first { Files.exists(it.resolve("stores/prolly-store-mysql/Cargo.toml")) }
        val process = ProcessBuilder("cargo", "run", "--quiet", "--manifest-path", "stores/prolly-store-mysql/Cargo.toml", "--example", "language_interop", "--", operation, databaseUrl, root, key, value).directory(repository.toFile()).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().use { it.readText() }
        check(process.waitFor() == 0) { "Rust MySQL interop failed: $output" }
    }
}

private fun mysqlDataSource(url: String): MysqlDataSource {
    val uri = URI(url); val credentials = checkNotNull(uri.userInfo).split(":", limit = 2)
    return MysqlDataSource().apply { setURL("jdbc:mysql://${uri.host}:${uri.port}${uri.path}?${uri.query.orEmpty()}"); user = credentials[0]; password = credentials.getOrElse(1) { "" } }
}
private fun clear(dataSource: MysqlDataSource) { dataSource.connection.use { it.createStatement().use { statement -> statement.executeUpdate("TRUNCATE prolly_nodes"); statement.executeUpdate("TRUNCATE prolly_hints"); statement.executeUpdate("TRUNCATE prolly_roots") } } }
private fun java.sql.Connection.execute(sql: String, vararg values: ByteArray) { prepareStatement(sql).use { statement -> values.forEachIndexed { index, value -> statement.setBytes(index + 1, value) }; statement.executeUpdate() } }
private fun java.sql.Connection.queryBytes(sql: String, vararg values: ByteArray): ByteArray = prepareStatement(sql).use { statement -> values.forEachIndexed { index, value -> statement.setBytes(index + 1, value) }; statement.executeQuery().use { rows -> check(rows.next()); rows.getBytes(1) } }
private fun java.sql.Connection.queryInt(sql: String, vararg values: ByteArray): Int = prepareStatement(sql).use { statement -> values.forEachIndexed { index, value -> statement.setBytes(index + 1, value) }; statement.executeQuery().use { rows -> check(rows.next()); rows.getInt(1) } }
private fun assertOptional(value: OptionalBytes, expected: String) { assertTrue(value.present); assertEquals(expected, value.value.decodeToString()) }
private fun String.bytes(): ByteArray = encodeToByteArray()
private fun ByteArray.hex(): String = joinToString("") { "%02x".format(it.toInt() and 0xff) }
