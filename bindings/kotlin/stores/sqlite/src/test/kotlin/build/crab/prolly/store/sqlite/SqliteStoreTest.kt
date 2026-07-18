package build.crab.prolly.store.sqlite

import build.crab.prolly.ProllyNative
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RemoteProlly
import build.crab.prolly.storetest.StoreConformance
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.sqlite.SQLiteDataSource

class SqliteStoreTest {
    @Test
    fun `SQLite satisfies conformance on a bounded JDBC dispatcher`() = withStore { _, store ->
        StoreConformance.run { store }
    }

    @Test
    fun `SQLite uses the Rust physical schema in both directions`() = withStore { dataSource, store ->
        dataSource.connection.use { connection ->
            val tables = connection.createStatement().use { statement ->
                statement.executeQuery(
                    "SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name",
                ).use { rows ->
                    buildList {
                        while (rows.next()) add(rows.getString(1) to rows.getString(2))
                    }
                }
            }
            assertEquals(listOf("prolly_hints", "prolly_nodes", "prolly_roots"), tables.map { it.first })
            assertTrue(tables.all { it.second.trim().endsWith("WITHOUT ROWID", ignoreCase = true) })

            connection.prepareStatement("INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)").use {
                it.setBytes(1, "rust-cid".bytes())
                it.setBytes(2, "rust-node".bytes())
                it.executeUpdate()
            }
            connection.prepareStatement(
                "INSERT INTO prolly_hints (namespace, key, value) VALUES (?, ?, ?)",
            ).use {
                it.setBytes(1, "rust-ns".bytes())
                it.setBytes(2, "rust-key".bytes())
                it.setBytes(3, "rust-hint".bytes())
                it.executeUpdate()
            }
            connection.prepareStatement("INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)").use {
                it.setBytes(1, "rust-root".bytes())
                it.setBytes(2, "rust-manifest".bytes())
                it.executeUpdate()
            }
        }

        assertOptional(store.getNode("rust-cid".bytes()), "rust-node")
        assertOptional(store.getHint("rust-ns".bytes(), "rust-key".bytes()), "rust-hint")
        assertOptional(store.getRootManifest("rust-root".bytes()), "rust-manifest")

        store.putNode("kotlin-cid".bytes(), "kotlin-node".bytes())
        store.putHint("kotlin-ns".bytes(), "kotlin-key".bytes(), "kotlin-hint".bytes())
        store.putRootManifest("kotlin-root".bytes(), "kotlin-manifest".bytes())
        dataSource.connection.use { connection ->
            assertEquals("kotlin-node", connection.queryBytes("SELECT node FROM prolly_nodes WHERE cid = ?", "kotlin-cid".bytes()).decodeToString())
            assertEquals("kotlin-hint", connection.queryBytes("SELECT value FROM prolly_hints WHERE namespace = ? AND key = ?", "kotlin-ns".bytes(), "kotlin-key".bytes()).decodeToString())
            assertEquals("kotlin-manifest", connection.queryBytes("SELECT manifest FROM prolly_roots WHERE name = ?", "kotlin-root".bytes()).decodeToString())
        }
    }

    @Test
    fun `close releases the adapter but not the injected data source`() = runBlocking {
        val path = Files.createTempFile("prolly-kotlin-sqlite-owner-", ".db")
        val dataSource = SQLiteDataSource().apply { url = "jdbc:sqlite:$path" }
        val executor = Executors.newSingleThreadExecutor()
        val store = SqliteStore(dataSource, executor.asCoroutineDispatcher())
        try {
            store.initializeSchema()
            store.close()
            dataSource.connection.use { connection ->
                assertEquals(1, connection.createStatement().use { it.executeQuery("SELECT 1").use { rows -> rows.next(); rows.getInt(1) } })
            }
        } finally {
            executor.shutdownNow()
            Files.deleteIfExists(path)
        }
    }

    @Test
    fun `SQLite serializes concurrent missing-root CAS to one winner`() = withStore { _, store ->
        val results = coroutineScope {
            (0 until 32).map { index ->
                async {
                    store.compareAndSwapRootManifest(
                        "race".bytes(),
                        OptionalBytes.missing(),
                        OptionalBytes.present("winner-$index".bytes()),
                    )
                }
            }.awaitAll()
        }
        assertEquals(1, results.count { it.applied })
    }

    @Test
    fun `cancellation before JDBC dispatch prevents the write`() = runBlocking {
        val path = Files.createTempFile("prolly-kotlin-sqlite-cancel-", ".db")
        val dataSource = SQLiteDataSource().apply { url = "jdbc:sqlite:$path" }
        val executor = Executors.newSingleThreadExecutor()
        val dispatcher = executor.asCoroutineDispatcher()
        val store = SqliteStore(dataSource, dispatcher)
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        try {
            store.initializeSchema()
            executor.submit {
                started.countDown()
                release.await()
            }
            started.await()
            val write = async { store.putNode("cancelled".bytes(), "must-not-write".bytes()) }
            yield()
            write.cancel()
            release.countDown()
            write.join()
            executor.submit {}.get()
            dataSource.connection.use { connection ->
                connection.prepareStatement("SELECT count(*) FROM prolly_nodes WHERE cid = ?").use {
                    it.setBytes(1, "cancelled".bytes())
                    it.executeQuery().use { rows -> rows.next(); assertEquals(0, rows.getInt(1)) }
                }
            }
        } finally {
            release.countDown()
            store.close()
            dispatcher.close()
            executor.shutdownNow()
            Files.deleteIfExists(path)
        }
    }

    @Test
    fun `SQLite reads Rust trees and Rust reads Kotlin trees`() = runBlocking {
        ProllyNative.useLocalDebugLibrary()
        val path = Files.createTempFile("prolly-kotlin-rust-sqlite-", ".db")
        val dataSource = SQLiteDataSource().apply { url = "jdbc:sqlite:$path" }
        val executor = Executors.newFixedThreadPool(2)
        val dispatcher = executor.asCoroutineDispatcher()
        val store = SqliteStore(dataSource, dispatcher)
        try {
            store.initializeSchema()
            runRustInterop("write", path, "rust-main", "rust-key", "rust-value")
            RemoteProlly.open(store).use { engine ->
                val rustTree = checkNotNull(engine.loadNamedRoot("rust-main".bytes()))
                assertEquals("rust-value", checkNotNull(engine.get(rustTree, "rust-key".bytes())).decodeToString())
                val kotlinTree = engine.put(engine.create(), "kotlin-key".bytes(), "kotlin-value".bytes())
                engine.publishNamedRoot("kotlin-main".bytes(), kotlinTree)
            }
            runRustInterop("verify", path, "kotlin-main", "kotlin-key", "kotlin-value")
        } finally {
            store.close()
            dispatcher.close()
            executor.shutdownNow()
            Files.deleteIfExists(path)
        }
    }

    private fun withStore(block: suspend (SQLiteDataSource, SqliteStore) -> Unit) = runBlocking {
        val path = Files.createTempFile("prolly-kotlin-sqlite-", ".db")
        val dataSource = SQLiteDataSource().apply { url = "jdbc:sqlite:$path" }
        val executor = Executors.newFixedThreadPool(2) { runnable -> Thread(runnable, "sqlite-test-jdbc") }
        val dispatcher = executor.asCoroutineDispatcher()
        val store = SqliteStore(dataSource, dispatcher)
        try {
            store.initializeSchema()
            block(dataSource, store)
        } finally {
            store.close()
            dispatcher.close()
            executor.shutdownNow()
            Files.deleteIfExists(path)
        }
    }
}

private fun assertOptional(value: OptionalBytes, expected: String) {
    assertTrue(value.present)
    assertEquals(expected, value.value.decodeToString())
}

private fun String.bytes(): ByteArray = encodeToByteArray()

private fun java.sql.Connection.queryBytes(sql: String, vararg values: ByteArray): ByteArray =
    prepareStatement(sql).use { statement ->
        values.forEachIndexed { index, value -> statement.setBytes(index + 1, value) }
        statement.executeQuery().use { rows ->
            check(rows.next())
            rows.getBytes(1)
        }
    }

private fun runRustInterop(
    operation: String,
    path: java.nio.file.Path,
    root: String,
    key: String,
    value: String,
) {
    val repository = generateSequence(java.nio.file.Path.of(System.getProperty("user.dir"))) { it.parent }
        .first { Files.exists(it.resolve("stores/prolly-store-sqlite/Cargo.toml")) }
    val process = ProcessBuilder(
        "cargo",
        "run",
        "--quiet",
        "--manifest-path",
        "stores/prolly-store-sqlite/Cargo.toml",
        "--example",
        "language_interop",
        "--",
        operation,
        path.toString(),
        root,
        key,
        value,
    ).directory(repository.toFile()).redirectErrorStream(true).start()
    val output = process.inputStream.bufferedReader().use { it.readText() }
    check(process.waitFor() == 0) { "Rust SQLite interop failed: $output" }
}
