package build.crab.prolly.store.postgres

import build.crab.prolly.ProllyNative
import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RemoteProlly
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.storetest.StoreConformance
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
import org.postgresql.ds.PGSimpleDataSource

class PostgresStoreTest {
    private val databaseUrl = System.getenv("PROLLY_POSTGRES_URL")

    @BeforeEach
    fun requirePostgres() {
        assumeTrue(!databaseUrl.isNullOrBlank(), "PROLLY_POSTGRES_URL is not set")
    }

    @Test
    fun `PostgreSQL satisfies shared conformance`() = withStore { _, store ->
        StoreConformance.run { store }
    }

    @Test
    fun `PostgreSQL uses the Rust physical layout in both directions`() = withStore { dataSource, store ->
        dataSource.connection.use { connection ->
            val actual = connection.prepareStatement(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name IN ('prolly_nodes', 'prolly_hints', 'prolly_roots')
                ORDER BY table_name, ordinal_position
                """.trimIndent(),
            ).use { statement ->
                statement.executeQuery().use { rows ->
                    buildList {
                        while (rows.next()) add(listOf(rows.getString(1), rows.getString(2), rows.getString(3)))
                    }
                }
            }
            assertEquals(
                listOf(
                    listOf("prolly_hints", "namespace", "bytea"),
                    listOf("prolly_hints", "key", "bytea"),
                    listOf("prolly_hints", "value", "bytea"),
                    listOf("prolly_nodes", "cid", "bytea"),
                    listOf("prolly_nodes", "node", "bytea"),
                    listOf("prolly_roots", "name", "bytea"),
                    listOf("prolly_roots", "manifest", "bytea"),
                ),
                actual,
            )
            connection.execute("INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)", "rust-cid".bytes(), "rust-node".bytes())
            connection.execute("INSERT INTO prolly_hints (namespace, key, value) VALUES (?, ?, ?)", "rust-ns".bytes(), "rust-key".bytes(), "rust-hint".bytes())
            connection.execute("INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)", "rust-root".bytes(), "rust-manifest".bytes())
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
    fun `PostgreSQL serializes missing-root CAS and rolls conflicts back`() = withStore { _, store ->
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
        val result = store.commitTransaction(
            listOf(NodeMutation.Upsert("rollback-node".bytes(), "must-not-write".bytes())),
            listOf(RootCondition("race".bytes(), OptionalBytes.missing())),
            listOf(RootWrite.Put("rollback-root".bytes(), "must-not-publish".bytes())),
        )
        assertFalse(result.applied)
        assertFalse(store.getNode("rollback-node".bytes()).present)
        assertFalse(store.getRootManifest("rollback-root".bytes()).present)
    }

    @Test
    fun `cancellation before JDBC dispatch prevents a write and close does not own inputs`() = runBlocking {
        val dataSource = postgresDataSource(checkNotNull(databaseUrl))
        val executor = Executors.newSingleThreadExecutor()
        val dispatcher = executor.asCoroutineDispatcher()
        val store = PostgresStore(dataSource, dispatcher)
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        try {
            store.initializeSchema()
            clear(dataSource)
            executor.submit { started.countDown(); release.await() }
            started.await()
            val write = async { store.putNode("cancelled".bytes(), "must-not-write".bytes()) }
            yield()
            write.cancel()
            release.countDown()
            write.join()
            executor.submit {}.get()
            dataSource.connection.use { connection ->
                assertEquals(0, connection.queryInt("SELECT count(*) FROM prolly_nodes WHERE cid = ?", "cancelled".bytes()))
            }
            store.close()
            dataSource.connection.use { connection -> assertEquals(1, connection.queryInt("SELECT 1")) }
        } finally {
            release.countDown()
            store.close()
            dispatcher.close()
            executor.shutdownNow()
        }
    }

    @Test
    fun `PostgreSQL reads Rust trees and Rust reads Kotlin trees`() = withStore { _, store ->
        ProllyNative.useLocalDebugLibrary()
        runRustInterop("write", "rust-main", "rust-key", "rust-value")
        RemoteProlly.open(store).use { engine ->
            val rustTree = checkNotNull(engine.loadNamedRoot("rust-main".bytes()))
            assertEquals("rust-value", checkNotNull(engine.get(rustTree, "rust-key".bytes())).decodeToString())
            val kotlinTree = engine.put(engine.create(), "kotlin-key".bytes(), "kotlin-value".bytes())
            engine.publishNamedRoot("kotlin-main".bytes(), kotlinTree)
        }
        runRustInterop("verify", "kotlin-main", "kotlin-key", "kotlin-value")
    }

    private fun withStore(block: suspend (PGSimpleDataSource, PostgresStore) -> Unit) = runBlocking {
        val dataSource = postgresDataSource(checkNotNull(databaseUrl))
        val executor = Executors.newFixedThreadPool(40) { runnable -> Thread(runnable, "postgres-test-jdbc") }
        val dispatcher = executor.asCoroutineDispatcher()
        val store = PostgresStore(dataSource, dispatcher)
        try {
            store.initializeSchema()
            clear(dataSource)
            block(dataSource, store)
        } finally {
            store.close()
            dispatcher.close()
            executor.shutdownNow()
        }
    }

    private fun runRustInterop(operation: String, root: String, key: String, value: String) {
        val repository = generateSequence(java.nio.file.Path.of(System.getProperty("user.dir"))) { it.parent }
            .first { Files.exists(it.resolve("stores/prolly-store-postgres/Cargo.toml")) }
        val process = ProcessBuilder(
            "cargo", "run", "--quiet", "--manifest-path", "stores/prolly-store-postgres/Cargo.toml",
            "--example", "language_interop", "--", operation, databaseUrl, root, key, value,
        ).directory(repository.toFile()).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().use { it.readText() }
        check(process.waitFor() == 0) { "Rust PostgreSQL interop failed: $output" }
    }
}

private fun postgresDataSource(url: String): PGSimpleDataSource {
    val uri = URI(url)
    val credentials = checkNotNull(uri.userInfo).split(":", limit = 2)
    return PGSimpleDataSource().apply {
        setURL("jdbc:postgresql://${uri.host}:${uri.port}${uri.path}?${uri.query.orEmpty()}")
        user = credentials[0]
        password = credentials.getOrElse(1) { "" }
    }
}

private fun clear(dataSource: PGSimpleDataSource) {
    dataSource.connection.use { connection -> connection.createStatement().use { it.executeUpdate("TRUNCATE prolly_nodes, prolly_hints, prolly_roots") } }
}

private fun java.sql.Connection.execute(sql: String, vararg values: ByteArray) {
    prepareStatement(sql).use { statement ->
        values.forEachIndexed { index, value -> statement.setBytes(index + 1, value) }
        statement.executeUpdate()
    }
}

private fun java.sql.Connection.queryBytes(sql: String, vararg values: ByteArray): ByteArray =
    prepareStatement(sql).use { statement ->
        values.forEachIndexed { index, value -> statement.setBytes(index + 1, value) }
        statement.executeQuery().use { rows -> check(rows.next()); rows.getBytes(1) }
    }

private fun java.sql.Connection.queryInt(sql: String, vararg values: ByteArray): Int =
    prepareStatement(sql).use { statement ->
        values.forEachIndexed { index, value -> statement.setBytes(index + 1, value) }
        statement.executeQuery().use { rows -> check(rows.next()); rows.getInt(1) }
    }

private fun assertOptional(value: OptionalBytes, expected: String) {
    assertTrue(value.present)
    assertEquals(expected, value.value.decodeToString())
}

private fun String.bytes(): ByteArray = encodeToByteArray()
