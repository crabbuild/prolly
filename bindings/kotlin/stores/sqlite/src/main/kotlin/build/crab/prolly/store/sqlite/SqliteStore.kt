package build.crab.prolly.store.sqlite

import build.crab.prolly.remote.NamedStoreRoot
import build.crab.prolly.remote.NodeEntry
import build.crab.prolly.remote.NodeMutation
import build.crab.prolly.remote.OptionalBytes
import build.crab.prolly.remote.RemoteStore
import build.crab.prolly.remote.RootCasResult
import build.crab.prolly.remote.RootCondition
import build.crab.prolly.remote.RootWrite
import build.crab.prolly.remote.StoreCapabilities
import build.crab.prolly.remote.StoreDescriptor
import build.crab.prolly.remote.StoreError
import build.crab.prolly.remote.StoreException
import build.crab.prolly.remote.StoreLimits
import build.crab.prolly.remote.StoreTransactionConflict
import build.crab.prolly.remote.StoreTransactionResult
import build.crab.prolly.remote.validateStoreDescriptor
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import javax.sql.DataSource
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runInterruptible

data class SqliteStoreOptions(
    val adapterName: String = "sqlite-v1",
    val readParallelism: UInt = 16u,
)

class SqliteStore @JvmOverloads constructor(
    private val dataSource: DataSource,
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(16),
    options: SqliteStoreOptions = SqliteStoreOptions(),
) : RemoteStore, AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val javaScope = CoroutineScope(SupervisorJob() + dispatcher)
    private val storeDescriptor = validateStoreDescriptor(
        StoreDescriptor(
            protocolMajor = 2u,
            adapterName = options.adapterName.ifBlank { "sqlite-v1" },
            provider = "sqlite",
            schemaVersion = 1u,
            capabilities = StoreCapabilities(
                nativeBatchReads = true,
                atomicBatchWrites = true,
                nodeScan = true,
                hints = true,
                atomicNodesAndHint = true,
                rootScan = true,
                rootCompareAndSwap = true,
                transactions = true,
                readParallelism = options.readParallelism,
            ),
            limits = StoreLimits(),
        ),
    )

    constructor(dataSource: DataSource, executor: Executor) :
        this(dataSource, executor.asCoroutineDispatcher())

    suspend fun initializeSchema() = io("initialize_schema") {
        connection().use { connection ->
            connection.createStatement().use { statement ->
                CREATE_SCHEMA.forEach(statement::executeUpdate)
            }
        }
    }

    fun initializeSchemaAsync(): CompletableFuture<Unit> = javaScope.future { initializeSchema() }

    override suspend fun descriptor(): StoreDescriptor = io("descriptor") { storeDescriptor }

    override suspend fun getNode(cid: ByteArray): OptionalBytes {
        val key = cid.copyOf()
        return io("get_node") { connection().use { queryOptional(it, SELECT_NODE, key) } }
    }

    override suspend fun putNode(cid: ByteArray, value: ByteArray) {
        val key = cid.copyOf()
        val node = value.copyOf()
        io("put_node") { connection().use { it.execute(UPSERT_NODE, key, node) } }
    }

    override suspend fun deleteNode(cid: ByteArray) {
        val key = cid.copyOf()
        io("delete_node") { connection().use { it.execute(DELETE_NODE, key) } }
    }

    override suspend fun batchNodes(operations: List<NodeMutation>) {
        val owned = operations.map(::cloneMutation)
        io("batch_nodes") {
            connection().use { connection ->
                connection.immediateTransaction { applyNodeMutations(connection, owned) }
            }
        }
    }

    override suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes> {
        val keys = cids.map(ByteArray::copyOf)
        return io("batch_get_nodes_ordered") {
            connection().use { connection ->
                connection.prepareStatement(SELECT_NODE).use { statement ->
                    keys.map { key -> queryOptional(statement, key) }
                }
            }
        }
    }

    override suspend fun listNodeCids(): List<ByteArray> = io("list_node_cids") {
        connection().use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery("SELECT cid FROM prolly_nodes ORDER BY cid").use { rows ->
                    buildList {
                        while (rows.next()) add(rows.getBytes(1).copyOf())
                    }
                }
            }
        }
    }

    override suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytes {
        val ownedNamespace = namespace.copyOf()
        val ownedKey = key.copyOf()
        return io("get_hint") {
            connection().use { queryOptional(it, SELECT_HINT, ownedNamespace, ownedKey) }
        }
    }

    override suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray) {
        val ownedNamespace = namespace.copyOf()
        val ownedKey = key.copyOf()
        val ownedValue = value.copyOf()
        io("put_hint") {
            connection().use { it.execute(UPSERT_HINT, ownedNamespace, ownedKey, ownedValue) }
        }
    }

    override suspend fun batchPutNodesWithHint(
        nodes: List<NodeEntry>,
        namespace: ByteArray,
        key: ByteArray,
        value: ByteArray,
    ) {
        val ownedNodes = nodes.map { NodeEntry(it.cid, it.node) }
        val ownedNamespace = namespace.copyOf()
        val ownedKey = key.copyOf()
        val ownedValue = value.copyOf()
        io("batch_put_nodes_with_hint") {
            connection().use { connection ->
                connection.immediateTransaction {
                    connection.prepareStatement(UPSERT_NODE).use { statement ->
                        ownedNodes.forEach { node -> statement.execute(node.cid, node.node) }
                    }
                    connection.execute(UPSERT_HINT, ownedNamespace, ownedKey, ownedValue)
                }
            }
        }
    }

    override suspend fun getRootManifest(name: ByteArray): OptionalBytes {
        val ownedName = name.copyOf()
        return io("get_root_manifest") {
            connection().use { queryOptional(it, SELECT_ROOT, ownedName) }
        }
    }

    override suspend fun putRootManifest(name: ByteArray, manifest: ByteArray) {
        val ownedName = name.copyOf()
        val ownedManifest = manifest.copyOf()
        io("put_root_manifest") {
            connection().use { it.execute(UPSERT_ROOT, ownedName, ownedManifest) }
        }
    }

    override suspend fun deleteRootManifest(name: ByteArray) {
        val ownedName = name.copyOf()
        io("delete_root_manifest") { connection().use { it.execute(DELETE_ROOT, ownedName) } }
    }

    override suspend fun compareAndSwapRootManifest(
        name: ByteArray,
        expected: OptionalBytes,
        replacement: OptionalBytes,
    ): RootCasResult {
        val ownedName = name.copyOf()
        val ownedExpected = OptionalBytes.of(expected.present, expected.value)
        val ownedReplacement = OptionalBytes.of(replacement.present, replacement.value)
        return io("compare_and_swap_root_manifest") {
            connection().use { connection ->
                connection.immediateTransaction {
                    val current = queryOptional(connection, SELECT_ROOT, ownedName)
                    if (!optionalEqual(current, ownedExpected)) {
                        RootCasResult(false, current)
                    } else {
                        writeOptionalRoot(connection, ownedName, ownedReplacement)
                        RootCasResult(
                            true,
                            OptionalBytes.of(ownedReplacement.present, ownedReplacement.value),
                        )
                    }
                }
            }
        }
    }

    override suspend fun listRootManifests(): List<NamedStoreRoot> = io("list_root_manifests") {
        connection().use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(
                    "SELECT name, manifest FROM prolly_roots ORDER BY name",
                ).use { rows ->
                    buildList {
                        while (rows.next()) add(NamedStoreRoot(rows.getBytes(1), rows.getBytes(2)))
                    }
                }
            }
        }
    }

    override suspend fun commitTransaction(
        nodes: List<NodeMutation>,
        conditions: List<RootCondition>,
        roots: List<RootWrite>,
    ): StoreTransactionResult {
        val ownedNodes = nodes.map(::cloneMutation)
        val ownedConditions = conditions.map {
            RootCondition(it.name, OptionalBytes.of(it.expected.present, it.expected.value))
        }
        val ownedRoots = roots.map(::cloneRootWrite)
        return io("commit_transaction") {
            connection().use { connection ->
                connection.immediateTransaction {
                    ownedConditions.forEach { condition ->
                        val current = queryOptional(connection, SELECT_ROOT, condition.name)
                        if (!optionalEqual(current, condition.expected)) {
                            return@immediateTransaction StoreTransactionResult.conflict(
                                StoreTransactionConflict(
                                    condition.name,
                                    condition.expected,
                                    current,
                                ),
                            )
                        }
                    }
                    applyNodeMutations(connection, ownedNodes)
                    ownedRoots.forEach { root ->
                        when (root) {
                            is RootWrite.Put -> connection.execute(UPSERT_ROOT, root.name, root.manifest)
                            is RootWrite.Delete -> connection.execute(DELETE_ROOT, root.name)
                        }
                    }
                    StoreTransactionResult.applied()
                }
            }
        }
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) javaScope.cancel()
    }

    private fun connection(): Connection {
        val connection = dataSource.connection
        try {
            connection.createStatement().use { it.execute("PRAGMA busy_timeout = 5000") }
            return connection
        } catch (error: Throwable) {
            runCatching(connection::close)
            throw error
        }
    }

    private suspend fun <T> io(operation: String, block: () -> T): T = try {
        runInterruptible(dispatcher) {
            ensureOpen()
            block()
        }
    } catch (error: CancellationException) {
        throw error
    } catch (error: StoreException) {
        throw error
    } catch (error: SQLException) {
        throw sqliteError(operation, error)
    } catch (error: Throwable) {
        throw StoreException(
            StoreError("internal", "SQLite operation failed"),
            error,
        )
    }

    private fun ensureOpen() {
        if (closed.get()) {
            throw StoreException(StoreError("internal", "SQLite store is closed"))
        }
    }
}

private val CREATE_SCHEMA = listOf(
    """
    CREATE TABLE IF NOT EXISTS prolly_nodes (
        cid  BLOB PRIMARY KEY NOT NULL,
        node BLOB NOT NULL
    ) WITHOUT ROWID
    """.trimIndent(),
    """
    CREATE TABLE IF NOT EXISTS prolly_hints (
        namespace BLOB NOT NULL,
        key       BLOB NOT NULL,
        value     BLOB NOT NULL,
        PRIMARY KEY (namespace, key)
    ) WITHOUT ROWID
    """.trimIndent(),
    """
    CREATE TABLE IF NOT EXISTS prolly_roots (
        name     BLOB PRIMARY KEY NOT NULL,
        manifest BLOB NOT NULL
    ) WITHOUT ROWID
    """.trimIndent(),
)

private const val SELECT_NODE = "SELECT node FROM prolly_nodes WHERE cid = ?"
private const val UPSERT_NODE = """INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node"""
private const val DELETE_NODE = "DELETE FROM prolly_nodes WHERE cid = ?"
private const val SELECT_HINT = "SELECT value FROM prolly_hints WHERE namespace = ? AND key = ?"
private const val UPSERT_HINT = """INSERT INTO prolly_hints (namespace, key, value) VALUES (?, ?, ?)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value"""
private const val SELECT_ROOT = "SELECT manifest FROM prolly_roots WHERE name = ?"
private const val UPSERT_ROOT = """INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)
ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest"""
private const val DELETE_ROOT = "DELETE FROM prolly_roots WHERE name = ?"

private fun queryOptional(connection: Connection, sql: String, vararg values: ByteArray): OptionalBytes =
    connection.prepareStatement(sql).use { queryOptional(it, *values) }

private fun queryOptional(statement: PreparedStatement, vararg values: ByteArray): OptionalBytes {
    values.forEachIndexed { index, value -> statement.setBytes(index + 1, value) }
    return statement.executeQuery().use { rows ->
        if (rows.next()) OptionalBytes.present(rows.getBytes(1)) else OptionalBytes.missing()
    }
}

private fun Connection.execute(sql: String, vararg values: ByteArray) {
    prepareStatement(sql).use { statement -> statement.execute(*values) }
}

private fun PreparedStatement.execute(vararg values: ByteArray) {
    values.forEachIndexed { index, value -> setBytes(index + 1, value) }
    executeUpdate()
}

private fun applyNodeMutations(connection: Connection, operations: List<NodeMutation>) {
    connection.prepareStatement(UPSERT_NODE).use { upsert ->
        connection.prepareStatement(DELETE_NODE).use { delete ->
            operations.forEach { operation ->
                when (operation) {
                    is NodeMutation.Upsert -> upsert.execute(operation.cid, operation.node)
                    is NodeMutation.Delete -> delete.execute(operation.cid)
                }
            }
        }
    }
}

private fun writeOptionalRoot(connection: Connection, name: ByteArray, value: OptionalBytes) {
    if (value.present) connection.execute(UPSERT_ROOT, name, value.value)
    else connection.execute(DELETE_ROOT, name)
}

private inline fun <T> Connection.immediateTransaction(block: () -> T): T {
    createStatement().use { it.execute("BEGIN IMMEDIATE") }
    try {
        val result = block()
        createStatement().use { it.execute("COMMIT") }
        return result
    } catch (error: Throwable) {
        runCatching { createStatement().use { it.execute("ROLLBACK") } }
        throw error
    }
}

private fun cloneMutation(operation: NodeMutation): NodeMutation = when (operation) {
    is NodeMutation.Upsert -> NodeMutation.Upsert(operation.cid, operation.node)
    is NodeMutation.Delete -> NodeMutation.Delete(operation.cid)
}

private fun cloneRootWrite(write: RootWrite): RootWrite = when (write) {
    is RootWrite.Put -> RootWrite.Put(write.name, write.manifest)
    is RootWrite.Delete -> RootWrite.Delete(write.name)
}

private fun optionalEqual(left: OptionalBytes, right: OptionalBytes): Boolean =
    left.present == right.present && (!left.present || left.value.contentEquals(right.value))

private fun sqliteError(operation: String, error: SQLException): StoreException {
    val baseCode = error.errorCode and 0xff
    val retryable = baseCode == 5 || baseCode == 6
    return StoreException(
        StoreError(
            code = if (retryable) "unavailable" else "internal",
            message = "SQLite operation failed",
            retryable = retryable,
            providerCode = "sqlite:$baseCode:$operation",
        ),
        error,
    )
}
