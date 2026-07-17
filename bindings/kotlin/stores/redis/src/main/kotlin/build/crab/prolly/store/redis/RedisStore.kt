package build.crab.prolly.store.redis

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
import io.lettuce.core.RedisException
import io.lettuce.core.ScanArgs
import io.lettuce.core.ScanCursor
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.async.RedisAsyncCommands
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.future.await

data class RedisStoreOptions(
    val keyPrefix: ByteArray = "prolly:".encodeToByteArray(),
    val adapterName: String = "redis-v1",
    val readParallelism: UInt = 16u,
)

class RedisStore @JvmOverloads constructor(
    private val commands: RedisAsyncCommands<ByteArray, ByteArray>,
    options: RedisStoreOptions = RedisStoreOptions(),
) : RemoteStore, AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val keyPrefix = options.keyPrefix.copyOf()
    private val storeDescriptor = validateStoreDescriptor(
        StoreDescriptor(
            protocolMajor = 1u,
            adapterName = options.adapterName.ifBlank { "redis-v1" },
            provider = "redis",
            schemaVersion = 1u,
            capabilities = StoreCapabilities(true, true, true, true, true, true, true, true, options.readParallelism),
            limits = StoreLimits(),
        ),
    )

    constructor(commands: RedisAsyncCommands<ByteArray, ByteArray>, keyPrefix: ByteArray) :
        this(commands, RedisStoreOptions(keyPrefix = keyPrefix))

    override suspend fun descriptor(): StoreDescriptor = operation("descriptor") { storeDescriptor }
    override suspend fun getNode(cid: ByteArray): OptionalBytes = get(familyKey(NODE, cid), "get_node")
    override suspend fun putNode(cid: ByteArray, value: ByteArray) = set(familyKey(NODE, cid), value, "put_node")
    override suspend fun deleteNode(cid: ByteArray) = delete(familyKey(NODE, cid), "delete_node")

    override suspend fun batchNodes(operations: List<NodeMutation>) {
        val owned = operations.map(::cloneMutation)
        operation("batch_nodes") {
            if (owned.isEmpty()) return@operation
            evalMutations(
                owned.map { familyKey(NODE, it.cid) },
                owned.flatMap { if (it is NodeMutation.Upsert) listOf(ONE, it.node) else listOf(ZERO, EMPTY) },
            )
        }
    }

    override suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes> {
        val keys = cids.map { familyKey(NODE, it) }
        return operation("batch_get_nodes_ordered") {
            if (keys.isEmpty()) emptyList()
            else commands.mget(*keys.toTypedArray()).await().map { if (it.hasValue()) OptionalBytes.present(it.value) else OptionalBytes.missing() }
        }
    }

    override suspend fun listNodeCids(): List<ByteArray> = operation("list_node_cids") {
        val family = keyPrefix + NODE
        scanFamily(family).map { it.copyOfRange(family.size, it.size) }.filter { it.size == 32 }.sortedWith(BYTE_ARRAY_COMPARATOR)
    }

    override suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytes = get(hintKey(namespace, key), "get_hint")
    override suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray) = set(hintKey(namespace, key), value, "put_hint")

    override suspend fun batchPutNodesWithHint(nodes: List<NodeEntry>, namespace: ByteArray, key: ByteArray, value: ByteArray) {
        val ownedNodes = nodes.map { NodeEntry(it.cid, it.node) }
        val ownedValue = value.copyOf()
        operation("batch_put_nodes_with_hint") {
            val keys = ownedNodes.map { familyKey(NODE, it.cid) } + hintKey(namespace, key)
            val values = ownedNodes.flatMap { listOf(ONE, it.node) } + listOf(ONE, ownedValue)
            evalMutations(keys, values)
        }
    }

    override suspend fun getRootManifest(name: ByteArray): OptionalBytes = get(familyKey(ROOT, name), "get_root_manifest")
    override suspend fun putRootManifest(name: ByteArray, manifest: ByteArray) = set(familyKey(ROOT, name), manifest, "put_root_manifest")
    override suspend fun deleteRootManifest(name: ByteArray) = delete(familyKey(ROOT, name), "delete_root_manifest")

    override suspend fun compareAndSwapRootManifest(name: ByteArray, expected: OptionalBytes, replacement: OptionalBytes): RootCasResult {
        val key = familyKey(ROOT, name)
        val ownedExpected = OptionalBytes.of(expected.present, expected.value)
        val ownedReplacement = OptionalBytes.of(replacement.present, replacement.value)
        return operation("compare_and_swap_root_manifest") {
            val result = eval(CAS_SCRIPT, listOf(key), listOf(flag(ownedExpected.present), ownedExpected.value, flag(ownedReplacement.present), ownedReplacement.value))
            RootCasResult(integer(result[0]) == 1L, optional(result[1], result[2]))
        }
    }

    override suspend fun listRootManifests(): List<NamedStoreRoot> = operation("list_root_manifests") {
        val family = keyPrefix + ROOT
        val keys = scanFamily(family).sortedWith(BYTE_ARRAY_COMPARATOR)
        if (keys.isEmpty()) emptyList()
        else commands.mget(*keys.toTypedArray()).await().mapIndexedNotNull { index, value ->
            if (value.hasValue()) NamedStoreRoot(keys[index].copyOfRange(family.size, keys[index].size), value.value) else null
        }
    }

    override suspend fun commitTransaction(nodes: List<NodeMutation>, conditions: List<RootCondition>, roots: List<RootWrite>): StoreTransactionResult {
        val ownedNodes = nodes.map(::cloneMutation)
        val ownedConditions = conditions.map { RootCondition(it.name, OptionalBytes.of(it.expected.present, it.expected.value)) }
        val ownedRoots = roots.map(::cloneRootWrite)
        return operation("commit_transaction") {
            val keys = ownedConditions.map { familyKey(ROOT, it.name) } + ownedNodes.map { familyKey(NODE, it.cid) } + ownedRoots.map { familyKey(ROOT, it.name) }
            val values = mutableListOf(ownedConditions.size.text(), ownedNodes.size.text(), ownedRoots.size.text())
            ownedConditions.forEach { values += flag(it.expected.present); values += it.expected.value }
            ownedNodes.forEach { values += flag(it is NodeMutation.Upsert); values += if (it is NodeMutation.Upsert) it.node else EMPTY }
            ownedRoots.forEach { values += flag(it is RootWrite.Put); values += if (it is RootWrite.Put) it.manifest else EMPTY }
            val result = eval(TRANSACTION_SCRIPT, keys, values)
            if (integer(result[0]) == 1L) StoreTransactionResult.applied()
            else {
                val index = integer(result[1]).toInt() - 1
                val conflict = ownedConditions.getOrNull(index) ?: throw invalidData("invalid transaction conflict index")
                StoreTransactionResult.conflict(StoreTransactionConflict(conflict.name, conflict.expected, optional(result[2], result[3])))
            }
        }
    }

    suspend fun clearNamespace() {
        if (keyPrefix.isEmpty()) throw StoreException(StoreError("invalid_argument", "refusing to clear an empty Redis key prefix"))
        operation("clear_namespace") { scanFamily(keyPrefix).chunked(256).forEach { commands.del(*it.toTypedArray()).await() } }
    }

    override fun close() { closed.set(true) }

    private suspend fun get(key: ByteArray, name: String): OptionalBytes = operation(name) { commands.get(key).await()?.let(OptionalBytes::present) ?: OptionalBytes.missing() }
    private suspend fun set(key: ByteArray, value: ByteArray, name: String) { val owned = value.copyOf(); operation(name) { commands.set(key, owned).await() } }
    private suspend fun delete(key: ByteArray, name: String) { operation(name) { commands.del(key).await() } }
    private fun familyKey(family: ByteArray, suffix: ByteArray): ByteArray = keyPrefix + family + suffix.copyOf()
    private fun hintKey(namespace: ByteArray, key: ByteArray): ByteArray = keyPrefix + HINT + ByteBuffer.allocate(8).putLong(namespace.size.toLong()).array() + namespace.copyOf() + key.copyOf()

    private suspend fun scanFamily(family: ByteArray): List<ByteArray> {
        val keys = mutableListOf<ByteArray>(); var cursor = ScanCursor.INITIAL
        do {
            val page = commands.scan(cursor, ScanArgs().limit(1024)).await()
            page.keys.filterTo(keys) { it.startsWithBytes(family) }; cursor = page
        } while (!cursor.isFinished)
        return keys.map(ByteArray::copyOf)
    }

    private suspend fun evalMutations(keys: List<ByteArray>, values: List<ByteArray>) { eval(MUTATE_SCRIPT, keys, values) }
    @Suppress("UNCHECKED_CAST")
    private suspend fun eval(script: String, keys: List<ByteArray>, values: List<ByteArray>): List<Any?> {
        val result = commands.eval<List<Any?>>(script, ScriptOutputType.MULTI, keys.toTypedArray(), *values.toTypedArray()).await()
        return result ?: throw invalidData("missing Lua response")
    }

    private suspend fun <T> operation(name: String, block: suspend () -> T): T {
        if (closed.get()) throw StoreException(StoreError("internal", "Redis store is closed"))
        return try { block() }
        catch (error: CancellationException) { throw error }
        catch (error: StoreException) { throw error }
        catch (error: RedisException) { throw redisError(name, error) }
        catch (error: Throwable) { throw StoreException(StoreError("internal", "Redis operation failed"), error) }
    }
}

private val NODE = "node:".encodeToByteArray(); private val ROOT = "root:".encodeToByteArray(); private val HINT = "hint:".encodeToByteArray()
private val ZERO = "0".encodeToByteArray(); private val ONE = "1".encodeToByteArray(); private val EMPTY = byteArrayOf()
private fun flag(value: Boolean) = if (value) ONE else ZERO
private fun Int.text() = toString().encodeToByteArray()
private fun cloneMutation(value: NodeMutation): NodeMutation = when (value) { is NodeMutation.Upsert -> NodeMutation.Upsert(value.cid, value.node); is NodeMutation.Delete -> NodeMutation.Delete(value.cid) }
private fun cloneRootWrite(value: RootWrite): RootWrite = when (value) { is RootWrite.Put -> RootWrite.Put(value.name, value.manifest); is RootWrite.Delete -> RootWrite.Delete(value.name) }
private fun optional(present: Any?, value: Any?): OptionalBytes = if (integer(present) == 0L) OptionalBytes.missing() else OptionalBytes.present(bytes(value))
private fun integer(value: Any?): Long = when (value) { is Number -> value.toLong(); is ByteArray -> value.decodeToString().toLongOrNull(); is String -> value.toLongOrNull(); else -> null } ?: throw invalidData("invalid integer")
private fun bytes(value: Any?): ByteArray = when (value) { is ByteArray -> value.copyOf(); is String -> value.encodeToByteArray(); else -> throw invalidData("non-binary value") }
private fun invalidData(message: String) = StoreException(StoreError("invalid_data", "Redis returned an $message"))
private fun ByteArray.startsWithBytes(prefix: ByteArray): Boolean = size >= prefix.size && prefix.indices.all { this[it] == prefix[it] }
private val BYTE_ARRAY_COMPARATOR = Comparator<ByteArray> { left, right -> val common = minOf(left.size, right.size); for (index in 0 until common) { val compared = left[index].toUByte().compareTo(right[index].toUByte()); if (compared != 0) return@Comparator compared }; left.size.compareTo(right.size) }
private fun redisError(operation: String, error: RedisException): StoreException {
    val retryable = error.message?.contains("connection", ignoreCase = true) == true || error.message?.contains("timeout", ignoreCase = true) == true
    return StoreException(StoreError(if (retryable) "unavailable" else "internal", "Redis operation failed", retryable, "redis:${error.javaClass.simpleName}:$operation"), error)
}

private const val CAS_SCRIPT = """
local current=redis.call('GET',KEYS[1]); local expected=ARGV[1]=='1'
if expected then if current==false or current~=ARGV[2] then return {0,current==false and 0 or 1,current or ''} end
elseif current~=false then return {0,1,current} end
if ARGV[3]=='1' then redis.call('SET',KEYS[1],ARGV[4]); return {1,1,ARGV[4]} end
redis.call('DEL',KEYS[1]); return {1,0,''}
"""
private const val MUTATE_SCRIPT = """
for i=1,#KEYS do local o=(i-1)*2; if ARGV[o+1]=='1' then redis.call('SET',KEYS[i],ARGV[o+2]) else redis.call('DEL',KEYS[i]) end end; return 1
"""
private const val TRANSACTION_SCRIPT = """
local cc=tonumber(ARGV[1]); local nc=tonumber(ARGV[2]); local rc=tonumber(ARGV[3]); local a=4
for i=1,cc do local current=redis.call('GET',KEYS[i]); local expected=ARGV[a]=='1'; local matches=(expected and current~=false and current==ARGV[a+1]) or (not expected and current==false); if not matches then return {0,i,current==false and 0 or 1,current or ''} end; a=a+2 end
local k=cc+1; for i=1,nc do if ARGV[a]=='1' then redis.call('SET',KEYS[k],ARGV[a+1]) else redis.call('DEL',KEYS[k]) end; a=a+2; k=k+1 end
for i=1,rc do if ARGV[a]=='1' then redis.call('SET',KEYS[k],ARGV[a+1]) else redis.call('DEL',KEYS[k]) end; a=a+2; k=k+1 end; return {1}
"""
