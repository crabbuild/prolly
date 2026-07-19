package build.crab.prolly.remote

const val STORE_PROTOCOL_MAJOR: UInt = 2u

const val GENERAL: UInt = 0u
const val POINT_UPSERT: UInt = 1u
const val POINT_DELETE: UInt = 2u
const val BATCH_MUTATION: UInt = 3u
const val TREE_BUILD: UInt = 4u
const val MERGE: UInt = 5u
const val RANGE_DELETE: UInt = 6u
const val REPLICATION: UInt = 7u
const val MAINTENANCE: UInt = 8u

fun normalizePublicationOriginCode(code: UInt): UInt = when (code) {
    GENERAL,
    POINT_UPSERT,
    POINT_DELETE,
    BATCH_MUTATION,
    TREE_BUILD,
    MERGE,
    RANGE_DELETE,
    REPLICATION,
    MAINTENANCE,
    -> code
    else -> GENERAL
}

data class StoreCapabilities(
    val nativeBatchReads: Boolean,
    val atomicBatchWrites: Boolean,
    val nodeScan: Boolean,
    val hints: Boolean,
    val atomicNodesAndHint: Boolean,
    val rootScan: Boolean,
    val rootCompareAndSwap: Boolean,
    val transactions: Boolean,
    val readParallelism: UInt,
)

data class StoreLimits(
    val maxBatchReadItems: UInt? = null,
    val maxBatchWriteItems: UInt? = null,
    val maxTransactionOperations: UInt? = null,
    val maxNodeBytes: ULong? = null,
)

data class StoreDescriptor(
    val protocolMajor: UInt,
    val adapterName: String,
    val provider: String,
    val schemaVersion: UInt,
    val capabilities: StoreCapabilities,
    val limits: StoreLimits = StoreLimits(),
)

data class StoreError(
    val code: String,
    val message: String,
    val retryable: Boolean = false,
    val providerCode: String? = null,
)

class StoreException(
    val error: StoreError,
    cause: Throwable? = null,
) : RuntimeException(error.message, cause)

class OptionalBytes private constructor(
    val present: Boolean,
    value: ByteArray,
) {
    private val ownedValue = value.copyOf()

    val value: ByteArray
        get() = ownedValue.copyOf()

    internal fun owned(): ByteArray = ownedValue.copyOf()

    companion object {
        @JvmStatic
        fun missing(): OptionalBytes = OptionalBytes(false, byteArrayOf())

        @JvmStatic
        fun present(value: ByteArray): OptionalBytes = OptionalBytes(true, value)

        @JvmStatic
        fun of(present: Boolean, value: ByteArray): OptionalBytes {
            require(present || value.isEmpty()) { "absent optional bytes must have an empty value" }
            return if (present) present(value) else missing()
        }

        internal fun from(present: Boolean, value: ByteArray): OptionalBytes = of(present, value)
    }
}

sealed interface NodeMutation {
    val cid: ByteArray

    class Upsert(cid: ByteArray, node: ByteArray) : NodeMutation {
        private val ownedCid = cid.copyOf()
        private val ownedNode = node.copyOf()
        override val cid: ByteArray get() = ownedCid.copyOf()
        val node: ByteArray get() = ownedNode.copyOf()
    }

    class Delete(cid: ByteArray) : NodeMutation {
        private val ownedCid = cid.copyOf()
        override val cid: ByteArray get() = ownedCid.copyOf()
    }
}

class NodeEntry(cid: ByteArray, node: ByteArray) {
    private val ownedCid = cid.copyOf()
    private val ownedNode = node.copyOf()
    val cid: ByteArray get() = ownedCid.copyOf()
    val node: ByteArray get() = ownedNode.copyOf()
}

data class PublicationOrigin(val code: UInt)

class NodePublicationHint(namespace: ByteArray, key: ByteArray, value: ByteArray) {
    private val ownedNamespace = namespace.copyOf()
    private val ownedKey = key.copyOf()
    private val ownedValue = value.copyOf()
    val namespace: ByteArray get() = ownedNamespace.copyOf()
    val key: ByteArray get() = ownedKey.copyOf()
    val value: ByteArray get() = ownedValue.copyOf()
}

class NodePublication(
    nodes: List<NodeEntry>,
    val hint: NodePublicationHint?,
    val origin: PublicationOrigin,
) {
    private val ownedNodes = nodes.map { NodeEntry(it.cid, it.node) }
    val nodes: List<NodeEntry> get() = ownedNodes.map { NodeEntry(it.cid, it.node) }
}

class NamedStoreRoot(name: ByteArray, manifest: ByteArray) {
    private val ownedName = name.copyOf()
    private val ownedManifest = manifest.copyOf()
    val name: ByteArray get() = ownedName.copyOf()
    val manifest: ByteArray get() = ownedManifest.copyOf()
}

data class RootCasResult(
    val applied: Boolean,
    val current: OptionalBytes,
)

class RootCondition(name: ByteArray, val expected: OptionalBytes) {
    private val ownedName = name.copyOf()
    val name: ByteArray get() = ownedName.copyOf()
}

sealed interface RootWrite {
    val name: ByteArray

    class Put(name: ByteArray, manifest: ByteArray) : RootWrite {
        private val ownedName = name.copyOf()
        private val ownedManifest = manifest.copyOf()
        override val name: ByteArray get() = ownedName.copyOf()
        val manifest: ByteArray get() = ownedManifest.copyOf()
    }

    class Delete(name: ByteArray) : RootWrite {
        private val ownedName = name.copyOf()
        override val name: ByteArray get() = ownedName.copyOf()
    }
}

class StoreTransactionConflict(
    name: ByteArray,
    val expected: OptionalBytes,
    val current: OptionalBytes,
) {
    private val ownedName = name.copyOf()
    val name: ByteArray get() = ownedName.copyOf()
}

data class StoreTransactionResult(
    val applied: Boolean,
    val conflict: StoreTransactionConflict? = null,
) {
    init {
        require(applied == (conflict == null)) {
            "transaction result must be either applied or one conflict"
        }
    }

    companion object {
        @JvmStatic
        fun applied(): StoreTransactionResult = StoreTransactionResult(true)

        @JvmStatic
        fun conflict(value: StoreTransactionConflict): StoreTransactionResult =
            StoreTransactionResult(false, value)
    }
}

interface RemoteStore {
    suspend fun descriptor(): StoreDescriptor
    suspend fun getNode(cid: ByteArray): OptionalBytes
    suspend fun putNode(cid: ByteArray, value: ByteArray)
    suspend fun deleteNode(cid: ByteArray)
    suspend fun batchNodes(operations: List<NodeMutation>)
    suspend fun publishNodes(publication: NodePublication) {
        normalizePublicationOriginCode(publication.origin.code)
        val hint = publication.hint
        if (hint != null) {
            batchPutNodesWithHint(
                publication.nodes,
                hint.namespace,
                hint.key,
                hint.value,
            )
        } else {
            batchNodes(publication.nodes.map { NodeMutation.Upsert(it.cid, it.node) })
        }
    }
    suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes>
    suspend fun listNodeCids(): List<ByteArray>
    suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytes
    suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray)
    suspend fun batchPutNodesWithHint(
        nodes: List<NodeEntry>,
        namespace: ByteArray,
        key: ByteArray,
        value: ByteArray,
    )
    suspend fun getRootManifest(name: ByteArray): OptionalBytes
    suspend fun putRootManifest(name: ByteArray, manifest: ByteArray)
    suspend fun deleteRootManifest(name: ByteArray)
    suspend fun compareAndSwapRootManifest(
        name: ByteArray,
        expected: OptionalBytes,
        replacement: OptionalBytes,
    ): RootCasResult
    suspend fun listRootManifests(): List<NamedStoreRoot>
    suspend fun commitTransaction(
        nodes: List<NodeMutation>,
        conditions: List<RootCondition>,
        roots: List<RootWrite>,
    ): StoreTransactionResult
}

fun validateStoreDescriptor(descriptor: StoreDescriptor): StoreDescriptor {
    require(descriptor.protocolMajor == STORE_PROTOCOL_MAJOR) {
        "protocol major must be $STORE_PROTOCOL_MAJOR, got ${descriptor.protocolMajor}"
    }
    require(descriptor.adapterName.isNotBlank()) { "adapter name must not be empty" }
    require(descriptor.provider.isNotBlank()) { "provider must not be empty" }
    require(descriptor.schemaVersion >= 1u) { "schema version must be at least 1" }
    require(descriptor.capabilities.readParallelism >= 1u) {
        "read parallelism must be at least 1"
    }
    require(!descriptor.capabilities.atomicNodesAndHint || descriptor.capabilities.hints) {
        "atomic nodes and hint requires hints support"
    }
    requirePositive("max batch read items", descriptor.limits.maxBatchReadItems)
    requirePositive("max batch write items", descriptor.limits.maxBatchWriteItems)
    requirePositive("max transaction operations", descriptor.limits.maxTransactionOperations)
    require(descriptor.limits.maxNodeBytes == null || descriptor.limits.maxNodeBytes > 0uL) {
        "max node bytes must be at least 1 when present"
    }
    return descriptor
}

private fun requirePositive(name: String, value: UInt?) {
    require(value == null || value > 0u) { "$name must be at least 1 when present" }
}
