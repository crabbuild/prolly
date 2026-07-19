package build.crab.prolly.store.dynamodb

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
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import software.amazon.awssdk.services.dynamodb.model.Delete
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement
import software.amazon.awssdk.services.dynamodb.model.KeyType
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes
import software.amazon.awssdk.services.dynamodb.model.Put
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutRequest
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.TableDescription
import software.amazon.awssdk.services.dynamodb.model.TableStatus
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException
import software.amazon.awssdk.services.dynamodb.model.WriteRequest

data class DynamoDbStoreOptions(
    val tableName: String,
    val keyPrefix: ByteArray = "prolly:".encodeToByteArray(),
    val adapterName: String = "dynamodb-v1",
    val readParallelism: UInt = 16u,
)

class DynamoDbStore constructor(
    private val client: DynamoDbAsyncClient,
    options: DynamoDbStoreOptions,
) : RemoteStore, AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val javaScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val tableName = options.tableName.also { require(it.isNotBlank()) { "DynamoDB table name is required" } }
    private val keyPrefix = options.keyPrefix.copyOf()
    private val storeDescriptor = validateStoreDescriptor(StoreDescriptor(
        2u, options.adapterName.ifBlank { "dynamodb-v1" }, "dynamodb", 1u,
        StoreCapabilities(true, false, true, true, false, true, true, true, options.readParallelism),
        StoreLimits(maxBatchReadItems = 100u, maxBatchWriteItems = 25u, maxTransactionOperations = 100u),
    ))

    constructor(client: DynamoDbAsyncClient, tableName: String, keyPrefix: ByteArray) : this(client, DynamoDbStoreOptions(tableName, keyPrefix))

    suspend fun initializeTable() = operation("initialize_table") {
        try { validateTable(client.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).await().table()); return@operation }
        catch (_: ResourceNotFoundException) { }
        try {
            client.createTable(CreateTableRequest.builder().tableName(tableName)
                .attributeDefinitions(AttributeDefinition.builder().attributeName(PK).attributeType(ScalarAttributeType.B).build())
                .keySchema(KeySchemaElement.builder().attributeName(PK).keyType(KeyType.HASH).build())
                .billingMode(BillingMode.PAY_PER_REQUEST).build()).await()
        } catch (_: ResourceInUseException) { }
        repeat(100) {
            try { val table = client.describeTable(DescribeTableRequest.builder().tableName(tableName).build()).await().table(); if (table.tableStatus() == TableStatus.ACTIVE) { validateTable(table); return@operation } }
            catch (_: ResourceNotFoundException) { }
            delay(50)
        }
        throw StoreException(StoreError("unavailable", "DynamoDB table did not become active", true))
    }

    fun initializeTableAsync(): CompletableFuture<Unit> = javaScope.future { initializeTable() }
    suspend fun deleteTable() = operation("delete_table") { try { client.deleteTable(DeleteTableRequest.builder().tableName(tableName).build()).await() } catch (_: ResourceNotFoundException) { } }
    override suspend fun descriptor(): StoreDescriptor = operation("descriptor") { storeDescriptor }
    override suspend fun getNode(cid: ByteArray): OptionalBytes = get(familyKey(NODE, cid), "get_node")
    override suspend fun putNode(cid: ByteArray, value: ByteArray) = put(familyKey(NODE, cid), value, "put_node")
    override suspend fun deleteNode(cid: ByteArray) = delete(familyKey(NODE, cid), "delete_node")

    override suspend fun batchNodes(operations: List<NodeMutation>) {
        val requests = operations.map { when (it) {
            is NodeMutation.Upsert -> WriteRequest.builder().putRequest(PutRequest.builder().item(item(familyKey(NODE, it.cid), it.node)).build()).build()
            is NodeMutation.Delete -> WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(keyItem(familyKey(NODE, it.cid))).build()).build()
        } }
        operation("batch_nodes") { batchWrite(requests) }
    }

    override suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes> {
        val storageKeys = cids.map { familyKey(NODE, it) }
        return operation("batch_get_nodes_ordered") {
            val values = HashMap<String, ByteArray>()
            storageKeys.distinctByteArrays().chunked(BATCH_GET_LIMIT).forEach { chunk ->
                var pending = chunk.map(::keyItem)
                repeat(RETRY_LIMIT) { attempt ->
                    if (pending.isEmpty()) return@repeat
                    val request = BatchGetItemRequest.builder().requestItems(mapOf(tableName to KeysAndAttributes.builder().keys(pending).consistentRead(true).projectionExpression("#pk, #value").expressionAttributeNames(mapOf("#pk" to PK, "#value" to VALUE)).build())).build()
                    val output = client.batchGetItem(request).await()
                    output.responses()[tableName].orEmpty().forEach { values[binary(it, PK).hex()] = binary(it, VALUE) }
                    pending = output.unprocessedKeys()[tableName]?.keys().orEmpty()
                    if (pending.isNotEmpty()) { if (attempt + 1 == RETRY_LIMIT) throw limit("DynamoDB batch get left ${pending.size} keys unprocessed"); delay(10L shl minOf(attempt, 6)) }
                }
            }
            storageKeys.map { values[it.hex()]?.let(OptionalBytes::present) ?: OptionalBytes.missing() }
        }
    }

    override suspend fun listNodeCids(): List<ByteArray> = operation("list_node_cids") { val prefix = keyPrefix + NODE; scanKeys(prefix).map { it.copyOfRange(prefix.size, it.size) }.filter { it.size == 32 }.sortedWith(BYTE_ARRAY_COMPARATOR) }
    override suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytes = get(hintKey(namespace, key), "get_hint")
    override suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray) = put(hintKey(namespace, key), value, "put_hint")
    override suspend fun batchPutNodesWithHint(nodes: List<NodeEntry>, namespace: ByteArray, key: ByteArray, value: ByteArray) { batchNodes(nodes.map { NodeMutation.Upsert(it.cid, it.node) }); putHint(namespace, key, value) }
    override suspend fun getRootManifest(name: ByteArray): OptionalBytes = get(familyKey(ROOT, name), "get_root_manifest")
    override suspend fun putRootManifest(name: ByteArray, manifest: ByteArray) = put(familyKey(ROOT, name), manifest, "put_root_manifest")
    override suspend fun deleteRootManifest(name: ByteArray) = delete(familyKey(ROOT, name), "delete_root_manifest")

    override suspend fun compareAndSwapRootManifest(name: ByteArray, expected: OptionalBytes, replacement: OptionalBytes): RootCasResult {
        val key = familyKey(ROOT, name); val wanted = OptionalBytes.of(expected.present, expected.value); val next = OptionalBytes.of(replacement.present, replacement.value)
        return operation("compare_and_swap_root_manifest") {
            try {
                if (next.present) { val builder = PutItemRequest.builder().tableName(tableName).item(item(key, next.value)).returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD); applyCondition(builder, wanted); client.putItem(builder.build()).await() }
                else { val builder = DeleteItemRequest.builder().tableName(tableName).key(keyItem(key)).returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD); applyCondition(builder, wanted); client.deleteItem(builder.build()).await() }
                RootCasResult(true, OptionalBytes.of(next.present, next.value))
            } catch (_: ConditionalCheckFailedException) { RootCasResult(false, getRaw(key)) }
        }
    }

    override suspend fun listRootManifests(): List<NamedStoreRoot> = operation("list_root_manifests") {
        val prefix = keyPrefix + ROOT; scanKeys(prefix).map { it.copyOfRange(prefix.size, it.size) }.sortedWith(BYTE_ARRAY_COMPARATOR).mapNotNull { name -> val value = getRaw(familyKey(ROOT, name)); if (value.present) NamedStoreRoot(name, value.value) else null }
    }

    override suspend fun commitTransaction(nodes: List<NodeMutation>, conditions: List<RootCondition>, roots: List<RootWrite>): StoreTransactionResult {
        val ownedNodes = nodes.map(::cloneMutation); val ownedConditions = conditions.map { RootCondition(it.name, OptionalBytes.of(it.expected.present, it.expected.value)) }; val ownedRoots = roots.map(::cloneRootWrite)
        val written = ownedRoots.map { it.name.hex() }.toSet(); val count = ownedNodes.size + ownedRoots.size + ownedConditions.count { it.name.hex() !in written }
        if (count > TRANSACTION_LIMIT) throw limit("DynamoDB transaction has $count operations, exceeding the $TRANSACTION_LIMIT operation limit")
        return operation("commit_transaction") {
            val conditionByName = ownedConditions.associateBy { it.name.hex() }; val items = mutableListOf<TransactWriteItem>()
            ownedConditions.filter { it.name.hex() !in written }.forEach { condition -> val builder = ConditionCheck.builder().tableName(tableName).key(keyItem(familyKey(ROOT, condition.name))).returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD); applyCondition(builder, condition.expected); items += TransactWriteItem.builder().conditionCheck(builder.build()).build() }
            ownedRoots.forEach { root ->
                val condition = conditionByName[root.name.hex()]
                when (root) {
                    is RootWrite.Put -> { val builder = Put.builder().tableName(tableName).item(item(familyKey(ROOT, root.name), root.manifest)); if (condition != null) { builder.returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD); applyCondition(builder, condition.expected) }; items += TransactWriteItem.builder().put(builder.build()).build() }
                    is RootWrite.Delete -> { val builder = Delete.builder().tableName(tableName).key(keyItem(familyKey(ROOT, root.name))); if (condition != null) { builder.returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD); applyCondition(builder, condition.expected) }; items += TransactWriteItem.builder().delete(builder.build()).build() }
                }
            }
            ownedNodes.forEach { node -> items += when (node) { is NodeMutation.Upsert -> TransactWriteItem.builder().put(Put.builder().tableName(tableName).item(item(familyKey(NODE, node.cid), node.node)).build()).build(); is NodeMutation.Delete -> TransactWriteItem.builder().delete(Delete.builder().tableName(tableName).key(keyItem(familyKey(NODE, node.cid))).build()).build() } }
            if (items.isEmpty()) return@operation StoreTransactionResult.applied()
            try { client.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(items).build()).await(); StoreTransactionResult.applied() }
            catch (error: TransactionCanceledException) {
                ownedConditions.forEach { condition -> val current = getRaw(familyKey(ROOT, condition.name)); if (!optionalEqual(current, condition.expected)) return@operation StoreTransactionResult.conflict(StoreTransactionConflict(condition.name, condition.expected, current)) }
                throw error
            }
        }
    }

    suspend fun clearNamespace() { if (keyPrefix.isEmpty()) throw StoreException(StoreError("invalid_argument", "refusing to clear an empty DynamoDB key prefix")); operation("clear_namespace") { batchWrite(scanKeys(keyPrefix).map { WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(keyItem(it)).build()).build() }) } }
    override fun close() { if (closed.compareAndSet(false, true)) javaScope.cancel() }

    private suspend fun get(key: ByteArray, name: String) = operation(name) { getRaw(key) }
    private suspend fun getRaw(key: ByteArray): OptionalBytes { val item = client.getItem(GetItemRequest.builder().tableName(tableName).key(keyItem(key)).consistentRead(true).projectionExpression("#value").expressionAttributeNames(mapOf("#value" to VALUE)).build()).await().item(); return if (item.isEmpty()) OptionalBytes.missing() else OptionalBytes.present(binary(item, VALUE)) }
    private suspend fun put(key: ByteArray, value: ByteArray, name: String) { val owned = value.copyOf(); operation(name) { client.putItem(PutItemRequest.builder().tableName(tableName).item(item(key, owned)).build()).await() } }
    private suspend fun delete(key: ByteArray, name: String) { operation(name) { client.deleteItem(DeleteItemRequest.builder().tableName(tableName).key(keyItem(key)).build()).await() } }
    private suspend fun batchWrite(requests: List<WriteRequest>) { requests.chunked(BATCH_WRITE_LIMIT).forEach { chunk -> var pending = chunk; repeat(RETRY_LIMIT) { attempt -> if (pending.isEmpty()) return@repeat; val output = client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(mapOf(tableName to pending)).build()).await(); pending = output.unprocessedItems()[tableName].orEmpty(); if (pending.isNotEmpty()) { if (attempt + 1 == RETRY_LIMIT) throw limit("DynamoDB batch write left ${pending.size} requests unprocessed"); delay(10L shl minOf(attempt, 6)) } } } }
    private suspend fun scanKeys(prefix: ByteArray): List<ByteArray> { val keys = mutableListOf<ByteArray>(); var start: Map<String, AttributeValue>? = null; do { val output = client.scan(ScanRequest.builder().tableName(tableName).consistentRead(true).projectionExpression("#pk").filterExpression("begins_with(#pk, :prefix)").expressionAttributeNames(mapOf("#pk" to PK)).expressionAttributeValues(mapOf(":prefix" to attribute(prefix))).exclusiveStartKey(start).build()).await(); output.items().forEach { keys += binary(it, PK) }; start = output.lastEvaluatedKey() } while (!start.isNullOrEmpty()); return keys }
    private fun familyKey(family: ByteArray, suffix: ByteArray) = keyPrefix + family + suffix.copyOf()
    private fun hintKey(namespace: ByteArray, key: ByteArray) = keyPrefix + HINT + ByteBuffer.allocate(8).putLong(namespace.size.toLong()).array() + namespace.copyOf() + key.copyOf()
    private suspend fun <T> operation(name: String, block: suspend () -> T): T { if (closed.get()) throw StoreException(StoreError("internal", "DynamoDB store is closed")); return try { block() } catch (error: CancellationException) { throw error } catch (error: StoreException) { throw error } catch (error: Throwable) { throw dynamoError(name, unwrap(error)) } }
}

private const val PK = "pk"; private const val VALUE = "value"; private const val BATCH_GET_LIMIT = 100; private const val BATCH_WRITE_LIMIT = 25; private const val TRANSACTION_LIMIT = 100; private const val RETRY_LIMIT = 8
private val NODE = "node:".encodeToByteArray(); private val ROOT = "root:".encodeToByteArray(); private val HINT = "hint:".encodeToByteArray()
private fun attribute(value: ByteArray) = AttributeValue.builder().b(SdkBytes.fromByteArray(value.copyOf())).build()
private fun keyItem(key: ByteArray) = mapOf(PK to attribute(key))
private fun item(key: ByteArray, value: ByteArray) = mapOf(PK to attribute(key), VALUE to attribute(value))
private fun binary(item: Map<String, AttributeValue>, name: String): ByteArray = item[name]?.b()?.asByteArray()?.copyOf() ?: throw StoreException(StoreError("invalid_data", "DynamoDB item has invalid $name attribute"))
private fun applyCondition(builder: PutItemRequest.Builder, expected: OptionalBytes) { if (expected.present) builder.conditionExpression("#value = :expected").expressionAttributeNames(mapOf("#value" to VALUE)).expressionAttributeValues(mapOf(":expected" to attribute(expected.value))) else builder.conditionExpression("attribute_not_exists(#pk)").expressionAttributeNames(mapOf("#pk" to PK)) }
private fun applyCondition(builder: DeleteItemRequest.Builder, expected: OptionalBytes) { if (expected.present) builder.conditionExpression("#value = :expected").expressionAttributeNames(mapOf("#value" to VALUE)).expressionAttributeValues(mapOf(":expected" to attribute(expected.value))) else builder.conditionExpression("attribute_not_exists(#pk)").expressionAttributeNames(mapOf("#pk" to PK)) }
private fun applyCondition(builder: ConditionCheck.Builder, expected: OptionalBytes) { if (expected.present) builder.conditionExpression("#value = :expected").expressionAttributeNames(mapOf("#value" to VALUE)).expressionAttributeValues(mapOf(":expected" to attribute(expected.value))) else builder.conditionExpression("attribute_not_exists(#pk)").expressionAttributeNames(mapOf("#pk" to PK)) }
private fun applyCondition(builder: Put.Builder, expected: OptionalBytes) { if (expected.present) builder.conditionExpression("#value = :expected").expressionAttributeNames(mapOf("#value" to VALUE)).expressionAttributeValues(mapOf(":expected" to attribute(expected.value))) else builder.conditionExpression("attribute_not_exists(#pk)").expressionAttributeNames(mapOf("#pk" to PK)) }
private fun applyCondition(builder: Delete.Builder, expected: OptionalBytes) { if (expected.present) builder.conditionExpression("#value = :expected").expressionAttributeNames(mapOf("#value" to VALUE)).expressionAttributeValues(mapOf(":expected" to attribute(expected.value))) else builder.conditionExpression("attribute_not_exists(#pk)").expressionAttributeNames(mapOf("#pk" to PK)) }
private fun validateTable(table: TableDescription?) { if (table == null || table.keySchema().size != 1 || table.keySchema()[0].attributeName() != PK || table.keySchema()[0].keyType() != KeyType.HASH || table.attributeDefinitions().none { it.attributeName() == PK && it.attributeType() == ScalarAttributeType.B }) throw StoreException(StoreError("invalid_argument", "DynamoDB table must use one binary HASH key named pk")) }
private fun cloneMutation(value: NodeMutation): NodeMutation = when (value) { is NodeMutation.Upsert -> NodeMutation.Upsert(value.cid, value.node); is NodeMutation.Delete -> NodeMutation.Delete(value.cid) }
private fun cloneRootWrite(value: RootWrite): RootWrite = when (value) { is RootWrite.Put -> RootWrite.Put(value.name, value.manifest); is RootWrite.Delete -> RootWrite.Delete(value.name) }
private fun optionalEqual(left: OptionalBytes, right: OptionalBytes) = left.present == right.present && (!left.present || left.value.contentEquals(right.value))
private fun List<ByteArray>.distinctByteArrays(): List<ByteArray> { val seen = HashSet<String>(); return filter { seen.add(it.hex()) }.map(ByteArray::copyOf) }
private fun ByteArray.hex() = joinToString("") { "%02x".format(it.toInt() and 0xff) }
private val BYTE_ARRAY_COMPARATOR = Comparator<ByteArray> { left, right -> val common = minOf(left.size, right.size); for (index in 0 until common) { val compared = left[index].toUByte().compareTo(right[index].toUByte()); if (compared != 0) return@Comparator compared }; left.size.compareTo(right.size) }
private fun limit(message: String) = StoreException(StoreError("resource_exhausted", message))
private fun unwrap(error: Throwable): Throwable = if (error is CompletionException && error.cause != null) error.cause!! else error
private fun dynamoError(operation: String, error: Throwable): StoreException { if (error is StoreException) return error; val name = error.javaClass.simpleName; val retryable = error is DynamoDbException && error.isThrottlingException; return StoreException(StoreError(if (retryable) "unavailable" else "internal", "DynamoDB operation failed", retryable, "dynamodb:$name:$operation"), error) }
