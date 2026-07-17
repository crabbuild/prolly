package build.crab.prolly.javaapi.remote.internal

import build.crab.prolly.ConfigRecord
import build.crab.prolly.EntryRecord
import build.crab.prolly.MutationRecord
import build.crab.prolly.NamedRootRecord
import build.crab.prolly.NamedRootUpdateRecord
import build.crab.prolly.TransactionUpdateRecord
import build.crab.prolly.TreeRecord
import build.crab.prolly.remote.RemoteProlly
import build.crab.prolly.remote.RemoteProllyTransaction
import build.crab.prolly.remote.RemoteStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

class RemoteJavaBridge private constructor(
    private val remote: RemoteProlly,
    private val scope: CoroutineScope,
) : AutoCloseable {
    companion object {
        @JvmStatic
        fun open(
            store: RemoteStore,
            config: ConfigRecord,
            executor: Executor,
        ): CompletableFuture<RemoteJavaBridge> {
            val scope = CoroutineScope(SupervisorJob() + executor.asCoroutineDispatcher())
            val future = scope.future { RemoteJavaBridge(RemoteProlly.open(store, config), scope) }
            future.whenComplete { _, error ->
                if (error != null || future.isCancelled) scope.cancel()
            }
            return future
        }
    }

    fun create(): TreeRecord = remote.create()

    fun get(tree: TreeRecord, key: ByteArray): CompletableFuture<ByteArray?> =
        scope.future { remote.get(tree, key) }

    fun getMany(tree: TreeRecord, keys: List<ByteArray>): CompletableFuture<List<ByteArray?>> =
        scope.future { remote.getMany(tree, keys) }

    fun put(tree: TreeRecord, key: ByteArray, value: ByteArray): CompletableFuture<TreeRecord> =
        scope.future { remote.put(tree, key, value) }

    fun delete(tree: TreeRecord, key: ByteArray): CompletableFuture<TreeRecord> =
        scope.future { remote.delete(tree, key) }

    fun batch(tree: TreeRecord, mutations: List<MutationRecord>): CompletableFuture<TreeRecord> =
        scope.future { remote.batch(tree, mutations) }

    fun range(
        tree: TreeRecord,
        start: ByteArray,
        end: ByteArray?,
    ): CompletableFuture<List<EntryRecord>> = scope.future { remote.range(tree, start, end) }

    fun loadNamedRoot(name: ByteArray): CompletableFuture<TreeRecord?> =
        scope.future { remote.loadNamedRoot(name) }

    fun listNamedRoots(): CompletableFuture<List<NamedRootRecord>> =
        scope.future { remote.listNamedRoots() }

    fun publishNamedRoot(name: ByteArray, tree: TreeRecord): CompletableFuture<Void?> =
        scope.future {
            remote.publishNamedRoot(name, tree)
            null
        }

    fun deleteNamedRoot(name: ByteArray): CompletableFuture<Void?> =
        scope.future {
            remote.deleteNamedRoot(name)
            null
        }

    fun compareAndSwapNamedRoot(
        name: ByteArray,
        expected: TreeRecord?,
        replacement: TreeRecord?,
    ): CompletableFuture<NamedRootUpdateRecord> =
        scope.future { remote.compareAndSwapNamedRoot(name, expected, replacement) }

    fun beginTransaction(): CompletableFuture<RemoteJavaTransactionBridge> =
        scope.future { RemoteJavaTransactionBridge(remote.beginTransaction(), scope) }

    override fun close() {
        scope.cancel()
        remote.close()
    }
}

class RemoteJavaTransactionBridge internal constructor(
    private val transaction: RemoteProllyTransaction,
    private val scope: CoroutineScope,
) : AutoCloseable {
    fun create(): CompletableFuture<TreeRecord> = scope.future { transaction.create() }

    fun get(tree: TreeRecord, key: ByteArray): CompletableFuture<ByteArray?> =
        scope.future { transaction.get(tree, key) }

    fun put(tree: TreeRecord, key: ByteArray, value: ByteArray): CompletableFuture<TreeRecord> =
        scope.future { transaction.put(tree, key, value) }

    fun delete(tree: TreeRecord, key: ByteArray): CompletableFuture<TreeRecord> =
        scope.future { transaction.delete(tree, key) }

    fun batch(tree: TreeRecord, mutations: List<MutationRecord>): CompletableFuture<TreeRecord> =
        scope.future { transaction.batch(tree, mutations) }

    fun loadNamedRoot(name: ByteArray): CompletableFuture<TreeRecord?> =
        scope.future { transaction.loadNamedRoot(name) }

    fun publishNamedRoot(name: ByteArray, tree: TreeRecord): CompletableFuture<Void?> =
        scope.future {
            transaction.publishNamedRoot(name, tree)
            null
        }

    fun deleteNamedRoot(name: ByteArray): CompletableFuture<Void?> =
        scope.future {
            transaction.deleteNamedRoot(name)
            null
        }

    fun compareAndSwapNamedRoot(
        name: ByteArray,
        expected: TreeRecord?,
        replacement: TreeRecord?,
    ): CompletableFuture<NamedRootUpdateRecord> =
        scope.future { transaction.compareAndSwapNamedRoot(name, expected, replacement) }

    fun commit(): CompletableFuture<TransactionUpdateRecord> = scope.future { transaction.commit() }

    fun rollback(): CompletableFuture<Void?> = scope.future {
        transaction.rollback()
        null
    }

    override fun close() {
        transaction.close()
    }
}
