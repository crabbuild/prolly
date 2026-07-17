package build.crab.prolly.javaapi.remote;

import build.crab.prolly.MutationRecord;
import build.crab.prolly.NamedRootUpdateRecord;
import build.crab.prolly.TransactionUpdateRecord;
import build.crab.prolly.TreeRecord;
import build.crab.prolly.javaapi.remote.internal.RemoteJavaTransactionBridge;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class RemoteProllyTransaction implements AutoCloseable {
    private final RemoteJavaTransactionBridge bridge;

    RemoteProllyTransaction(RemoteJavaTransactionBridge bridge) {
        this.bridge = bridge;
    }

    public CompletableFuture<TreeRecord> create() {
        return bridge.create();
    }

    public CompletableFuture<byte[]> get(TreeRecord tree, byte[] key) {
        return bridge.get(tree, key);
    }

    public CompletableFuture<TreeRecord> put(TreeRecord tree, byte[] key, byte[] value) {
        return bridge.put(tree, key, value);
    }

    public CompletableFuture<TreeRecord> delete(TreeRecord tree, byte[] key) {
        return bridge.delete(tree, key);
    }

    public CompletableFuture<TreeRecord> batch(TreeRecord tree, List<MutationRecord> mutations) {
        return bridge.batch(tree, mutations);
    }

    public CompletableFuture<TreeRecord> loadNamedRoot(byte[] name) {
        return bridge.loadNamedRoot(name);
    }

    public CompletableFuture<Void> publishNamedRoot(byte[] name, TreeRecord tree) {
        return bridge.publishNamedRoot(name, tree);
    }

    public CompletableFuture<Void> deleteNamedRoot(byte[] name) {
        return bridge.deleteNamedRoot(name);
    }

    public CompletableFuture<NamedRootUpdateRecord> compareAndSwapNamedRoot(
            byte[] name,
            TreeRecord expected,
            TreeRecord replacement) {
        return bridge.compareAndSwapNamedRoot(name, expected, replacement);
    }

    public CompletableFuture<TransactionUpdateRecord> commit() {
        return bridge.commit();
    }

    public CompletableFuture<Void> rollback() {
        return bridge.rollback();
    }

    @Override
    public void close() {
        bridge.close();
    }
}
