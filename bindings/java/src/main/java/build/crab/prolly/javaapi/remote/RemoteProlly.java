package build.crab.prolly.javaapi.remote;

import build.crab.prolly.ConfigRecord;
import build.crab.prolly.EntryRecord;
import build.crab.prolly.MutationRecord;
import build.crab.prolly.NamedRootRecord;
import build.crab.prolly.NamedRootUpdateRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TreeRecord;
import build.crab.prolly.javaapi.remote.internal.RemoteJavaBridge;
import build.crab.prolly.remote.RemoteStore;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public final class RemoteProlly implements AutoCloseable {
    private final RemoteJavaBridge bridge;

    private RemoteProlly(RemoteJavaBridge bridge) {
        this.bridge = bridge;
    }

    public static CompletableFuture<RemoteProlly> open(RemoteStore store) {
        return open(store, Prolly.defaultConfig(), ForkJoinPool.commonPool());
    }

    public static CompletableFuture<RemoteProlly> open(RemoteStore store, Executor executor) {
        return open(store, Prolly.defaultConfig(), executor);
    }

    public static CompletableFuture<RemoteProlly> open(
            RemoteStore store,
            ConfigRecord config,
            Executor executor) {
        return RemoteJavaBridge.open(store, config, executor).thenApply(RemoteProlly::new);
    }

    public TreeRecord create() {
        return bridge.create();
    }

    public CompletableFuture<byte[]> get(TreeRecord tree, byte[] key) {
        return bridge.get(tree, key);
    }

    public CompletableFuture<List<byte[]>> getMany(TreeRecord tree, List<byte[]> keys) {
        return bridge.getMany(tree, keys);
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

    public CompletableFuture<List<EntryRecord>> range(TreeRecord tree, byte[] start, byte[] end) {
        return bridge.range(tree, start, end);
    }

    public CompletableFuture<TreeRecord> loadNamedRoot(byte[] name) {
        return bridge.loadNamedRoot(name);
    }

    public CompletableFuture<List<NamedRootRecord>> listNamedRoots() {
        return bridge.listNamedRoots();
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

    public CompletableFuture<RemoteProllyTransaction> beginTransaction() {
        return bridge.beginTransaction().thenApply(RemoteProllyTransaction::new);
    }

    @Override
    public void close() {
        bridge.close();
    }
}
