# Prolly Java Cookbook

The Java facade wraps the generated Kotlin/JVM UniFFI binding with Java
collections, `Optional`, and `CompletableFuture` helpers. The package name is
`build.crab.prolly`.

Runnable scenarios live as separate classes under
`build.crab.prolly.examples`, matching the Rust example style:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
mvn -q -f bindings/pom.xml install -Dmaven.test.skip=true
mvn -q -f bindings/java/pom.xml \
  compile \
  -Dexec.mainClass=build.crab.prolly.examples.CookbookScenarios \
  exec:java
mvn -q -f bindings/java/pom.xml \
  compile \
  -Dexec.mainClass=build.crab.prolly.examples.BasicMap \
  exec:java
mvn -q -f bindings/java/pom.xml \
  compile \
  -Dexec.mainClass=build.crab.prolly.examples.SecondaryIndex \
  exec:java
```

Application-style classes include `BatchBuild`, `LocalFirstState`,
`Resolver`, `CrdtMerge`, `ConversationMemory`, `AgentEventLog`,
`BackgroundCompaction`, `DeterministicRagSnapshot`, `DocumentChunkIndex`,
`VectorSidecar`, `ProvenanceValues`, `MaterializedView`,
`FilesystemSnapshot`, and `DurableSqlite`.

## Build And Force A TurboQuant RAG Sidecar

TurboQuant is a disposable routing sidecar. The proximity map remains
authoritative and every shortlisted record is reranked from its full-precision
vector. Keep selection explicit until qualification enables `Auto`.

```java
import build.crab.prolly.javaapi.*;
import java.util.ArrayList;

var records = new ArrayList<ProximityRecord>();
for (int index = 0; index < 32; index++) {
    records.add(new ProximityRecord(
            String.format("chunk/%02d", index).getBytes(),
            new float[] {index, index % 3, 0, 1, 2, 3, 4, 5},
            String.format("document-%02d", index).getBytes()));
}

try (Engine engine = Engine.memory();
     ProximityMap proximity = engine.buildProximity(8, records)) {
    var built = proximity.buildTurboquant(TurboQuantizationConfig.defaults(), 2);
    var request = SearchRequest.fixedBudget(
            new float[] {0, 0, 0, 1, 2, 3, 4, 5},
            3,
            SearchRequest.SearchBudget.unlimited(),
            SearchRequest.SearchFilter.all(),
            SearchRequest.Kernel.AUTO_DETERMINISTIC,
            SearchRequest.Backend.TURBO_QUANTIZED);

    byte[] manifest;
    try (var index = built.index()) {
        if (index.verify(proximity).encodedVectors() != records.size()) {
            throw new IllegalStateException("incomplete TurboQuant sidecar");
        }
        if (!index.search(proximity, request).backend().equals("turbo_quantized")) {
            throw new IllegalStateException("unexpected backend");
        }
        manifest = index.manifest();
    }
    try (var reopened = proximity.loadTurboquant(manifest)) {
        // The manifest CID is the stable handle for this immutable sidecar.
    }
}
```

## Create A Durable Index

```java
import build.crab.prolly.*;
import java.nio.file.Path;
import java.util.Optional;
import java.util.List;

public final class App {
    public static void main(String[] args) throws Exception {
        Prolly.useLocalDebugLibrary();

        try (Prolly engine = Prolly.sqlite(Path.of("app.prolly.db"))) {
            TreeRecord tree = engine.create();
            tree = engine.batch(tree, List.of(
                    Prolly.upsert("user/1".getBytes(), "Ada".getBytes()),
                    Prolly.upsert("user/2".getBytes(), "Linus".getBytes())));

            engine.publishNamedRoot("users/main".getBytes(), tree);
        }
    }
}
```

## Prefix Queries And Pages

```java
byte[] prefix = "user/".getBytes();
byte[] end = Prolly.prefixEnd(prefix);

for (Entry entry : engine.range(tree, prefix, Optional.ofNullable(end))) {
    System.out.printf("%s=%s%n", new String(entry.key()), new String(entry.value()));
}

RangeCursorRecord cursor = null;
while (true) {
    RangePageRecord page = engine.rangePage(tree, cursor, Optional.empty(), 100);
    for (EntryRecord entry : page.getEntries()) {
        handle(entry.getKey(), entry.getValue());
    }
    cursor = page.getNextCursor();
    if (cursor == null) {
        break;
    }
}

List<DiffRecord> diffs = engine.diffFromCursor(
        oldTree,
        newTree,
        new RangeCursorRecord("user/42".getBytes()),
        Optional.empty());
```

## Use CompletableFuture Wrappers

```java
try (AsyncProlly async = AsyncProlly.memory()) {
    TreeRecord tree = async.create().get();
    tree = async.put(tree, "k".getBytes(), "v".getBytes()).get();
    Optional<byte[]> value = async.get(tree, "k".getBytes()).get();
}
```

## Merge Writers

```java
TreeRecord base = tree;
TreeRecord left = engine.put(base, "user/1".getBytes(), "Ada Lovelace".getBytes());
TreeRecord right = engine.put(base, "user/1".getBytes(), "Countess Ada".getBytes());

TreeRecord merged = engine.merge(base, left, right, "prefer_right");

TreeRecord callbackMerged = engine.mergeWithResolver(base, left, right, conflict -> {
    if (conflict.getLeft() != null && conflict.getRight() != null) {
        byte[] value = (new String(conflict.getLeft()) + " | " + new String(conflict.getRight())).getBytes();
        return new ResolutionRecord(ResolutionKind.VALUE, value);
    }
    return new ResolutionRecord(ResolutionKind.UNRESOLVED, null);
});
```

## Large Values And Blob GC

```java
try (BlobStore blobStore = BlobStore.file(Path.of("app.blobs"))) {
    byte[] large = new byte[1_000_000];
    TreeRecord withLarge = engine.putLargeValue(
            blobStore,
            tree,
            "doc/1".getBytes(),
            large,
            Prolly.largeValueConfig(4096));

    Optional<byte[]> loaded = engine.getLargeValue(blobStore, withLarge, "doc/1".getBytes());
    BlobGcPlan plan = engine.planBlobStoreGc(blobStore, List.of(withLarge));
    if (plan.reclaimableBlobCount() > 0) {
        engine.sweepBlobStoreGc(blobStore, List.of(withLarge));
    }
}
```

## Custom Stores

Implement `HostStore` when Java owns persistence. The Java adapter converts
exceptions into UniFFI callback result records.

```java
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class MemoryHostStore implements HostStore {
    private final Map<List<Byte>, byte[]> nodes = new HashMap<>();

    @Override
    public Optional<byte[]> get(byte[] key) {
        return Optional.ofNullable(nodes.get(toKey(key))).map(byte[]::clone);
    }

    @Override
    public void put(byte[] key, byte[] value) {
        nodes.put(toKey(key), value.clone());
    }

    @Override
    public void delete(byte[] key) {
        nodes.remove(toKey(key));
    }

    @Override
    public List<byte[]> listNodeCids() {
        return nodes.keySet().stream().map(MemoryHostStore::fromKey).toList();
    }

    private static List<Byte> toKey(byte[] bytes) {
        List<Byte> out = new ArrayList<>(bytes.length);
        for (byte b : bytes) out.add(b);
        return out;
    }

    private static byte[] fromKey(List<Byte> key) {
        byte[] out = new byte[key.size()];
        for (int i = 0; i < key.size(); i++) out[i] = key.get(i);
        return out;
    }
}

try (Prolly engine = Prolly.customStore(new MemoryHostStore())) {
    TreeRecord tree = engine.create();
}
```
