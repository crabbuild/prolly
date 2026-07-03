package build.crab.prolly.examples;

import build.crab.prolly.BlobStore;
import build.crab.prolly.CrdtDeletePolicyKind;
import build.crab.prolly.Entry;
import build.crab.prolly.GcPlan;
import build.crab.prolly.MutationRecord;
import build.crab.prolly.Prolly;
import build.crab.prolly.TimestampedValueRecord;
import build.crab.prolly.TreeRecord;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class CookbookApps {
    private CookbookApps() {
    }

    public static void runAll() throws Exception {
        batchBuild();
        localFirstState();
        resolver();
        crdtMerge();
        conversationMemory();
        agentEventLog();
        backgroundCompaction();
        deterministicRagSnapshot();
        documentChunkIndex();
        vectorSidecar();
        provenanceValues();
        materializedView();
        filesystemSnapshot();
        durableSqlite();
    }

    public static void batchBuild() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            List<Entry> entries = new ArrayList<>();
            for (int idx = 64; idx >= 1; idx--) {
                entries.add(new Entry(bytes(String.format("event/%04d", idx)), bytes("payload-" + idx)));
            }
            TreeRecord tree = prolly.buildFromEntries(entries);
            List<Entry> rows = prolly.range(tree, bytes("event/"), Optional.of(bytes("event0")));
            require(rows.size() == 64, "expected 64 rows");
            requireBytes(bytes("event/0001"), rows.get(0).key(), "first event key");
            require(prolly.collectStatsJson(tree).contains("num_nodes"), "stats should include num_nodes");

            System.out.printf("batch_build: imported %d events%n", rows.size());
        }
    }

    public static void localFirstState() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] main = bytes("app/demo/root/main");
            TreeRecord base = prolly.batch(prolly.create(), List.of(
                    Prolly.upsert(bytes("entity/user/001"), bytes("Ada")),
                    Prolly.upsert(bytes("index/user/name/Ada/001"), new byte[0])));
            prolly.publishNamedRoot(main, base);

            TreeRecord device = prolly.batch(base, List.of(
                    Prolly.upsert(bytes("entity/task/900"), bytes("offline draft")),
                    Prolly.upsert(bytes("index/task/status/open/900"), new byte[0])));
            TreeRecord canonical = prolly.put(base, bytes("entity/user/002"), bytes("Grace"));
            prolly.publishNamedRoot(main, canonical);

            TreeRecord current = prolly.loadNamedRoot(main).orElseThrow();
            TreeRecord merged = prolly.merge(base, current, device, "prefer_right");
            var update = prolly.compareAndSwapNamedRoot(main, Optional.of(current), Optional.of(merged));

            require(update.getApplied(), "main root CAS failed");
            requireBytes(bytes("Grace"), prolly.get(merged, bytes("entity/user/002")).orElseThrow(), "canonical user");
            requireBytes(bytes("offline draft"), prolly.get(merged, bytes("entity/task/900")).orElseThrow(), "device task");

            System.out.println("local_first_state: merged offline branch into main");
        }
    }

    public static void resolver() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            TreeRecord base = prolly.put(prolly.create(), bytes("settings/theme"), bytes("light"));
            TreeRecord leftDelete = prolly.delete(base, bytes("settings/theme"));
            TreeRecord rightUpdate = prolly.put(base, bytes("settings/theme"), bytes("dark"));

            TreeRecord updateWins = prolly.merge(base, leftDelete, rightUpdate, "update_wins");
            TreeRecord deleteWins = prolly.merge(base, leftDelete, rightUpdate, "delete_wins");

            requireBytes(bytes("dark"), prolly.get(updateWins, bytes("settings/theme")).orElseThrow(), "update-wins setting");
            require(prolly.get(deleteWins, bytes("settings/theme")).isEmpty(), "delete-wins should remove setting");

            System.out.println("resolver: demonstrated update-wins and delete-wins policies");
        }
    }

    public static void crdtMerge() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] baseValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("base"), 1));
            byte[] leftValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("left"), 2));
            byte[] rightValue = Prolly.timestampedValueToBytes(Prolly.timestampedValue(bytes("right"), 3));

            TreeRecord base = prolly.put(prolly.create(), bytes("counter/global"), baseValue);
            TreeRecord left = prolly.put(base, bytes("counter/global"), leftValue);
            TreeRecord right = prolly.put(base, bytes("counter/global"), rightValue);
            TreeRecord merged = prolly.crdtMerge(base, left, right, Prolly.crdtConfigLww("update_wins"));
            TimestampedValueRecord decoded =
                    Prolly.timestampedValueFromBytes(prolly.get(merged, bytes("counter/global")).orElseThrow());
            List<byte[]> mergedSet = Prolly.multiValueSetMerge(List.of(bytes("candidate-b")), List.of(bytes("candidate-a"), bytes("candidate-b")));

            requireBytes(bytes("right"), decoded.getValue(), "CRDT value");
            require(Prolly.timestampedValueTimestamp(decoded) == 3, "CRDT timestamp");
            requireBytes(bytes("candidate-a"), mergedSet.get(0), "first multi-value item");

            System.out.println("crdt_merge: last-writer-wins and multi-value helpers passed");
        }
    }

    public static void conversationMemory() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] main = bytes("conversation/c42/root/main");
            byte[] attemptName = bytes("conversation/c42/attempt/extractor/a1");
            TreeRecord base = prolly.put(prolly.create(), bytes("conversation/c42/memory/m001"), bytes("user|likes terse summaries|0.91"));
            prolly.publishNamedRoot(main, base);
            TreeRecord attempt = prolly.put(base, bytes("conversation/c42/memory/m002"), bytes("user|uses Java|0.87"));
            prolly.publishNamedRoot(attemptName, attempt);
            TreeRecord canonical = prolly.put(base, bytes("conversation/c42/memory/m003"), bytes("user|prefers local-first apps|0.82"));
            prolly.publishNamedRoot(main, canonical);

            TreeRecord merged = prolly.merge(
                    base,
                    prolly.loadNamedRoot(main).orElseThrow(),
                    prolly.loadNamedRoot(attemptName).orElseThrow(),
                    "prefer_right");
            var update = prolly.compareAndSwapNamedRoot(main, Optional.of(canonical), Optional.of(merged));
            int count = prolly.range(merged, bytes("conversation/c42/memory/"), Optional.of(bytes("conversation/c42/memory0"))).size();

            require(update.getApplied(), "main root CAS failed");
            require(count == 3, "expected three memories");

            System.out.println("conversation_memory: accepted extractor attempt into canonical memory");
        }
    }

    public static void agentEventLog() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] root = bytes("agent-log/run-7/root/events/current");
            TreeRecord tree = prolly.batch(prolly.create(), List.of(
                    upsertText("agent-log/run-7/event/1783036805000/0001", "user|Summarize the plan"),
                    upsertText("agent-log/run-7/event/1783036805000/0002", "tool-call|search-docs"),
                    upsertText("agent-log/run-7/event/1783036806000/0003", "assistant|Plan ready")));
            prolly.publishNamedRoot(root, tree);

            var page = prolly.rangePage(prolly.loadNamedRoot(root).orElseThrow(), null, Optional.empty(), 2);
            require(page.getEntries().size() == 2, "expected first event page");
            require(page.getNextCursor() != null, "expected next cursor");

            System.out.printf("agent_event_log: first page has %d events%n", page.getEntries().size());
        }
    }

    public static void backgroundCompaction() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            List<MutationRecord> mutations = new ArrayList<>();
            for (int idx = 1; idx <= 6; idx++) {
                mutations.add(upsertText(String.format("event/%04d", idx), "raw-event-" + idx));
            }
            TreeRecord events = prolly.batch(prolly.create(), mutations);
            prolly.publishNamedRoot(bytes("compaction/run/r7/root/events/0001"), events);
            TreeRecord compacted = prolly.batch(events, List.of(
                    Prolly.deleteMutation(bytes("event/0001")),
                    Prolly.deleteMutation(bytes("event/0002")),
                    Prolly.deleteMutation(bytes("event/0003")),
                    Prolly.deleteMutation(bytes("event/0004")),
                    upsertText("event/0004-summary", "summary of events 1..4")));
            prolly.publishNamedRoot(bytes("compaction/run/r7/root/events/current"), compacted);

            GcPlan plan = prolly.planStoreGc(List.of(events, compacted));
            List<Entry> remaining = prolly.range(compacted, bytes("event/"), Optional.of(bytes("event0")));
            require(remaining.size() == 3, "expected compacted log records");
            require(plan.reclaimableNodes() >= 0, "invalid GC plan");

            System.out.printf("background_compaction: compacted log to %d records%n", remaining.size());
        }
    }

    public static void deterministicRagSnapshot() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] indexRoot = bytes("rag/corpus/docs/root/index/current");
            TreeRecord indexV1 = prolly.batch(prolly.create(), List.of(
                    upsertText("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
                    upsertText("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key")));
            prolly.publishNamedRoot(indexRoot, indexV1);
            TreeRecord answers = prolly.put(
                    prolly.create(),
                    bytes("rag/answer/q1"),
                    bytes("query:q1|snapshot:" + HexFormat.of().formatHex(indexV1.getRoot()) + "|citation:doc-1/0001"));
            prolly.publishNamedRoot(bytes("rag/corpus/docs/root/answers"), answers);

            TreeRecord indexV2 = prolly.put(indexV1, bytes("rag/corpus/docs/chunk/doc-3/0001"), bytes("vector:v3|New content"));
            prolly.publishNamedRoot(indexRoot, indexV2);

            int replayRows = prolly.range(indexV1, bytes("rag/corpus/docs/chunk/"), Optional.of(bytes("rag/corpus/docs/chunk0"))).size();
            int currentRows = prolly.range(prolly.loadNamedRoot(indexRoot).orElseThrow(), bytes("rag/corpus/docs/chunk/"), Optional.of(bytes("rag/corpus/docs/chunk0"))).size();
            require(replayRows == 2 && currentRows == 3, "RAG snapshot validation failed");

            System.out.println("deterministic_rag_snapshot: replay kept original index root");
        }
    }

    public static void documentChunkIndex() throws Exception {
        try (Prolly prolly = Prolly.memory(); BlobStore blobStore = BlobStore.memory()) {
            byte[] textKey = bytes("doc-index/corpus/text/parser-v1/doc-1/chunk-0001");
            byte[] metadataKey = bytes("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000");
            TreeRecord tree = prolly.putLargeValue(
                    blobStore,
                    prolly.create(),
                    textKey,
                    bytes("CrabDB stores large chunk text outside prolly leaves.".repeat(8)),
                    Prolly.largeValueConfig(32));
            tree = prolly.put(tree, metadataKey, bytes("doc-1|chunk-0001|0|384|vector-0001"));

            require(prolly.range(tree, bytes("doc-index/corpus/parser/"), Optional.of(bytes("doc-index/corpus/parser0"))).size() == 1, "metadata missing");
            require(new String(prolly.getLargeValue(blobStore, tree, textKey).orElseThrow(), StandardCharsets.UTF_8).startsWith("CrabDB stores"), "chunk text missing");

            System.out.println("document_chunk_index: metadata and blob-backed chunk text are linked");
        }
    }

    public static void vectorSidecar() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            Map<String, double[]> sidecar = new LinkedHashMap<>();
            sidecar.put("vec-1", new double[] {0.9, 0.1});
            sidecar.put("vec-2", new double[] {0.8, 0.2});
            sidecar.put("vec-stale", new double[] {1.0, 0.0});
            TreeRecord tree = prolly.batch(prolly.create(), List.of(
                    upsertText("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
                    upsertText("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1")));
            List<String> allowed = prolly.range(tree, bytes("vector-sidecar/corpus/docs/chunk/"), Optional.of(bytes("vector-sidecar/corpus/docs/chunk0")))
                    .stream()
                    .map(entry -> new String(entry.value(), StandardCharsets.UTF_8).split("\\|", 2)[0])
                    .toList();
            List<String> hits = sidecar.keySet().stream().sorted().filter(allowed::contains).toList();
            require(hits.equals(List.of("vec-1", "vec-2")), "unexpected sidecar hits");

            System.out.printf("vector_sidecar: filtered sidecar hits to %d snapshot vectors%n", hits.size());
        }
    }

    public static void provenanceValues() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            byte[] source = bytes("CrabDB language bindings design");
            String sourceCid = HexFormat.of().formatHex(Prolly.cidFromBytes(source));
            String chunkCid = HexFormat.of().formatHex(Prolly.cidFromBytes(Arrays.copyOfRange(source, 0, 16)));
            TreeRecord tree = prolly.batch(prolly.create(), List.of(
                    upsertText("provenance/chunk/file-1/chunk-1", "source=" + sourceCid + "|chunk=" + chunkCid + "|parser=v1"),
                    upsertText("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1")));
            List<Entry> claims = prolly.range(tree, bytes("provenance/claim/file-1/"), Optional.of(bytes("provenance/claim/file-10")));
            require(claims.size() == 1, "expected one claim");
            require(new String(claims.get(0).value(), StandardCharsets.UTF_8).contains("Rust-backed"), "missing claim text");

            System.out.println("provenance_values: claim links back to source and chunk CIDs");
        }
    }

    public static void materializedView() throws Exception {
        try (Prolly prolly = Prolly.memory()) {
            Order o1 = new Order("acme", "o1", "paid", 1200);
            Order o2 = new Order("acme", "o2", "open", 500);
            TreeRecord sourceV1 = prolly.batch(prolly.create(), List.of(
                    Prolly.upsert(orderKey(o1), encodeOrder(o1)),
                    Prolly.upsert(orderKey(o2), encodeOrder(o2))));
            Order paidO2 = new Order("acme", "o2", "paid", 500);
            TreeRecord sourceV2 = prolly.put(sourceV1, orderKey(paidO2), encodeOrder(paidO2));
            TreeRecord viewV2 = buildRevenueView(prolly, sourceV2);

            requireBytes(bytes("1700"), prolly.get(viewV2, viewKey("acme", "paid")).orElseThrow(), "paid revenue");
            require(prolly.get(viewV2, viewKey("acme", "open")).isEmpty(), "open revenue should be absent");

            System.out.printf("materialized_view: folded %d source diff%n", prolly.diff(sourceV1, sourceV2).size());
        }
    }

    public static void filesystemSnapshot() throws Exception {
        try (Prolly prolly = Prolly.memory(); BlobStore blobStore = BlobStore.memory()) {
            TreeRecord tree = prolly.create();
            Map<String, String> files = Map.of(
                    "README.md", "# Demo\n",
                    "src/lib.rs", "pub fn answer() -> u8 { 42 }\n");
            for (Map.Entry<String, String> file : files.entrySet()) {
                tree = prolly.putLargeValue(blobStore, tree, bytes("path/" + file.getKey()), bytes(file.getValue()), Prolly.largeValueConfig(4));
            }
            prolly.publishNamedRoot(bytes("refs/heads/main"), tree);
            TreeRecord loaded = prolly.loadNamedRoot(bytes("refs/heads/main")).orElseThrow();
            requireBytes(bytes("# Demo\n"), prolly.getLargeValue(blobStore, loaded, bytes("path/README.md")).orElseThrow(), "README.md");

            System.out.println("filesystem_snapshot: published branch with blob-backed file contents");
        }
    }

    public static void durableSqlite() throws Exception {
        Path dir = Files.createTempDirectory("prolly-java-");
        try (Prolly prolly = Prolly.sqlite(dir.resolve("app.prolly.sqlite"))) {
            TreeRecord tree = prolly.batch(prolly.create(), List.of(
                    upsertText("user/1", "Ada"),
                    upsertText("user/2", "Grace")));
            prolly.publishNamedRoot(bytes("users/main"), tree);
            TreeRecord loaded = prolly.loadNamedRoot(bytes("users/main")).orElseThrow();
            requireBytes(tree.getRoot(), loaded.getRoot(), "loaded SQLite root");
            requireBytes(bytes("Ada"), prolly.get(loaded, bytes("user/1")).orElseThrow(), "sqlite user");
        } finally {
            deleteTree(dir);
        }
        System.out.println("durable_sqlite: named root survived through SQLite store API");
    }

    private record Order(String tenant, String id, String status, int cents) {
    }

    private static byte[] orderKey(Order order) {
        return bytes("orders/source/tenant/" + order.tenant() + "/order/" + order.id());
    }

    private static byte[] encodeOrder(Order order) {
        return bytes(order.tenant() + "|" + order.id() + "|" + order.status() + "|" + order.cents());
    }

    private static Order decodeOrder(byte[] value) {
        String[] parts = new String(value, StandardCharsets.UTF_8).split("\\|", 4);
        return new Order(parts[0], parts[1], parts[2], Integer.parseInt(parts[3]));
    }

    private static byte[] viewKey(String tenant, String status) {
        return bytes("orders/view/by-status/tenant/" + tenant + "/status/" + status);
    }

    private static TreeRecord buildRevenueView(Prolly prolly, TreeRecord source) throws Exception {
        Map<String, Integer> totals = new LinkedHashMap<>();
        for (Entry entry : prolly.range(source, bytes("orders/source/"), Optional.of(bytes("orders/source0")))) {
            Order order = decodeOrder(entry.value());
            String key = order.tenant() + "|" + order.status();
            totals.put(key, totals.getOrDefault(key, 0) + order.cents());
        }
        List<MutationRecord> mutations = totals.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> {
                    String[] parts = entry.getKey().split("\\|", 2);
                    return Prolly.upsert(viewKey(parts[0], parts[1]), bytes(Integer.toString(entry.getValue())));
                })
                .toList();
        return prolly.batch(prolly.create(), mutations);
    }

    private static MutationRecord upsertText(String key, String value) {
        return Prolly.upsert(bytes(key), bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static void requireBytes(byte[] expected, byte[] actual, String label) {
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException(label + " mismatch");
        }
    }

    private static void deleteTree(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return;
        }
        try (var paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
