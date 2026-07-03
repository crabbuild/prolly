package cookbook

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	prolly "build.crab/prolly-go"
)

func bytesOf(value string) []byte {
	return []byte(value)
}

func upsert(key string, value []byte) prolly.Mutation {
	return prolly.Mutation{Kind: "upsert", Key: bytesOf(key), Value: value}
}

func upsertText(key string, value string) prolly.Mutation {
	return upsert(key, bytesOf(value))
}

func deleteMutation(key string) prolly.Mutation {
	return prolly.Mutation{Kind: "delete", Key: bytesOf(key)}
}

func mustBytes(label string, expected []byte, actual []byte, ok bool) error {
	if !ok || !bytes.Equal(expected, actual) {
		return fmt.Errorf("%s: expected %q, got %q", label, expected, actual)
	}
	return nil
}

func BatchBuild() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	entries := make([]prolly.Entry, 0, 64)
	for idx := 64; idx >= 1; idx-- {
		entries = append(entries, prolly.Entry{
			Key:   bytesOf(fmt.Sprintf("event/%04d", idx)),
			Value: bytesOf(fmt.Sprintf("payload-%d", idx)),
		})
	}
	tree, err := engine.BuildFromEntries(entries)
	if err != nil {
		return err
	}
	rows, err := engine.Range(tree, bytesOf("event/"), bytesOf("event0"))
	if err != nil {
		return err
	}
	stats, err := engine.CollectStatsJSON(tree)
	if err != nil {
		return err
	}
	if len(rows) != 64 || !bytes.Equal(rows[0].Key, bytesOf("event/0001")) || !strings.Contains(stats, "num_nodes") {
		return fmt.Errorf("batch build validation failed")
	}
	fmt.Printf("batch_build: imported %d events\n", len(rows))
	return nil
}

func LocalFirstState() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	main := bytesOf("app/demo/root/main")
	base, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("entity/user/001", "Ada"),
		upsert("index/user/name/Ada/001", []byte{}),
	})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(main, base); err != nil {
		return err
	}

	device, err := engine.Batch(base, []prolly.Mutation{
		upsertText("entity/task/900", "offline draft"),
		upsert("index/task/status/open/900", []byte{}),
	})
	if err != nil {
		return err
	}
	canonical, err := engine.Put(base, bytesOf("entity/user/002"), bytesOf("Grace"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(main, canonical); err != nil {
		return err
	}

	current, err := engine.LoadNamedRoot(main)
	if err != nil {
		return err
	}
	merged, err := engine.Merge(base, *current, device, "prefer_right")
	if err != nil {
		return err
	}
	update, err := engine.CompareAndSwapNamedRoot(main, current, &merged)
	if err != nil {
		return err
	}
	if !update.Applied {
		return fmt.Errorf("main root CAS failed")
	}
	if err := requireGet(engine, merged, "entity/user/002", "Grace"); err != nil {
		return err
	}
	if err := requireGet(engine, merged, "entity/task/900", "offline draft"); err != nil {
		return err
	}

	fmt.Println("local_first_state: merged offline branch into main")
	return nil
}

func Resolver() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	base, err := engine.Put(mustCreate(engine), bytesOf("settings/theme"), bytesOf("light"))
	if err != nil {
		return err
	}
	leftDelete, err := engine.Delete(base, bytesOf("settings/theme"))
	if err != nil {
		return err
	}
	rightUpdate, err := engine.Put(base, bytesOf("settings/theme"), bytesOf("dark"))
	if err != nil {
		return err
	}
	updateWins, err := engine.Merge(base, leftDelete, rightUpdate, "update_wins")
	if err != nil {
		return err
	}
	deleteWins, err := engine.Merge(base, leftDelete, rightUpdate, "delete_wins")
	if err != nil {
		return err
	}
	if err := requireGet(engine, updateWins, "settings/theme", "dark"); err != nil {
		return err
	}
	_, ok, err := engine.Get(deleteWins, bytesOf("settings/theme"))
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("delete_wins should remove settings/theme")
	}
	fmt.Println("resolver: demonstrated update-wins and delete-wins policies")
	return nil
}

func CrdtMerge() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	baseValue, err := prolly.TimestampedValueToBytes(prolly.TimestampedValue{Value: bytesOf("base"), Timestamp: 1})
	if err != nil {
		return err
	}
	leftValue, err := prolly.TimestampedValueToBytes(prolly.TimestampedValue{Value: bytesOf("left"), Timestamp: 2})
	if err != nil {
		return err
	}
	rightValue, err := prolly.TimestampedValueToBytes(prolly.TimestampedValue{Value: bytesOf("right"), Timestamp: 3})
	if err != nil {
		return err
	}
	base, err := engine.Put(mustCreate(engine), bytesOf("counter/global"), baseValue)
	if err != nil {
		return err
	}
	left, err := engine.Put(base, bytesOf("counter/global"), leftValue)
	if err != nil {
		return err
	}
	right, err := engine.Put(base, bytesOf("counter/global"), rightValue)
	if err != nil {
		return err
	}
	config, err := prolly.CrdtConfigLWW("update_wins")
	if err != nil {
		return err
	}
	merged, err := engine.CrdtMerge(base, left, right, config)
	if err != nil {
		return err
	}
	value, ok, err := engine.Get(merged, bytesOf("counter/global"))
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("missing CRDT value")
	}
	decoded, err := prolly.TimestampedValueFromBytes(value)
	if err != nil {
		return err
	}
	mergedSet, err := prolly.MultiValueSetMerge([][]byte{bytesOf("candidate-b")}, [][]byte{bytesOf("candidate-a"), bytesOf("candidate-b")})
	if err != nil {
		return err
	}
	if !bytes.Equal(decoded.Value, bytesOf("right")) || decoded.Timestamp != 3 || len(mergedSet) != 2 || !bytes.Equal(mergedSet[0], bytesOf("candidate-a")) {
		return fmt.Errorf("CRDT validation failed")
	}
	fmt.Println("crdt_merge: last-writer-wins and multi-value helpers passed")
	return nil
}

func ConversationMemory() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	main := bytesOf("conversation/c42/root/main")
	attemptName := bytesOf("conversation/c42/attempt/extractor/a1")
	base, err := engine.Put(mustCreate(engine), bytesOf("conversation/c42/memory/m001"), bytesOf("user|likes terse summaries|0.91"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(main, base); err != nil {
		return err
	}
	attempt, err := engine.Put(base, bytesOf("conversation/c42/memory/m002"), bytesOf("user|uses Go|0.87"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(attemptName, attempt); err != nil {
		return err
	}
	canonical, err := engine.Put(base, bytesOf("conversation/c42/memory/m003"), bytesOf("user|prefers local-first apps|0.82"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(main, canonical); err != nil {
		return err
	}
	current, err := engine.LoadNamedRoot(main)
	if err != nil {
		return err
	}
	attemptRoot, err := engine.LoadNamedRoot(attemptName)
	if err != nil {
		return err
	}
	merged, err := engine.Merge(base, *current, *attemptRoot, "prefer_right")
	if err != nil {
		return err
	}
	update, err := engine.CompareAndSwapNamedRoot(main, &canonical, &merged)
	if err != nil {
		return err
	}
	rows, err := engine.Range(merged, bytesOf("conversation/c42/memory/"), bytesOf("conversation/c42/memory0"))
	if err != nil {
		return err
	}
	if !update.Applied || len(rows) != 3 {
		return fmt.Errorf("conversation memory merge failed")
	}
	fmt.Println("conversation_memory: accepted extractor attempt into canonical memory")
	return nil
}

func AgentEventLog() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	root := bytesOf("agent-log/run-7/root/events/current")
	tree, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("agent-log/run-7/event/1783036805000/0001", "user|Summarize the plan"),
		upsertText("agent-log/run-7/event/1783036805000/0002", "tool-call|search-docs"),
		upsertText("agent-log/run-7/event/1783036806000/0003", "assistant|Plan ready"),
	})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(root, tree); err != nil {
		return err
	}
	loaded, err := engine.LoadNamedRoot(root)
	if err != nil {
		return err
	}
	page, err := engine.RangePage(*loaded, nil, nil, 2)
	if err != nil {
		return err
	}
	if len(page.Entries) != 2 || page.NextCursor == nil {
		return fmt.Errorf("expected first event page and cursor")
	}
	fmt.Printf("agent_event_log: first page has %d events\n", len(page.Entries))
	return nil
}

func BackgroundCompaction() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	mutations := make([]prolly.Mutation, 0, 6)
	for idx := 1; idx <= 6; idx++ {
		mutations = append(mutations, upsertText(fmt.Sprintf("event/%04d", idx), fmt.Sprintf("raw-event-%d", idx)))
	}
	events, err := engine.Batch(mustCreate(engine), mutations)
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(bytesOf("compaction/run/r7/root/events/0001"), events); err != nil {
		return err
	}
	compacted, err := engine.Batch(events, []prolly.Mutation{
		deleteMutation("event/0001"),
		deleteMutation("event/0002"),
		deleteMutation("event/0003"),
		deleteMutation("event/0004"),
		upsertText("event/0004-summary", "summary of events 1..4"),
	})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(bytesOf("compaction/run/r7/root/events/current"), compacted); err != nil {
		return err
	}
	plan, err := engine.PlanStoreGC([]prolly.Tree{events, compacted})
	if err != nil {
		return err
	}
	remaining, err := engine.Range(compacted, bytesOf("event/"), bytesOf("event0"))
	if err != nil {
		return err
	}
	if len(remaining) != 3 || plan.ReclaimableNodes < 0 {
		return fmt.Errorf("compaction validation failed")
	}
	fmt.Printf("background_compaction: compacted log to %d records\n", len(remaining))
	return nil
}

func DeterministicRagSnapshot() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()

	indexRoot := bytesOf("rag/corpus/docs/root/index/current")
	indexV1, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("rag/corpus/docs/chunk/doc-1/0001", "vector:v1|CrabDB stores deterministic roots"),
		upsertText("rag/corpus/docs/chunk/doc-2/0001", "vector:v2|Prolly trees diff by key"),
	})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(indexRoot, indexV1); err != nil {
		return err
	}
	indexRootBytes, _, err := indexV1.Root()
	if err != nil {
		return err
	}
	answerValue := []byte("query:q1|snapshot:" + hex.EncodeToString(indexRootBytes) + "|citation:doc-1/0001")
	answers, err := engine.Put(mustCreate(engine), bytesOf("rag/answer/q1"), answerValue)
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(bytesOf("rag/corpus/docs/root/answers"), answers); err != nil {
		return err
	}
	indexV2, err := engine.Put(indexV1, bytesOf("rag/corpus/docs/chunk/doc-3/0001"), bytesOf("vector:v3|New content"))
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(indexRoot, indexV2); err != nil {
		return err
	}
	replay, err := engine.Range(indexV1, bytesOf("rag/corpus/docs/chunk/"), bytesOf("rag/corpus/docs/chunk0"))
	if err != nil {
		return err
	}
	currentRoot, err := engine.LoadNamedRoot(indexRoot)
	if err != nil {
		return err
	}
	current, err := engine.Range(*currentRoot, bytesOf("rag/corpus/docs/chunk/"), bytesOf("rag/corpus/docs/chunk0"))
	if err != nil {
		return err
	}
	if len(replay) != 2 || len(current) != 3 {
		return fmt.Errorf("RAG snapshot validation failed")
	}
	fmt.Println("deterministic_rag_snapshot: replay kept original index root")
	return nil
}

func DocumentChunkIndex() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	blobStore, err := prolly.MemoryBlobStore()
	if err != nil {
		return err
	}
	defer blobStore.Close()

	textKey := bytesOf("doc-index/corpus/text/parser-v1/doc-1/chunk-0001")
	metadataKey := bytesOf("doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000")
	tree, err := engine.PutLargeValue(blobStore, mustCreate(engine), textKey, bytes.Repeat(bytesOf("CrabDB stores large chunk text outside prolly leaves."), 8), prolly.LargeValueConfig{InlineThreshold: 32})
	if err != nil {
		return err
	}
	tree, err = engine.Put(tree, metadataKey, bytesOf("doc-1|chunk-0001|0|384|vector-0001"))
	if err != nil {
		return err
	}
	metadata, err := engine.Range(tree, bytesOf("doc-index/corpus/parser/"), bytesOf("doc-index/corpus/parser0"))
	if err != nil {
		return err
	}
	loaded, ok, err := engine.GetLargeValue(blobStore, tree, textKey)
	if err != nil {
		return err
	}
	if len(metadata) != 1 || !ok || !bytes.HasPrefix(loaded, bytesOf("CrabDB stores")) {
		return fmt.Errorf("document chunk validation failed")
	}
	fmt.Println("document_chunk_index: metadata and blob-backed chunk text are linked")
	return nil
}

func VectorSidecar() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	sidecar := map[string][]float64{"vec-1": {0.9, 0.1}, "vec-2": {0.8, 0.2}, "vec-stale": {1.0, 0.0}}
	tree, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("vector-sidecar/corpus/docs/chunk/doc-1/0001", "vec-1|doc-1|parser-v1"),
		upsertText("vector-sidecar/corpus/docs/chunk/doc-2/0001", "vec-2|doc-2|parser-v1"),
	})
	if err != nil {
		return err
	}
	allowed := map[string]bool{}
	rows, err := engine.Range(tree, bytesOf("vector-sidecar/corpus/docs/chunk/"), bytesOf("vector-sidecar/corpus/docs/chunk0"))
	if err != nil {
		return err
	}
	for _, row := range rows {
		allowed[strings.SplitN(string(row.Value), "|", 2)[0]] = true
	}
	var hits []string
	for vectorID := range sidecar {
		if allowed[vectorID] {
			hits = append(hits, vectorID)
		}
	}
	sort.Strings(hits)
	if strings.Join(hits, ",") != "vec-1,vec-2" {
		return fmt.Errorf("unexpected sidecar hits %v", hits)
	}
	fmt.Printf("vector_sidecar: filtered sidecar hits to %d snapshot vectors\n", len(hits))
	return nil
}

func ProvenanceValues() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	source := bytesOf("CrabDB language bindings design")
	sourceCid, err := prolly.CidFromBytes(source)
	if err != nil {
		return err
	}
	chunkCid, err := prolly.CidFromBytes(source[:16])
	if err != nil {
		return err
	}
	tree, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		upsertText("provenance/chunk/file-1/chunk-1", "source="+hex.EncodeToString(sourceCid)+"|chunk="+hex.EncodeToString(chunkCid)+"|parser=v1"),
		upsertText("provenance/claim/file-1/claim-1", "CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1"),
	})
	if err != nil {
		return err
	}
	claims, err := engine.Range(tree, bytesOf("provenance/claim/file-1/"), bytesOf("provenance/claim/file-10"))
	if err != nil {
		return err
	}
	if len(claims) != 1 || !bytes.Contains(claims[0].Value, bytesOf("Rust-backed")) {
		return fmt.Errorf("provenance validation failed")
	}
	fmt.Println("provenance_values: claim links back to source and chunk CIDs")
	return nil
}

type order struct {
	tenant string
	id     string
	status string
	cents  int
}

func orderKey(order order) []byte {
	return bytesOf("orders/source/tenant/" + order.tenant + "/order/" + order.id)
}

func encodeOrder(order order) []byte {
	return bytesOf(fmt.Sprintf("%s|%s|%s|%d", order.tenant, order.id, order.status, order.cents))
}

func decodeOrder(value []byte) (order, error) {
	parts := strings.Split(string(value), "|")
	if len(parts) != 4 {
		return order{}, fmt.Errorf("invalid order value %q", value)
	}
	cents, err := strconv.Atoi(parts[3])
	if err != nil {
		return order{}, err
	}
	return order{tenant: parts[0], id: parts[1], status: parts[2], cents: cents}, nil
}

func viewKey(tenant string, status string) []byte {
	return bytesOf("orders/view/by-status/tenant/" + tenant + "/status/" + status)
}

func buildRevenueView(engine *prolly.Engine, source prolly.Tree) (prolly.Tree, error) {
	rows, err := engine.Range(source, bytesOf("orders/source/"), bytesOf("orders/source0"))
	if err != nil {
		return prolly.Tree{}, err
	}
	totals := map[string]int{}
	for _, row := range rows {
		order, err := decodeOrder(row.Value)
		if err != nil {
			return prolly.Tree{}, err
		}
		totals[order.tenant+"|"+order.status] += order.cents
	}
	keys := make([]string, 0, len(totals))
	for key := range totals {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	mutations := make([]prolly.Mutation, 0, len(keys))
	for _, key := range keys {
		parts := strings.Split(key, "|")
		mutations = append(mutations, upsert(viewKeyString(parts[0], parts[1]), []byte(strconv.Itoa(totals[key]))))
	}
	return engine.Batch(mustCreate(engine), mutations)
}

func viewKeyString(tenant string, status string) string {
	return "orders/view/by-status/tenant/" + tenant + "/status/" + status
}

func MaterializedView() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	o1 := order{"acme", "o1", "paid", 1200}
	o2 := order{"acme", "o2", "open", 500}
	sourceV1, err := engine.Batch(mustCreate(engine), []prolly.Mutation{
		{Kind: "upsert", Key: orderKey(o1), Value: encodeOrder(o1)},
		{Kind: "upsert", Key: orderKey(o2), Value: encodeOrder(o2)},
	})
	if err != nil {
		return err
	}
	paidO2 := order{"acme", "o2", "paid", 500}
	sourceV2, err := engine.Put(sourceV1, orderKey(paidO2), encodeOrder(paidO2))
	if err != nil {
		return err
	}
	viewV2, err := buildRevenueView(engine, sourceV2)
	if err != nil {
		return err
	}
	if err := requireGet(engine, viewV2, viewKeyString("acme", "paid"), "1700"); err != nil {
		return err
	}
	_, ok, err := engine.Get(viewV2, viewKey("acme", "open"))
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("open aggregate should be absent")
	}
	diff, err := engine.Diff(sourceV1, sourceV2)
	if err != nil {
		return err
	}
	fmt.Printf("materialized_view: folded %d source diff\n", len(diff))
	return nil
}

func FilesystemSnapshot() error {
	engine, err := prolly.OpenMemory()
	if err != nil {
		return err
	}
	defer engine.Close()
	blobStore, err := prolly.MemoryBlobStore()
	if err != nil {
		return err
	}
	defer blobStore.Close()
	tree := mustCreate(engine)
	for path, contents := range map[string][]byte{
		"README.md":  bytesOf("# Demo\n"),
		"src/lib.rs": bytesOf("pub fn answer() -> u8 { 42 }\n"),
	} {
		tree, err = engine.PutLargeValue(blobStore, tree, bytesOf("path/"+path), contents, prolly.LargeValueConfig{InlineThreshold: 4})
		if err != nil {
			return err
		}
	}
	if err := engine.PublishNamedRoot(bytesOf("refs/heads/main"), tree); err != nil {
		return err
	}
	loaded, err := engine.LoadNamedRoot(bytesOf("refs/heads/main"))
	if err != nil {
		return err
	}
	readme, ok, err := engine.GetLargeValue(blobStore, *loaded, bytesOf("path/README.md"))
	if err != nil {
		return err
	}
	if err := mustBytes("README.md", bytesOf("# Demo\n"), readme, ok); err != nil {
		return err
	}
	fmt.Println("filesystem_snapshot: published branch with blob-backed file contents")
	return nil
}

func DurableSQLite() error {
	dir, err := os.MkdirTemp("", "prolly-go-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	engine, err := prolly.OpenSQLite(filepath.Join(dir, "app.prolly.sqlite"))
	if err != nil {
		return err
	}
	defer engine.Close()
	tree, err := engine.Batch(mustCreate(engine), []prolly.Mutation{upsertText("user/1", "Ada"), upsertText("user/2", "Grace")})
	if err != nil {
		return err
	}
	if err := engine.PublishNamedRoot(bytesOf("users/main"), tree); err != nil {
		return err
	}
	loaded, err := engine.LoadNamedRoot(bytesOf("users/main"))
	if err != nil {
		return err
	}
	loadedRoot, _, err := loaded.Root()
	if err != nil {
		return err
	}
	treeRoot, _, err := tree.Root()
	if err != nil {
		return err
	}
	if !bytes.Equal(loadedRoot, treeRoot) {
		return fmt.Errorf("loaded SQLite root mismatch")
	}
	if err := requireGet(engine, *loaded, "user/1", "Ada"); err != nil {
		return err
	}
	fmt.Println("durable_sqlite: named root survived through SQLite store API")
	return nil
}

func RunAll() error {
	scenarios := []func() error{
		BatchBuild,
		LocalFirstState,
		Resolver,
		CrdtMerge,
		ConversationMemory,
		AgentEventLog,
		BackgroundCompaction,
		DeterministicRagSnapshot,
		DocumentChunkIndex,
		VectorSidecar,
		ProvenanceValues,
		MaterializedView,
		FilesystemSnapshot,
		DurableSQLite,
	}
	for _, scenario := range scenarios {
		if err := scenario(); err != nil {
			return err
		}
	}
	return nil
}

func mustCreate(engine *prolly.Engine) prolly.Tree {
	tree, err := engine.Create()
	if err != nil {
		panic(err)
	}
	return tree
}

func requireGet(engine *prolly.Engine, tree prolly.Tree, key string, expected string) error {
	value, ok, err := engine.Get(tree, bytesOf(key))
	if err != nil {
		return err
	}
	return mustBytes(key, bytesOf(expected), value, ok)
}
