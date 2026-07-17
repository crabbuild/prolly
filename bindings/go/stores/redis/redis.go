package redis

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"

	prolly "build.crab/prolly-go"
	redisclient "github.com/redis/go-redis/v9"
)

var rootCASScript = redisclient.NewScript(`
local current = redis.call('GET', KEYS[1])
if ARGV[1] == '1' then
  if current == false or current ~= ARGV[2] then return {0, current} end
else
  if current ~= false then return {0, current} end
end
if ARGV[3] == '1' then redis.call('SET', KEYS[1], ARGV[4]) else redis.call('DEL', KEYS[1]) end
return {1, false}
`)

var transactionScript = redisclient.NewScript(`
local condition_count = tonumber(ARGV[1])
local node_write_count = tonumber(ARGV[2])
local root_write_count = tonumber(ARGV[3])
local arg_index = 4
for i = 1, condition_count do
  local current = redis.call('GET', KEYS[i])
  local has_expected = ARGV[arg_index]
  local expected = ARGV[arg_index + 1]
  arg_index = arg_index + 2
  if has_expected == '1' then
    if current == false or current ~= expected then return {0, i, current} end
  else
    if current ~= false then return {0, i, current} end
  end
end
local node_offset = condition_count
for i = 1, node_write_count do
  local kind = ARGV[arg_index]
  arg_index = arg_index + 1
  local key = KEYS[node_offset + i]
  if kind == 'upsert' then redis.call('SET', key, ARGV[arg_index]); arg_index = arg_index + 1
  elseif kind == 'delete' then redis.call('DEL', key)
  else error('unknown transaction node op') end
end
local root_offset = condition_count + node_write_count
for i = 1, root_write_count do
  local kind = ARGV[arg_index]
  arg_index = arg_index + 1
  local key = KEYS[root_offset + i]
  if kind == 'put' then redis.call('SET', key, ARGV[arg_index]); arg_index = arg_index + 1
  elseif kind == 'delete' then redis.call('DEL', key)
  else error('unknown transaction root op') end
end
return {1, 0, false}
`)

type Options struct {
	AdapterName     string
	KeyPrefix       []byte
	ReadParallelism uint32
}

type Store struct {
	client  redisclient.UniversalClient
	options Options
}

func New(client redisclient.UniversalClient, options Options) *Store {
	if strings.TrimSpace(options.AdapterName) == "" {
		options.AdapterName = "redis-v1"
	}
	if options.KeyPrefix == nil {
		options.KeyPrefix = []byte("prolly:")
	}
	options.KeyPrefix = clone(options.KeyPrefix)
	if options.ReadParallelism == 0 {
		options.ReadParallelism = 16
	}
	return &Store{client: client, options: options}
}

func (s *Store) Descriptor(ctx context.Context) (prolly.StoreDescriptor, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreDescriptor{}, err
	}
	return prolly.StoreDescriptor{
		ProtocolMajor: 1, AdapterName: s.options.AdapterName, Provider: "redis", SchemaVersion: 1,
		Capabilities: prolly.StoreCapabilities{
			NativeBatchReads: true, AtomicBatchWrites: true, NodeScan: true, Hints: true,
			AtomicNodesAndHint: true, RootScan: true, RootCompareAndSwap: true,
			Transactions: true, ReadParallelism: s.options.ReadParallelism,
		},
	}, nil
}

func (s *Store) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.nodeKey(key))
}
func (s *Store) PutNode(ctx context.Context, key, value []byte) error {
	return redisError("put_node", s.client.Set(ctx, s.nodeKey(key), value, 0).Err())
}
func (s *Store) DeleteNode(ctx context.Context, key []byte) error {
	return redisError("delete_node", s.client.Del(ctx, s.nodeKey(key)).Err())
}

func (s *Store) BatchNodes(ctx context.Context, mutations []prolly.NodeMutation) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.client.TxPipelined(ctx, func(pipe redisclient.Pipeliner) error {
		for _, mutation := range mutations {
			if mutation.Value.Present {
				pipe.Set(ctx, s.nodeKey(mutation.Key), mutation.Value.Value, 0)
			} else {
				pipe.Del(ctx, s.nodeKey(mutation.Key))
			}
		}
		return nil
	})
	return redisError("batch_nodes", err)
}

func (s *Store) BatchGetNodesOrdered(ctx context.Context, keys [][]byte) ([]prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	redisKeys := make([]string, len(keys))
	for index, key := range keys {
		redisKeys[index] = s.nodeKey(key)
	}
	values, err := s.client.MGet(ctx, redisKeys...).Result()
	if err != nil {
		return nil, redisError("batch_get", err)
	}
	result := make([]prolly.OptionalBytes, len(values))
	for index, value := range values {
		if value == nil {
			continue
		}
		switch typed := value.(type) {
		case string:
			result[index] = prolly.PresentBytes([]byte(typed))
		case []byte:
			result[index] = prolly.PresentBytes(typed)
		default:
			return nil, &prolly.StoreError{Code: "invalid_result", Message: fmt.Sprintf("unexpected Redis MGET value %T", value)}
		}
	}
	return result, nil
}

func (s *Store) ListNodeCIDs(ctx context.Context) ([][]byte, error) {
	keys, err := s.scanFamily(ctx, []byte("node:"))
	if err != nil {
		return nil, err
	}
	result := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) == 32 {
			result = append(result, key)
		}
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i], result[j]) < 0 })
	return result, nil
}

func (s *Store) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.hintKey(namespace, key))
}
func (s *Store) PutHint(ctx context.Context, namespace, key, value []byte) error {
	return redisError("put_hint", s.client.Set(ctx, s.hintKey(namespace, key), value, 0).Err())
}
func (s *Store) BatchPutNodesWithHint(ctx context.Context, nodes []prolly.NodeEntry, namespace, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.client.TxPipelined(ctx, func(pipe redisclient.Pipeliner) error {
		for _, node := range nodes {
			pipe.Set(ctx, s.nodeKey(node.Key), node.Value, 0)
		}
		pipe.Set(ctx, s.hintKey(namespace, key), value, 0)
		return nil
	})
	return redisError("batch_nodes_hint", err)
}

func (s *Store) GetRootManifest(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	return s.get(ctx, s.rootKey(name))
}
func (s *Store) PutRootManifest(ctx context.Context, name, manifest []byte) error {
	return redisError("put_root", s.client.Set(ctx, s.rootKey(name), manifest, 0).Err())
}
func (s *Store) DeleteRootManifest(ctx context.Context, name []byte) error {
	return redisError("delete_root", s.client.Del(ctx, s.rootKey(name)).Err())
}

func (s *Store) CompareAndSwapRootManifest(ctx context.Context, name []byte, expected, replacement prolly.OptionalBytes) (prolly.RootCASResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.RootCASResult{}, err
	}
	response, err := rootCASScript.Run(ctx, s.client, []string{s.rootKey(name)}, boolString(expected.Present), expected.Value, boolString(replacement.Present), replacement.Value).Result()
	if err != nil {
		return prolly.RootCASResult{}, redisError("root_cas", err)
	}
	values, ok := response.([]interface{})
	if !ok || len(values) != 2 {
		return prolly.RootCASResult{}, invalidRedisResult("root CAS", response)
	}
	applied, err := redisBool(values[0])
	if err != nil {
		return prolly.RootCASResult{}, err
	}
	if applied {
		return prolly.RootCASResult{Applied: true, Current: replacement.Clone()}, nil
	}
	current, err := redisOptional(values[1])
	return prolly.RootCASResult{Current: current}, err
}

func (s *Store) ListRootManifests(ctx context.Context) ([]prolly.NamedStoreRoot, error) {
	names, err := s.scanFamily(ctx, []byte("root:"))
	if err != nil {
		return nil, err
	}
	sort.Slice(names, func(i, j int) bool { return bytes.Compare(names[i], names[j]) < 0 })
	result := make([]prolly.NamedStoreRoot, 0, len(names))
	for _, name := range names {
		value, err := s.GetRootManifest(ctx, name)
		if err != nil {
			return nil, err
		}
		if value.Present {
			result = append(result, prolly.NamedStoreRoot{Name: name, Manifest: value.Value})
		}
	}
	return result, nil
}

func (s *Store) CommitTransaction(ctx context.Context, nodes []prolly.NodeMutation, conditions []prolly.RootCondition, roots []prolly.RootWrite) (prolly.StoreTransactionResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	keys := make([]string, 0, len(conditions)+len(nodes)+len(roots))
	args := []interface{}{len(conditions), len(nodes), len(roots)}
	for _, condition := range conditions {
		keys = append(keys, s.rootKey(condition.Name))
		args = append(args, boolString(condition.Expected.Present), condition.Expected.Value)
	}
	for _, node := range nodes {
		keys = append(keys, s.nodeKey(node.Key))
		if node.Value.Present {
			args = append(args, "upsert", node.Value.Value)
		} else {
			args = append(args, "delete")
		}
	}
	for _, root := range roots {
		keys = append(keys, s.rootKey(root.Name))
		if root.Replacement.Present {
			args = append(args, "put", root.Replacement.Value)
		} else {
			args = append(args, "delete")
		}
	}
	response, err := transactionScript.Run(ctx, s.client, keys, args...).Result()
	if err != nil {
		return prolly.StoreTransactionResult{}, redisError("transaction", err)
	}
	values, ok := response.([]interface{})
	if !ok || len(values) != 3 {
		return prolly.StoreTransactionResult{}, invalidRedisResult("transaction", response)
	}
	applied, err := redisBool(values[0])
	if err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	if applied {
		return prolly.StoreTransactionResult{Applied: true}, nil
	}
	index, ok := values[1].(int64)
	if !ok || index < 1 || int(index) > len(conditions) {
		return prolly.StoreTransactionResult{}, invalidRedisResult("transaction conflict index", values[1])
	}
	current, err := redisOptional(values[2])
	if err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	condition := conditions[index-1]
	return prolly.StoreTransactionResult{Conflict: &prolly.StoreTransactionConflict{Name: clone(condition.Name), Expected: condition.Expected.Clone(), Current: current}}, nil
}

func (s *Store) Clear(ctx context.Context) error {
	if len(s.options.KeyPrefix) == 0 {
		return &prolly.StoreError{Code: "invalid_argument", Message: "refusing to clear an empty Redis key prefix"}
	}
	var cursor uint64
	pattern := string(append(clone(s.options.KeyPrefix), '*'))
	for {
		keys, next, err := s.client.Scan(ctx, cursor, pattern, 1024).Result()
		if err != nil {
			return redisError("clear_scan", err)
		}
		if len(keys) != 0 {
			if err := s.client.Del(ctx, keys...).Err(); err != nil {
				return redisError("clear_delete", err)
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

func (s *Store) get(ctx context.Context, key string) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	value, err := s.client.Get(ctx, key).Bytes()
	if errors.Is(err, redisclient.Nil) {
		return prolly.MissingBytes(), nil
	}
	if err != nil {
		return prolly.OptionalBytes{}, redisError("get", err)
	}
	return prolly.PresentBytes(value), nil
}
func (s *Store) familyKey(family, suffix []byte) string {
	value := make([]byte, 0, len(s.options.KeyPrefix)+len(family)+len(suffix))
	value = append(value, s.options.KeyPrefix...)
	value = append(value, family...)
	value = append(value, suffix...)
	return string(value)
}
func (s *Store) nodeKey(key []byte) string { return s.familyKey([]byte("node:"), key) }
func (s *Store) rootKey(key []byte) string { return s.familyKey([]byte("root:"), key) }
func (s *Store) hintKey(namespace, key []byte) string {
	suffix := make([]byte, 8, 8+len(namespace)+len(key))
	binary.BigEndian.PutUint64(suffix, uint64(len(namespace)))
	suffix = append(suffix, namespace...)
	suffix = append(suffix, key...)
	return s.familyKey([]byte("hint:"), suffix)
}
func (s *Store) scanFamily(ctx context.Context, family []byte) ([][]byte, error) {
	prefix := []byte(s.familyKey(family, nil))
	pattern := string(append(clone(prefix), '*'))
	var cursor uint64
	var result [][]byte
	for {
		keys, next, err := s.client.Scan(ctx, cursor, pattern, 1024).Result()
		if err != nil {
			return nil, redisError("scan", err)
		}
		for _, key := range keys {
			raw := []byte(key)
			if bytes.HasPrefix(raw, prefix) {
				result = append(result, clone(raw[len(prefix):]))
			}
		}
		if next == 0 {
			return result, nil
		}
		cursor = next
	}
}
func (s *Store) ready(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if s == nil || s.client == nil {
		return &prolly.StoreError{Code: "invalid_store", Message: "Redis client is nil"}
	}
	return nil
}
func boolString(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
func redisBool(value interface{}) (bool, error) {
	switch typed := value.(type) {
	case int64:
		return typed == 1, nil
	case bool:
		return typed, nil
	default:
		return false, invalidRedisResult("boolean", value)
	}
}
func redisOptional(value interface{}) (prolly.OptionalBytes, error) {
	switch typed := value.(type) {
	case nil:
		return prolly.MissingBytes(), nil
	case bool:
		if !typed {
			return prolly.MissingBytes(), nil
		}
	case string:
		return prolly.PresentBytes([]byte(typed)), nil
	case []byte:
		return prolly.PresentBytes(typed), nil
	}
	return prolly.OptionalBytes{}, invalidRedisResult("optional bytes", value)
}
func invalidRedisResult(context string, value interface{}) error {
	return &prolly.StoreError{Code: "invalid_result", Message: fmt.Sprintf("Redis %s returned %T", context, value)}
}
func redisError(operation string, err error) error {
	if err == nil {
		return nil
	}
	retryable := errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "connection")
	return &prolly.StoreError{Code: "redis", Message: fmt.Sprintf("%s: %v", operation, err), Retryable: retryable, Cause: err}
}
func clone(value []byte) []byte { return append([]byte(nil), value...) }

var _ prolly.RemoteStore = (*Store)(nil)
