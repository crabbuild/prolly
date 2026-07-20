package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	prolly "build.crab/prolly-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	AdapterName     string
	ReadParallelism uint32
}

type Store struct {
	pool    *pgxpool.Pool
	options Options
	owned   bool
	closed  atomic.Bool
}

func New(pool *pgxpool.Pool, options Options) *Store {
	return &Store{pool: pool, options: normalizeOptions(options)}
}

func Open(ctx context.Context, connectionString string, options Options) (*Store, error) {
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, pgError("open", err)
	}
	store := New(pool, options)
	store.owned = true
	return store, nil
}

func normalizeOptions(options Options) Options {
	if strings.TrimSpace(options.AdapterName) == "" {
		options.AdapterName = "postgres-v1"
	}
	if options.ReadParallelism == 0 {
		options.ReadParallelism = 16
	}
	return options
}

func (s *Store) InitializeSchema(ctx context.Context) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	for _, statement := range strings.Split(Schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return pgError("initialize_schema", err)
		}
	}
	return nil
}

func (s *Store) Close() error {
	if s == nil || s.closed.Swap(true) || !s.owned || s.pool == nil {
		return nil
	}
	s.pool.Close()
	return nil
}

func (s *Store) Descriptor(ctx context.Context) (prolly.StoreDescriptor, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreDescriptor{}, err
	}
	return prolly.StoreDescriptor{
		ProtocolMajor: prolly.StoreProtocolMajor, AdapterName: s.options.AdapterName, Provider: "postgresql", SchemaVersion: 1,
		Capabilities: prolly.StoreCapabilities{
			NativeBatchReads: true, AtomicBatchWrites: true, NodeScan: true, Hints: true,
			AtomicNodesAndHint: true, RootScan: true, RootCompareAndSwap: true,
			Transactions: true, ReadParallelism: s.options.ReadParallelism,
		},
	}, nil
}

func (s *Store) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.pool, selectNode, key)
}

func (s *Store) PutNode(ctx context.Context, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, upsertNode, key, value)
	return pgError("put_node", err)
}

func (s *Store) DeleteNode(ctx context.Context, key []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, deleteNode, key)
	return pgError("delete_node", err)
}

func (s *Store) BatchNodes(ctx context.Context, mutations []prolly.NodeMutation) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	return s.withTx(ctx, "batch_nodes", func(tx pgx.Tx) error { return applyNodes(ctx, tx, mutations) })
}

func (s *Store) PublishNodes(ctx context.Context, publication prolly.NodePublication) error {
	return prolly.PublishNodesWithGeneralPath(ctx, s, publication)
}

func (s *Store) BatchGetNodesOrdered(ctx context.Context, keys [][]byte) ([]prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	result := make([]prolly.OptionalBytes, len(keys))
	for index, key := range keys {
		value, err := queryOptional(ctx, s.pool, selectNode, key)
		if err != nil {
			return nil, err
		}
		result[index] = value
	}
	return result, nil
}

func (s *Store) ListNodeCIDs(ctx context.Context) ([][]byte, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT cid FROM prolly_nodes ORDER BY cid`)
	if err != nil {
		return nil, pgError("list_nodes", err)
	}
	defer rows.Close()
	var result [][]byte
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, pgError("list_nodes_scan", err)
		}
		result = append(result, clone(value))
	}
	return result, pgError("list_nodes_rows", rows.Err())
}

func (s *Store) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.pool, selectHint, namespace, key)
}

func (s *Store) PutHint(ctx context.Context, namespace, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, upsertHint, namespace, key, value)
	return pgError("put_hint", err)
}

func (s *Store) BatchPutNodesWithHint(ctx context.Context, nodes []prolly.NodeEntry, namespace, key, value []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	return s.withTx(ctx, "batch_nodes_hint", func(tx pgx.Tx) error {
		for _, node := range nodes {
			if _, err := tx.Exec(ctx, upsertNode, node.Key, node.Value); err != nil {
				return pgError("batch_node", err)
			}
		}
		_, err := tx.Exec(ctx, upsertHint, namespace, key, value)
		return pgError("batch_hint", err)
	})
}

func (s *Store) GetRootManifest(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	return queryOptional(ctx, s.pool, selectRoot, name)
}

func (s *Store) PutRootManifest(ctx context.Context, name, manifest []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, upsertRoot, name, manifest)
	return pgError("put_root", err)
}

func (s *Store) DeleteRootManifest(ctx context.Context, name []byte) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, deleteRoot, name)
	return pgError("delete_root", err)
}

func (s *Store) CompareAndSwapRootManifest(ctx context.Context, name []byte, expected, replacement prolly.OptionalBytes) (prolly.RootCASResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.RootCASResult{}, err
	}
	var result prolly.RootCASResult
	err := s.withTx(ctx, "root_cas", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `LOCK TABLE prolly_roots IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return pgError("lock_roots", err)
		}
		current, err := queryOptional(ctx, tx, selectRoot, name)
		if err != nil {
			return err
		}
		if !optionalEqual(current, expected) {
			result.Current = current
			return nil
		}
		if err := writeRoot(ctx, tx, name, replacement); err != nil {
			return err
		}
		result = prolly.RootCASResult{Applied: true, Current: replacement.Clone()}
		return nil
	})
	return result, err
}

func (s *Store) ListRootManifests(ctx context.Context) ([]prolly.NamedStoreRoot, error) {
	if err := s.ready(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT name, manifest FROM prolly_roots ORDER BY name`)
	if err != nil {
		return nil, pgError("list_roots", err)
	}
	defer rows.Close()
	var result []prolly.NamedStoreRoot
	for rows.Next() {
		var name, manifest []byte
		if err := rows.Scan(&name, &manifest); err != nil {
			return nil, pgError("list_roots_scan", err)
		}
		result = append(result, prolly.NamedStoreRoot{Name: clone(name), Manifest: clone(manifest)})
	}
	return result, pgError("list_roots_rows", rows.Err())
}

func (s *Store) CommitTransaction(ctx context.Context, nodes []prolly.NodeMutation, conditions []prolly.RootCondition, roots []prolly.RootWrite) (prolly.StoreTransactionResult, error) {
	if err := s.ready(ctx); err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	var result prolly.StoreTransactionResult
	err := s.withTx(ctx, "transaction", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `LOCK TABLE prolly_roots IN SHARE ROW EXCLUSIVE MODE`); err != nil {
			return pgError("lock_roots", err)
		}
		for _, condition := range conditions {
			current, err := queryOptional(ctx, tx, selectRoot, condition.Name)
			if err != nil {
				return err
			}
			if !optionalEqual(current, condition.Expected) {
				result.Conflict = &prolly.StoreTransactionConflict{Name: clone(condition.Name), Expected: condition.Expected.Clone(), Current: current}
				return nil
			}
		}
		if err := applyNodes(ctx, tx, nodes); err != nil {
			return err
		}
		for _, root := range roots {
			if err := writeRoot(ctx, tx, root.Name, root.Replacement); err != nil {
				return err
			}
		}
		result.Applied = true
		return nil
	})
	return result, err
}

type queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}
type execer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func queryOptional(ctx context.Context, target queryer, query string, args ...any) (prolly.OptionalBytes, error) {
	var value []byte
	err := target.QueryRow(ctx, query, args...).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return prolly.MissingBytes(), nil
	}
	if err != nil {
		return prolly.OptionalBytes{}, pgError("query", err)
	}
	return prolly.PresentBytes(value), nil
}

func applyNodes(ctx context.Context, tx pgx.Tx, mutations []prolly.NodeMutation) error {
	for _, mutation := range mutations {
		var err error
		if mutation.Value.Present {
			_, err = tx.Exec(ctx, upsertNode, mutation.Key, mutation.Value.Value)
		} else {
			_, err = tx.Exec(ctx, deleteNode, mutation.Key)
		}
		if err != nil {
			return pgError("node_mutation", err)
		}
	}
	return nil
}

func writeRoot(ctx context.Context, target execer, name []byte, value prolly.OptionalBytes) error {
	var err error
	if value.Present {
		_, err = target.Exec(ctx, upsertRoot, name, value.Value)
	} else {
		_, err = target.Exec(ctx, deleteRoot, name)
	}
	return pgError("write_root", err)
}

func (s *Store) withTx(ctx context.Context, operation string, call func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return pgError(operation+"_begin", err)
	}
	defer tx.Rollback(ctx)
	if err := call(tx); err != nil {
		return err
	}
	return pgError(operation+"_commit", tx.Commit(ctx))
}

func optionalEqual(left, right prolly.OptionalBytes) bool {
	return left.Present == right.Present && (!left.Present || bytes.Equal(left.Value, right.Value))
}
func clone(value []byte) []byte { return append([]byte(nil), value...) }

func (s *Store) ready(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if s == nil || s.pool == nil {
		return &prolly.StoreError{Code: "invalid_store", Message: "PostgreSQL pool is nil"}
	}
	if s.closed.Load() {
		return &prolly.StoreError{Code: "closed", Message: "PostgreSQL store is closed"}
	}
	return nil
}

func pgError(operation string, err error) error {
	if err == nil {
		return nil
	}
	retryable := false
	providerCode := ""
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		providerCode = pgErr.Code
		retryable = strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "40001" || pgErr.Code == "40P01" || pgErr.Code == "55P03"
	}
	return &prolly.StoreError{Code: "postgresql", Message: fmt.Sprintf("%s: %v", operation, err), Retryable: retryable, ProviderCode: providerCode, Cause: err}
}

var _ prolly.RemoteStore = (*Store)(nil)
