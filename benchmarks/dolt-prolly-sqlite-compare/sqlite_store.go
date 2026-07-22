package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
	_ "github.com/mattn/go-sqlite3"
)

type storeMetrics struct {
	chunkReads, chunkWrites, bytesRead, bytesWritten uint64
}

type sqliteChunkStore struct {
	db          *sql.DB
	mu          sync.Mutex
	pending     map[hash.Hash]chunks.Chunk
	pendingRefs hash.HashSet
	root        hash.Hash
	closed      bool
	metrics     storeMetrics
}

var _ chunks.ChunkStore = (*sqliteChunkStore)(nil)

func openSQLiteChunkStore(path string) (*sqliteChunkStore, error) {
	dsn := "file:" + path + "?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL&_temp_store=MEMORY&_txlock=immediate"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`
CREATE TABLE IF NOT EXISTS prolly_chunks (
  hash BLOB PRIMARY KEY CHECK(length(hash) = 20),
  data BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name TEXT PRIMARY KEY,
  hash BLOB NOT NULL CHECK(length(hash) = 20)
);`); err != nil {
		db.Close()
		return nil, err
	}
	store := &sqliteChunkStore{db: db, pending: make(map[hash.Hash]chunks.Chunk), pendingRefs: hash.HashSet{}}
	var root []byte
	err = db.QueryRow(`SELECT hash FROM prolly_roots WHERE name = 'chunk_store'`).Scan(&root)
	if err == nil {
		store.root = hash.New(root)
	} else if !errors.Is(err, sql.ErrNoRows) {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *sqliteChunkStore) Get(ctx context.Context, h hash.Hash) (chunks.Chunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return chunks.EmptyChunk, err
	}
	if chunk, ok := s.pending[h]; ok {
		s.metrics.chunkReads++
		s.metrics.bytesRead += uint64(chunk.Size())
		return chunk, nil
	}
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data FROM prolly_chunks WHERE hash = ?`, h[:]).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return chunks.EmptyChunk, nil
	}
	if err != nil {
		return chunks.EmptyChunk, err
	}
	s.metrics.chunkReads++
	s.metrics.bytesRead += uint64(len(data))
	return chunks.NewChunkWithHash(h, data), nil
}

func (s *sqliteChunkStore) GetMany(ctx context.Context, hashes hash.HashSet, found func(context.Context, *chunks.Chunk)) error {
	for h := range hashes {
		chunk, err := s.Get(ctx, h)
		if err != nil {
			return err
		}
		if !chunk.IsEmpty() {
			found(ctx, &chunk)
		}
	}
	return nil
}

func (s *sqliteChunkStore) Has(ctx context.Context, h hash.Hash) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return false, err
	}
	if _, ok := s.pending[h]; ok {
		return true, nil
	}
	var one int
	err := s.db.QueryRowContext(ctx, `SELECT 1 FROM prolly_chunks WHERE hash = ?`, h[:]).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *sqliteChunkStore) HasMany(ctx context.Context, hashes hash.HashSet) (hash.HashSet, error) {
	absent := hash.HashSet{}
	for h := range hashes {
		ok, err := s.Has(ctx, h)
		if err != nil {
			return nil, err
		}
		if !ok {
			absent.Insert(h)
		}
	}
	return absent, nil
}

func (s *sqliteChunkStore) Put(ctx context.Context, chunk chunks.Chunk, getAddrs chunks.InsertAddrsCurry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	refs := hash.HashSet{}
	if err := getAddrs(chunk)(ctx, refs, chunks.NoopPendingRefExists); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("SQLite chunk store is closed")
	}
	s.pending[chunk.Hash()] = chunk
	s.pendingRefs.InsertAll(refs)
	return nil
}

func (s *sqliteChunkStore) Version() string { return constants.FormatDoltString }
func (s *sqliteChunkStore) AccessMode() chunks.ExclusiveAccessMode {
	return chunks.ExclusiveAccessMode_Shared
}

func (s *sqliteChunkStore) Rebase(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	root, err := readRoot(ctx, s.db)
	if err != nil {
		return err
	}
	s.root = root
	return nil
}

func (s *sqliteChunkStore) Root(ctx context.Context) (hash.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return hash.Hash{}, err
	}
	return s.root, nil
}

func (s *sqliteChunkStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	persisted, err := readRoot(ctx, tx)
	if err != nil {
		return false, err
	}
	if persisted != last {
		s.root = persisted
		return false, nil
	}
	if err := s.persistPending(ctx, tx); err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO prolly_roots(name, hash) VALUES('chunk_store', ?) ON CONFLICT(name) DO UPDATE SET hash = excluded.hash`, current[:]); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	s.recordPendingWrites()
	s.clearPending()
	s.root = current
	return true, nil
}

func (s *sqliteChunkStore) flushPending(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.persistPending(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.recordPendingWrites()
	s.clearPending()
	return nil
}

func (s *sqliteChunkStore) persistPending(ctx context.Context, tx *sql.Tx) error {
	for ref := range s.pendingRefs {
		if _, ok := s.pending[ref]; ok {
			continue
		}
		var one int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM prolly_chunks WHERE hash = ?`, ref[:]).Scan(&one); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("dangling reference to %s", ref)
			}
			return err
		}
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO prolly_chunks(hash, data) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for h, chunk := range s.pending {
		if _, err := stmt.ExecContext(ctx, h[:], chunk.Data()); err != nil {
			return err
		}
	}
	return nil
}

func (s *sqliteChunkStore) checkpoint(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

func (s *sqliteChunkStore) resetMetrics() {
	s.mu.Lock()
	s.metrics = storeMetrics{}
	s.mu.Unlock()
}

func (s *sqliteChunkStore) snapshotMetrics() storeMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metrics
}

func (s *sqliteChunkStore) Stats() interface{}   { return s.snapshotMetrics() }
func (s *sqliteChunkStore) StatsSummary() string { return "SQLite benchmark chunk store" }
func (s *sqliteChunkStore) PersistGhostHashes(context.Context, hash.HashSet) error {
	return chunks.ErrUnsupportedOperation
}
func (s *sqliteChunkStore) Teardown(ctx context.Context) error { return ctx.Err() }

func (s *sqliteChunkStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

func (s *sqliteChunkStore) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return errors.New("SQLite chunk store is closed")
	}
	return nil
}

func (s *sqliteChunkStore) recordPendingWrites() {
	s.metrics.chunkWrites += uint64(len(s.pending))
	for _, chunk := range s.pending {
		s.metrics.bytesWritten += uint64(chunk.Size())
	}
}

func (s *sqliteChunkStore) clearPending() {
	s.pending = make(map[hash.Hash]chunks.Chunk)
	s.pendingRefs = hash.HashSet{}
}

type rootQuery interface {
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func readRoot(ctx context.Context, query rootQuery) (hash.Hash, error) {
	var raw []byte
	err := query.QueryRowContext(ctx, `SELECT hash FROM prolly_roots WHERE name = 'chunk_store'`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return hash.Hash{}, nil
	}
	if err != nil {
		return hash.Hash{}, err
	}
	if len(raw) != hash.ByteLen {
		return hash.Hash{}, fmt.Errorf("stored root has %d bytes", len(raw))
	}
	return hash.New(raw), nil
}
