# frozen_string_literal: true

require 'pg'
require 'prolly'

module Prolly
  class PostgresRemoteStore < ForeignRemoteStore
    CREATE_SCHEMA = <<~SQL
      CREATE TABLE IF NOT EXISTS prolly_nodes (cid bytea PRIMARY KEY, node bytea NOT NULL);
      CREATE TABLE IF NOT EXISTS prolly_hints (namespace bytea NOT NULL, key bytea NOT NULL, value bytea NOT NULL, PRIMARY KEY (namespace, key));
      CREATE TABLE IF NOT EXISTS prolly_roots (name bytea PRIMARY KEY, manifest bytea NOT NULL);
    SQL
    UPSERT_NODE = 'INSERT INTO prolly_nodes VALUES ($1::bytea, $2::bytea) ON CONFLICT(cid) DO UPDATE SET node=excluded.node'
    UPSERT_HINT = 'INSERT INTO prolly_hints VALUES ($1::bytea, $2::bytea, $3::bytea) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value'
    UPSERT_ROOT = 'INSERT INTO prolly_roots VALUES ($1::bytea, $2::bytea) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest'
    LOCK_ROOT = "SELECT pg_advisory_xact_lock(hashtextextended(encode($1::bytea, 'hex'), 0))"

    def initialize(connection)
      raise ArgumentError, 'connection must be PG::Connection' unless connection.is_a?(PG::Connection)

      @connection = connection
      @mutex = Mutex.new
      @closed = false
    end

    def initialize_schema = synchronize { @connection.exec(CREATE_SCHEMA) }
    def close = @closed = true

    def descriptor
      capabilities = StoreCapabilitiesRecord.new(
        native_batch_reads: false, atomic_batch_writes: true, node_scan: true,
        hints: true, atomic_nodes_and_hint: true, root_scan: true,
        root_compare_and_swap: true, transactions: true, read_parallelism: 16
      )
      limits = StoreLimitsRecord.new(
        max_batch_read_items: nil, max_batch_write_items: nil,
        max_transaction_operations: nil, max_node_bytes: nil
      )
      value = StoreDescriptorRecord.new(
        protocol_major: 1, adapter_name: 'postgres-v1', provider: 'postgresql',
        schema_version: 1, capabilities: capabilities, limits: limits
      )
      StoreDescriptorResultRecord.new(value: value, error: nil)
    end

    def get_node(cid) = optional_result(query('SELECT node FROM prolly_nodes WHERE cid = $1::bytea', cid))
    def put_node(cid, value) = write(UPSERT_NODE, cid, value)
    def delete_node(cid) = write('DELETE FROM prolly_nodes WHERE cid = $1::bytea', cid)

    def batch_nodes(operations)
      transaction do
        operations.each do |item|
          item.value.present ? execute(UPSERT_NODE, item.key, item.value.value) : execute('DELETE FROM prolly_nodes WHERE cid = $1::bytea', item.key)
        end
      end
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def batch_get_nodes_ordered(cids)
      values = synchronize { cids.map { |cid| optional(query_unlocked('SELECT node FROM prolly_nodes WHERE cid = $1::bytea', cid)) } }
      OptionalBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      OptionalBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def list_node_cids
      values = synchronize { @connection.exec('SELECT cid FROM prolly_nodes ORDER BY cid').map { |row| decode(row['cid']) } }
      BytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      BytesListResultRecord.new(values: [], error: store_error(error))
    end

    def get_hint(namespace, key) = optional_result(query('SELECT value FROM prolly_hints WHERE namespace = $1::bytea AND key = $2::bytea', namespace, key))
    def put_hint(namespace, key, value) = write(UPSERT_HINT, namespace, key, value)

    def batch_put_nodes_with_hint(nodes, namespace, key, value)
      transaction do
        nodes.each { |node| execute(UPSERT_NODE, node.key, node.value) }
        execute(UPSERT_HINT, namespace, key, value)
      end
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def get_root_manifest(name) = optional_result(query('SELECT manifest FROM prolly_roots WHERE name = $1::bytea', name))
    def put_root_manifest(name, manifest) = write(UPSERT_ROOT, name, manifest)
    def delete_root_manifest(name) = write('DELETE FROM prolly_roots WHERE name = $1::bytea', name)

    def compare_and_swap_root_manifest(name, expected, replacement)
      synchronize do
        applied = false
        current = nil
        transaction_unlocked do
          execute(LOCK_ROOT, name)
          current = query_unlocked('SELECT manifest FROM prolly_roots WHERE name = $1::bytea FOR UPDATE', name)
          if matches?(current, expected)
            write_root(name, replacement)
            current = replacement.present ? replacement.value : nil
            applied = true
          end
        end
        RootCasResultRecord.new(applied: applied, current: optional(current), error: nil)
      end
    rescue StandardError => error
      RootCasResultRecord.new(applied: false, current: optional(nil), error: store_error(error))
    end

    def list_root_manifests
      values = synchronize do
        @connection.exec('SELECT name, manifest FROM prolly_roots ORDER BY name').map do |row|
          NamedBytesRecord.new(name: decode(row['name']), value: decode(row['manifest']))
        end
      end
      NamedBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      NamedBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def commit_transaction(nodes, conditions, roots)
      conflict = nil
      synchronize do
        transaction_unlocked do
          conditions.map(&:name).uniq.sort.each { |name| execute(LOCK_ROOT, name) }
          conditions.each do |condition|
            current = query_unlocked('SELECT manifest FROM prolly_roots WHERE name = $1::bytea FOR UPDATE', condition.name)
            next if matches?(current, condition.expected)

            conflict = StoreTransactionConflictRecord.new(
              name: condition.name, expected: condition.expected, current: optional(current)
            )
            break
          end
          unless conflict
            nodes.each do |item|
              item.value.present ? execute(UPSERT_NODE, item.key, item.value.value) : execute('DELETE FROM prolly_nodes WHERE cid = $1::bytea', item.key)
            end
            roots.each { |root| write_root(root.name, root.replacement) }
          end
        end
      end
      TransactionResultRecord.new(applied: conflict.nil?, conflict: conflict, error: nil)
    rescue StandardError => error
      TransactionResultRecord.new(applied: false, conflict: nil, error: store_error(error))
    end

    private

    def synchronize(&block)
      raise 'PostgreSQL store is closed' if @closed
      @mutex.synchronize(&block)
    end

    def encode(value) = @connection.escape_bytea(value.b)
    def decode(value) = PG::Connection.unescape_bytea(value).b
    def execute(sql, *values) = @connection.exec_params(sql, values.map { |value| encode(value) })
    def query(sql, *values) = synchronize { query_unlocked(sql, *values) }

    def query_unlocked(sql, *values)
      result = execute(sql, *values)
      result.ntuples.zero? ? nil : decode(result.getvalue(0, 0))
    end

    def write(sql, *values)
      synchronize { execute(sql, *values) }
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def transaction(&block) = synchronize { transaction_unlocked(&block) }

    def transaction_unlocked
      @connection.exec('BEGIN')
      begin
        value = yield
        @connection.exec('COMMIT')
        value
      rescue Exception
        @connection.exec('ROLLBACK') rescue nil
        raise
      end
    end

    def optional(value) = OptionalBytesRecord.new(present: !value.nil?, value: value || ''.b)
    def optional_result(value) = OptionalBytesResultRecord.new(value: optional(value), error: nil)
    def unit = UnitResultRecord.new(error: nil)
    def matches?(current, expected) = expected.present ? current == expected.value : current.nil?

    def write_root(name, replacement)
      replacement.present ? execute(UPSERT_ROOT, name, replacement.value) : execute('DELETE FROM prolly_roots WHERE name = $1::bytea', name)
    end

    def store_error(error)
      StoreErrorRecord.new(
        code: 'internal', message: 'PostgreSQL provider operation failed', retryable: false,
        provider_code: error.respond_to?(:result) ? error.result&.error_field(PG::Result::PG_DIAG_SQLSTATE) : nil
      )
    end
  end
end
