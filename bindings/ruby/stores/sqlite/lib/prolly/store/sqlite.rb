# frozen_string_literal: true

require 'prolly'
require 'sqlite3'

module Prolly
  class SqliteRemoteStore < ForeignRemoteStore
    CREATE_SCHEMA = <<~SQL
      CREATE TABLE IF NOT EXISTS prolly_nodes (cid BLOB PRIMARY KEY NOT NULL, node BLOB NOT NULL) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS prolly_hints (namespace BLOB NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL, PRIMARY KEY (namespace, key)) WITHOUT ROWID;
      CREATE TABLE IF NOT EXISTS prolly_roots (name BLOB PRIMARY KEY NOT NULL, manifest BLOB NOT NULL) WITHOUT ROWID;
    SQL

    def initialize(database)
      raise ArgumentError, 'database must be SQLite3::Database' unless database.is_a?(SQLite3::Database)

      @database = database
      @mutex = Mutex.new
      @closed = false
    end

    def initialize_schema = synchronize { @database.execute_batch(CREATE_SCHEMA) }
    def close = @closed = true

    def descriptor
      capabilities = StoreCapabilitiesRecord.new(
        native_batch_reads: true, atomic_batch_writes: true, node_scan: true,
        hints: true, atomic_nodes_and_hint: true, root_scan: true,
        root_compare_and_swap: true, transactions: true, read_parallelism: 16
      )
      limits = StoreLimitsRecord.new(
        max_batch_read_items: nil, max_batch_write_items: nil,
        max_transaction_operations: nil, max_node_bytes: nil
      )
      value = StoreDescriptorRecord.new(
        protocol_major: 1, adapter_name: 'sqlite-v1', provider: 'sqlite',
        schema_version: 1, capabilities: capabilities, limits: limits
      )
      StoreDescriptorResultRecord.new(value: value, error: nil)
    end

    def get_node(cid) = optional_result(query('SELECT node FROM prolly_nodes WHERE cid = ?', cid))
    def put_node(cid, value) = write('INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node', cid, value)
    def delete_node(cid) = write('DELETE FROM prolly_nodes WHERE cid = ?', cid)

    def batch_nodes(operations)
      transaction do
        operations.each do |item|
          if item.value.present
            execute('INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node', item.key, item.value.value)
          else
            execute('DELETE FROM prolly_nodes WHERE cid = ?', item.key)
          end
        end
      end
      unit
    end

    def batch_get_nodes_ordered(cids)
      values = synchronize { cids.map { |cid| optional(query_unlocked('SELECT node FROM prolly_nodes WHERE cid = ?', cid)) } }
      OptionalBytesListResultRecord.new(values: values, error: nil)
    end

    def list_node_cids
      values = synchronize { @database.execute('SELECT cid FROM prolly_nodes ORDER BY cid').map { |row| row[0].b } }
      BytesListResultRecord.new(values: values, error: nil)
    end

    def get_hint(namespace, key) = optional_result(query('SELECT value FROM prolly_hints WHERE namespace = ? AND key = ?', namespace, key))
    def put_hint(namespace, key, value) = write('INSERT INTO prolly_hints VALUES (?, ?, ?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value', namespace, key, value)

    def batch_put_nodes_with_hint(nodes, namespace, key, value)
      transaction do
        nodes.each { |node| execute('INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node', node.key, node.value) }
        execute('INSERT INTO prolly_hints VALUES (?, ?, ?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value', namespace, key, value)
      end
      unit
    end

    def get_root_manifest(name) = optional_result(query('SELECT manifest FROM prolly_roots WHERE name = ?', name))
    def put_root_manifest(name, manifest) = write('INSERT INTO prolly_roots VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest', name, manifest)
    def delete_root_manifest(name) = write('DELETE FROM prolly_roots WHERE name = ?', name)

    def compare_and_swap_root_manifest(name, expected, replacement)
      synchronize do
        applied = false
        current = nil
        @database.transaction(:immediate) do
          current = query_unlocked('SELECT manifest FROM prolly_roots WHERE name = ?', name)
          if matches?(current, expected)
            write_root(name, replacement)
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
        @database.execute('SELECT name, manifest FROM prolly_roots ORDER BY name').map do |row|
          NamedBytesRecord.new(name: row[0].b, value: row[1].b)
        end
      end
      NamedBytesListResultRecord.new(values: values, error: nil)
    end

    def commit_transaction(nodes, conditions, roots)
      conflict = nil
      synchronize do
        @database.transaction(:immediate) do
          conditions.each do |condition|
            current = query_unlocked('SELECT manifest FROM prolly_roots WHERE name = ?', condition.name)
            next if matches?(current, condition.expected)

            conflict = StoreTransactionConflictRecord.new(
              name: condition.name, expected: condition.expected, current: optional(current)
            )
            break
          end
          unless conflict
            nodes.each do |item|
              item.value.present ? execute('INSERT INTO prolly_nodes VALUES (?, ?) ON CONFLICT(cid) DO UPDATE SET node=excluded.node', item.key, item.value.value) : execute('DELETE FROM prolly_nodes WHERE cid = ?', item.key)
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
      raise 'SQLite store is closed' if @closed
      @mutex.synchronize(&block)
    end

    def blob(value) = SQLite3::Blob.new(value.b)
    def execute(sql, *values) = @database.execute(sql, values.map { |value| blob(value) })
    def query(sql, *values) = synchronize { query_unlocked(sql, *values) }
    def query_unlocked(sql, *values) = @database.get_first_value(sql, values.map { |value| blob(value) })&.b

    def write(sql, *values)
      synchronize { execute(sql, *values) }
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def transaction(&block) = synchronize { @database.transaction(:immediate, &block) }
    def optional(value) = OptionalBytesRecord.new(present: !value.nil?, value: value || ''.b)
    def optional_result(value) = OptionalBytesResultRecord.new(value: optional(value), error: nil)
    def unit = UnitResultRecord.new(error: nil)
    def matches?(current, expected) = expected.present ? current == expected.value : current.nil?

    def write_root(name, replacement)
      if replacement.present
        execute('INSERT INTO prolly_roots VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET manifest=excluded.manifest', name, replacement.value)
      else
        execute('DELETE FROM prolly_roots WHERE name = ?', name)
      end
    end

    def store_error(_error)
      StoreErrorRecord.new(code: 'internal', message: 'SQLite provider operation failed', retryable: false, provider_code: nil)
    end
  end
end
