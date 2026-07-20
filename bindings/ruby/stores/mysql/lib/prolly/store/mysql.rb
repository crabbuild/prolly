# frozen_string_literal: true

require 'mysql2'
require 'prolly'

module Prolly
  class MysqlRemoteStore < ForeignRemoteStore
    CREATE_SCHEMA = [
      'CREATE TABLE IF NOT EXISTS prolly_nodes (cid VARBINARY(32) PRIMARY KEY, node LONGBLOB NOT NULL)',
      'CREATE TABLE IF NOT EXISTS prolly_hints (namespace VARBINARY(255) NOT NULL, `key` VARBINARY(255) NOT NULL, value LONGBLOB NOT NULL, PRIMARY KEY(namespace, `key`))',
      'CREATE TABLE IF NOT EXISTS prolly_roots (name VARBINARY(255) PRIMARY KEY, manifest LONGBLOB NOT NULL)'
    ].freeze
    UPSERT_NODE = 'INSERT INTO prolly_nodes VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE node=new.node'
    UPSERT_HINT = 'INSERT INTO prolly_hints VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE value=new.value'
    UPSERT_ROOT = 'INSERT INTO prolly_roots VALUES (?, ?) AS new ON DUPLICATE KEY UPDATE manifest=new.manifest'

    def initialize(client)
      raise ArgumentError, 'client must be Mysql2::Client' unless client.is_a?(Mysql2::Client)

      @client = client
      @mutex = Mutex.new
      @closed = false
    end

    def initialize_schema = synchronize { CREATE_SCHEMA.each { |sql| @client.query(sql) } }
    def close = @closed = true

    def descriptor
      capabilities = StoreCapabilitiesRecord.new(
        native_batch_reads: false, atomic_batch_writes: true, node_scan: true,
        hints: true, atomic_nodes_and_hint: true, root_scan: true,
        root_compare_and_swap: true, transactions: true, read_parallelism: 1
      )
      limits = StoreLimitsRecord.new(
        max_batch_read_items: nil, max_batch_write_items: nil,
        max_transaction_operations: nil, max_node_bytes: nil
      )
      value = StoreDescriptorRecord.new(
        protocol_major: STORE_PROTOCOL_MAJOR, adapter_name: 'mysql-v1', provider: 'mysql',
        schema_version: 1, capabilities: capabilities, limits: limits
      )
      StoreDescriptorResultRecord.new(value: value, error: nil)
    end

    def get_node(cid)
      optional_result(query('SELECT node FROM prolly_nodes WHERE cid=?', key(cid, 32, 'node CID')))
    rescue StandardError => error
      OptionalBytesResultRecord.new(value: optional(nil), error: store_error(error))
    end
    def put_node(cid, value)
      write(UPSERT_NODE, key(cid, 32, 'node CID'), value)
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def delete_node(cid)
      write('DELETE FROM prolly_nodes WHERE cid=?', key(cid, 32, 'node CID'))
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def batch_nodes(operations)
      transaction do
        operations.each do |item|
          cid = key(item.key, 32, 'node CID')
          item.value.present ? execute(UPSERT_NODE, cid, item.value.value) : execute('DELETE FROM prolly_nodes WHERE cid=?', cid)
        end
      end
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def batch_get_nodes_ordered(cids)
      values = synchronize { cids.map { |cid| optional(query_unlocked('SELECT node FROM prolly_nodes WHERE cid=?', key(cid, 32, 'node CID'))) } }
      OptionalBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      OptionalBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def list_node_cids
      values = synchronize { rows('SELECT cid FROM prolly_nodes ORDER BY cid').map { |row| row[0].b } }
      BytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      BytesListResultRecord.new(values: [], error: store_error(error))
    end

    def get_hint(namespace, hint_key)
      optional_result(query('SELECT value FROM prolly_hints WHERE namespace=? AND `key`=?', key(namespace, 255, 'hint namespace'), key(hint_key, 255, 'hint key')))
    rescue StandardError => error
      OptionalBytesResultRecord.new(value: optional(nil), error: store_error(error))
    end

    def put_hint(namespace, hint_key, value)
      write(UPSERT_HINT, key(namespace, 255, 'hint namespace'), key(hint_key, 255, 'hint key'), value)
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def batch_put_nodes_with_hint(nodes, namespace, hint_key, value)
      transaction do
        nodes.each { |node| execute(UPSERT_NODE, key(node.key, 32, 'node CID'), node.value) }
        execute(UPSERT_HINT, key(namespace, 255, 'hint namespace'), key(hint_key, 255, 'hint key'), value)
      end
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def get_root_manifest(name)
      optional_result(query('SELECT manifest FROM prolly_roots WHERE name=?', key(name, 255, 'root name')))
    rescue StandardError => error
      OptionalBytesResultRecord.new(value: optional(nil), error: store_error(error))
    end

    def put_root_manifest(name, manifest)
      write(UPSERT_ROOT, key(name, 255, 'root name'), manifest)
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def delete_root_manifest(name)
      write('DELETE FROM prolly_roots WHERE name=?', key(name, 255, 'root name'))
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def compare_and_swap_root_manifest(name, expected, replacement)
      name = key(name, 255, 'root name')
      synchronize do
        transaction_unlocked([name]) do
          current = query_unlocked('SELECT manifest FROM prolly_roots WHERE name=? FOR UPDATE', name)
          unless matches?(current, expected)
            next RootCasResultRecord.new(applied: false, current: optional(current), error: nil)
          end
          write_root(name, replacement)
          RootCasResultRecord.new(applied: true, current: replacement, error: nil)
        end
      end
    rescue StandardError => error
      RootCasResultRecord.new(applied: false, current: optional(nil), error: store_error(error))
    end

    def list_root_manifests
      values = synchronize do
        rows('SELECT name,manifest FROM prolly_roots ORDER BY name').map { |row| NamedBytesRecord.new(name: row[0].b, value: row[1].b) }
      end
      NamedBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      NamedBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def commit_transaction(nodes, conditions, roots)
      names = conditions.map { |condition| key(condition.name, 255, 'root name') }.uniq.sort
      synchronize do
        transaction_unlocked(names) do
          conflict = nil
          conditions.each do |condition|
            current = query_unlocked('SELECT manifest FROM prolly_roots WHERE name=? FOR UPDATE', condition.name)
            next if matches?(current, condition.expected)

            conflict = StoreTransactionConflictRecord.new(name: condition.name, expected: condition.expected, current: optional(current))
            break
          end
          next TransactionResultRecord.new(applied: false, conflict: conflict, error: nil) if conflict

          nodes.each do |item|
            cid = key(item.key, 32, 'node CID')
            item.value.present ? execute(UPSERT_NODE, cid, item.value.value) : execute('DELETE FROM prolly_nodes WHERE cid=?', cid)
          end
          roots.each { |root| write_root(key(root.name, 255, 'root name'), root.replacement) }
          TransactionResultRecord.new(applied: true, conflict: nil, error: nil)
        end
      end
    rescue StandardError => error
      TransactionResultRecord.new(applied: false, conflict: nil, error: store_error(error))
    end

    private

    def synchronize(&block)
      raise 'MySQL store is closed' if @closed
      @mutex.synchronize(&block)
    end

    def key(value, maximum, label)
      value = value.b
      raise ArgumentError, "#{label} exceeds #{maximum} bytes" if value.bytesize > maximum
      value
    end

    def execute(sql, *values)
      statement = @client.prepare(sql)
      statement.execute(*values.map(&:b), as: :array).to_a
    ensure
      statement&.close
    end

    def rows(sql) = @client.query(sql, as: :array, cast: false).to_a
    def query(sql, *values) = synchronize { query_unlocked(sql, *values) }
    def query_unlocked(sql, *values) = execute(sql, *values).first&.first&.b

    def write(sql, *values)
      synchronize { execute(sql, *values) }
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def transaction(&block) = synchronize { transaction_unlocked([], &block) }

    def transaction_unlocked(lock_names)
      acquired = []
      lock_names.each do |name|
        raise 'MySQL root lock timed out' unless execute("SELECT GET_LOCK(CONCAT('prolly:', HEX(?)), 10)", name).first.first == 1
        acquired << name
      end
      @client.query('BEGIN')
      begin
        value = yield
        @client.query('COMMIT')
        value
      rescue Exception
        @client.query('ROLLBACK') rescue nil
        raise
      ensure
        acquired.reverse_each { |name| execute("SELECT RELEASE_LOCK(CONCAT('prolly:', HEX(?)))", name) }
      end
    end

    def optional(value) = OptionalBytesRecord.new(present: !value.nil?, value: value || ''.b)
    def optional_result(value) = OptionalBytesResultRecord.new(value: optional(value), error: nil)
    def unit = UnitResultRecord.new(error: nil)
    def matches?(current, expected) = expected.present ? current == expected.value : current.nil?
    def write_root(name, replacement) = replacement.present ? execute(UPSERT_ROOT, name, replacement.value) : execute('DELETE FROM prolly_roots WHERE name=?', name)

    def store_error(error)
      StoreErrorRecord.new(
        code: error.is_a?(ArgumentError) ? 'invalid_argument' : 'internal',
        message: 'MySQL provider operation failed', retryable: false,
        provider_code: error.respond_to?(:error_number) ? error.error_number.to_s : nil
      )
    end
  end
end
