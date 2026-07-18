# frozen_string_literal: true

require 'redis'
require 'prolly'

module Prolly
  class RedisRemoteStore < ForeignRemoteStore
    CAS_SCRIPT = <<~LUA.freeze
      local current = redis.call('GET', KEYS[1])
      local expected_present = ARGV[1] == '1'
      if expected_present then
        if current == false or current ~= ARGV[2] then
          return {0, current == false and 0 or 1, current or ''}
        end
      elseif current ~= false then
        return {0, 1, current}
      end
      if ARGV[3] == '1' then
        redis.call('SET', KEYS[1], ARGV[4])
        return {1, 1, ARGV[4]}
      end
      redis.call('DEL', KEYS[1])
      return {1, 0, ''}
    LUA

    MUTATE_SCRIPT = <<~LUA.freeze
      for index = 1, #KEYS do
        local offset = (index - 1) * 2
        if ARGV[offset + 1] == '1' then
          redis.call('SET', KEYS[index], ARGV[offset + 2])
        else
          redis.call('DEL', KEYS[index])
        end
      end
      return 1
    LUA

    TRANSACTION_SCRIPT = <<~LUA.freeze
      local condition_count = tonumber(ARGV[1])
      local node_count = tonumber(ARGV[2])
      local root_count = tonumber(ARGV[3])
      local argument = 4
      for index = 1, condition_count do
        local current = redis.call('GET', KEYS[index])
        local expected_present = ARGV[argument] == '1'
        local matches = (expected_present and current ~= false and current == ARGV[argument + 1])
          or (not expected_present and current == false)
        if not matches then
          return {0, index, current == false and 0 or 1, current or ''}
        end
        argument = argument + 2
      end
      local key_index = condition_count + 1
      for _ = 1, node_count do
        if ARGV[argument] == '1' then redis.call('SET', KEYS[key_index], ARGV[argument + 1])
        else redis.call('DEL', KEYS[key_index]) end
        argument = argument + 2
        key_index = key_index + 1
      end
      for _ = 1, root_count do
        if ARGV[argument] == '1' then redis.call('SET', KEYS[key_index], ARGV[argument + 1])
        else redis.call('DEL', KEYS[key_index]) end
        argument = argument + 2
        key_index = key_index + 1
      end
      return {1}
    LUA

    def initialize(client, key_prefix: 'prolly:'.b)
      raise ArgumentError, 'client must be Redis' unless client.is_a?(Redis)

      @client = client
      @prefix = key_prefix.b
      @mutex = Mutex.new
      @closed = false
    end

    def close = @closed = true

    def clear_namespace
      synchronize do
        raise ArgumentError, 'refusing to clear an empty Redis key prefix' if @prefix.empty?

        keys = scan_family(@prefix)
        keys.each_slice(256) { |slice| @client.del(*slice) }
      end
    end

    def descriptor
      value = StoreDescriptorRecord.new(
        protocol_major: 1, adapter_name: 'redis-v1', provider: 'redis', schema_version: 1,
        capabilities: StoreCapabilitiesRecord.new(
          native_batch_reads: true, atomic_batch_writes: true, node_scan: true,
          hints: true, atomic_nodes_and_hint: true, root_scan: true,
          root_compare_and_swap: true, transactions: true, read_parallelism: 1
        ),
        limits: StoreLimitsRecord.new(
          max_batch_read_items: nil, max_batch_write_items: nil,
          max_transaction_operations: nil, max_node_bytes: nil
        )
      )
      StoreDescriptorResultRecord.new(value: value, error: nil)
    end

    def get_node(cid) = optional_call { @client.get(family('node:', cid)) }
    def put_node(cid, value) = unit_call { @client.set(family('node:', cid), value.b) }
    def delete_node(cid) = unit_call { @client.del(family('node:', cid)) }

    def batch_nodes(operations)
      keys = operations.map { |item| family('node:', item.key) }
      arguments = operations.flat_map { |item| item.value.present ? ['1'.b, item.value.value.b] : ['0'.b, ''.b] }
      mutate(keys, arguments)
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def batch_get_nodes_ordered(cids)
      values = synchronize do
        keys = cids.map { |cid| family('node:', cid) }
        keys.empty? ? [] : @client.mget(*keys).map { |value| optional(value) }
      end
      OptionalBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      OptionalBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def list_node_cids
      family_prefix = @prefix + 'node:'.b
      values = synchronize do
        scan_family(family_prefix).filter_map do |key|
          cid = key.byteslice(family_prefix.bytesize..)
          cid if cid.bytesize == 32
        end.sort
      end
      BytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      BytesListResultRecord.new(values: [], error: store_error(error))
    end

    def get_hint(namespace, hint_key) = optional_call { @client.get(hint_key(namespace, hint_key)) }
    def put_hint(namespace, hint_key, value) = unit_call { @client.set(hint_key(namespace, hint_key), value.b) }

    def batch_put_nodes_with_hint(nodes, namespace, hint_key_value, value)
      keys = nodes.map { |node| family('node:', node.key) }
      arguments = nodes.flat_map { |node| ['1'.b, node.value.b] }
      keys << hint_key(namespace, hint_key_value)
      arguments.concat(['1'.b, value.b])
      mutate(keys, arguments)
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def get_root_manifest(name) = optional_call { @client.get(family('root:', name)) }
    def put_root_manifest(name, manifest) = unit_call { @client.set(family('root:', name), manifest.b) }
    def delete_root_manifest(name) = unit_call { @client.del(family('root:', name)) }

    def compare_and_swap_root_manifest(name, expected, replacement)
      response = synchronize do
        @client.eval(
          CAS_SCRIPT, keys: [family('root:', name)],
          argv: [flag(expected.present), expected.value.b, flag(replacement.present), replacement.value.b]
        )
      end
      validate_array(response, 3, 'CAS')
      RootCasResultRecord.new(
        applied: response[0].to_i == 1,
        current: optional_parts(response[1], response[2]), error: nil
      )
    rescue StandardError => error
      RootCasResultRecord.new(applied: false, current: optional(nil), error: store_error(error))
    end

    def list_root_manifests
      family_prefix = @prefix + 'root:'.b
      values = synchronize do
        keys = scan_family(family_prefix).sort
        manifests = keys.empty? ? [] : @client.mget(*keys)
        keys.zip(manifests).filter_map do |key, manifest|
          NamedBytesRecord.new(name: key.byteslice(family_prefix.bytesize..), value: manifest.b) if manifest
        end
      end
      NamedBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      NamedBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def commit_transaction(nodes, conditions, roots)
      keys = conditions.map { |item| family('root:', item.name) }
      keys.concat(nodes.map { |item| family('node:', item.key) })
      keys.concat(roots.map { |item| family('root:', item.name) })
      arguments = [conditions.length, nodes.length, roots.length]
      conditions.each { |item| arguments.concat([flag(item.expected.present), item.expected.value.b]) }
      nodes.each { |item| arguments.concat([flag(item.value.present), item.value.value.b]) }
      roots.each { |item| arguments.concat([flag(item.replacement.present), item.replacement.value.b]) }
      response = synchronize { @client.eval(TRANSACTION_SCRIPT, keys: keys, argv: arguments) }
      raise 'Redis returned an invalid transaction response' unless response.is_a?(Array) && !response.empty?
      return TransactionResultRecord.new(applied: true, conflict: nil, error: nil) if response[0].to_i == 1

      validate_array(response, 4, 'transaction conflict')
      index = response[1].to_i - 1
      raise 'Redis returned an invalid conflict index' unless index.between?(0, conditions.length - 1)
      condition = conditions[index]
      conflict = StoreTransactionConflictRecord.new(
        name: condition.name, expected: condition.expected,
        current: optional_parts(response[2], response[3])
      )
      TransactionResultRecord.new(applied: false, conflict: conflict, error: nil)
    rescue StandardError => error
      TransactionResultRecord.new(applied: false, conflict: nil, error: store_error(error))
    end

    private

    def synchronize(&block)
      raise 'Redis store is closed' if @closed
      @mutex.synchronize(&block)
    end

    def family(name, suffix) = @prefix + name.b + suffix.b
    def hint_key(namespace, key) = @prefix + 'hint:'.b + [namespace.bytesize].pack('Q>') + namespace.b + key.b
    def flag(value) = value ? '1'.b : '0'.b

    def scan_family(prefix)
      @client.scan_each.select { |key| key.b.start_with?(prefix) }.map(&:b)
    end

    def mutate(keys, arguments)
      synchronize { @client.eval(MUTATE_SCRIPT, keys: keys, argv: arguments) } unless keys.empty?
    end

    def optional(value) = OptionalBytesRecord.new(present: !value.nil?, value: value&.b || ''.b)
    def optional_parts(present, value) = present.to_i == 1 ? optional(value.b) : optional(nil)
    def unit = UnitResultRecord.new(error: nil)

    def optional_call
      OptionalBytesResultRecord.new(value: optional(synchronize { yield }), error: nil)
    rescue StandardError => error
      OptionalBytesResultRecord.new(value: optional(nil), error: store_error(error))
    end

    def unit_call
      synchronize { yield }
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def validate_array(value, minimum, label)
      raise "Redis returned an invalid #{label} response" unless value.is_a?(Array) && value.length >= minimum
    end

    def store_error(error)
      StoreErrorRecord.new(
        code: error.is_a?(ArgumentError) ? 'invalid_argument' : 'internal',
        message: 'Redis provider operation failed', retryable: false,
        provider_code: error.class.name
      )
    end
  end
end
