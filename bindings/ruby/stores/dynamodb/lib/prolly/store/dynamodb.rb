# frozen_string_literal: true

require 'aws-sdk-dynamodb'
require 'prolly'
require 'stringio'

module Prolly
  class DynamoDbRemoteStore < ForeignRemoteStore
    BATCH_GET_LIMIT = 100
    BATCH_WRITE_LIMIT = 25
    TRANSACTION_LIMIT = 100
    RETRY_LIMIT = 8

    def initialize(client, table_name:, key_prefix: 'prolly:'.b)
      raise ArgumentError, 'client must be Aws::DynamoDB::Client' unless client.is_a?(Aws::DynamoDB::Client)
      raise ArgumentError, 'table name is required' if table_name.strip.empty?

      @client = client
      @table = table_name
      @prefix = key_prefix.b
      @mutex = Mutex.new
      @closed = false
    end

    def close = @closed = true

    def initialize_table
      synchronize do
        begin
          validate_table(@client.describe_table(table_name: @table).table)
          next
        rescue Aws::DynamoDB::Errors::ResourceNotFoundException
        end
        begin
          @client.create_table(
            table_name: @table,
            attribute_definitions: [{ attribute_name: 'pk', attribute_type: 'B' }],
            key_schema: [{ attribute_name: 'pk', key_type: 'HASH' }], billing_mode: 'PAY_PER_REQUEST'
          )
        rescue Aws::DynamoDB::Errors::ResourceInUseException
        end
        active = false
        100.times do
          begin
            table = @client.describe_table(table_name: @table).table
            if table.table_status == 'ACTIVE'
              validate_table(table)
              active = true
              break
            end
          rescue Aws::DynamoDB::Errors::ResourceNotFoundException
          end
          sleep 0.05
        end
        raise 'DynamoDB table did not become active' unless active
      end
    end

    def descriptor
      value = StoreDescriptorRecord.new(
        protocol_major: STORE_PROTOCOL_MAJOR, adapter_name: 'dynamodb-v1', provider: 'dynamodb', schema_version: 1,
        capabilities: StoreCapabilitiesRecord.new(
          native_batch_reads: true, atomic_batch_writes: false, node_scan: true,
          hints: true, atomic_nodes_and_hint: false, root_scan: true,
          root_compare_and_swap: true, transactions: true, read_parallelism: 1
        ),
        limits: StoreLimitsRecord.new(
          max_batch_read_items: BATCH_GET_LIMIT, max_batch_write_items: BATCH_WRITE_LIMIT,
          max_transaction_operations: TRANSACTION_LIMIT, max_node_bytes: nil
        )
      )
      StoreDescriptorResultRecord.new(value: value, error: nil)
    end

    def get_node(cid) = optional_call { get_unlocked(family('node:', cid)) }
    def put_node(cid, value) = unit_call { put_unlocked(family('node:', cid), value) }
    def delete_node(cid) = unit_call { delete_unlocked(family('node:', cid)) }

    def batch_nodes(operations)
      requests = operations.map do |item|
        key = family('node:', item.key)
        item.value.present ? { put_request: { item: item(key, item.value.value) } } : { delete_request: { key: key_item(key) } }
      end
      synchronize { batch_write_unlocked(requests) }
      unit
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def batch_get_nodes_ordered(cids)
      values = synchronize do
        storage_keys = cids.map { |cid| family('node:', cid) }
        found = {}
        storage_keys.uniq.each_slice(BATCH_GET_LIMIT) do |slice|
          pending = slice.map { |key| key_item(key) }
          RETRY_LIMIT.times do |attempt|
            break if pending.empty?
            output = @client.batch_get_item(request_items: {
              @table => { keys: pending, consistent_read: true, projection_expression: '#pk, #value', expression_attribute_names: { '#pk' => 'pk', '#value' => 'value' } }
            })
            output.responses.fetch(@table, []).each { |entry| found[binary(entry, 'pk')] = binary(entry, 'value') }
            pending = output.unprocessed_keys.fetch(@table, Aws::DynamoDB::Types::KeysAndAttributes.new).keys || []
            raise 'DynamoDB batch get left keys unprocessed' if !pending.empty? && attempt + 1 == RETRY_LIMIT
            sleep(0.01 * (2**[attempt, 6].min)) unless pending.empty?
          end
        end
        storage_keys.map { |key| optional(found[key]) }
      end
      OptionalBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      OptionalBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def list_node_cids
      prefix = family('node:', ''.b)
      values = synchronize { scan_keys(prefix).filter_map { |key| suffix = key.byteslice(prefix.bytesize..); suffix if suffix.bytesize == 32 }.sort }
      BytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      BytesListResultRecord.new(values: [], error: store_error(error))
    end

    def get_hint(namespace, hint_key) = optional_call { get_unlocked(hint_key(namespace, hint_key)) }
    def put_hint(namespace, hint_key, value) = unit_call { put_unlocked(hint_key(namespace, hint_key), value) }

    def batch_put_nodes_with_hint(nodes, namespace, hint_key_value, value)
      mutations = nodes.map { |node| NodeMutationRecord.new(key: node.key, value: optional(node.value)) }
      result = batch_nodes(mutations)
      return result if result.error
      put_hint(namespace, hint_key_value, value)
    end

    def get_root_manifest(name) = optional_call { get_unlocked(family('root:', name)) }
    def put_root_manifest(name, manifest) = unit_call { put_unlocked(family('root:', name), manifest) }
    def delete_root_manifest(name) = unit_call { delete_unlocked(family('root:', name)) }

    def compare_and_swap_root_manifest(name, expected, replacement)
      key = family('root:', name)
      synchronize do
        begin
          if replacement.present
            @client.put_item(table_name: @table, item: item(key, replacement.value), **condition(expected))
          else
            @client.delete_item(table_name: @table, key: key_item(key), **condition(expected))
          end
          RootCasResultRecord.new(applied: true, current: replacement, error: nil)
        rescue Aws::DynamoDB::Errors::ConditionalCheckFailedException
          RootCasResultRecord.new(applied: false, current: optional(get_unlocked(key)), error: nil)
        end
      end
    rescue StandardError => error
      RootCasResultRecord.new(applied: false, current: optional(nil), error: store_error(error))
    end

    def list_root_manifests
      prefix = family('root:', ''.b)
      values = synchronize do
        scan_keys(prefix).sort.filter_map do |key|
          manifest = get_unlocked(key)
          NamedBytesRecord.new(name: key.byteslice(prefix.bytesize..), value: manifest) if manifest
        end
      end
      NamedBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      NamedBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def commit_transaction(nodes, conditions, roots)
      root_names = roots.map(&:name)
      count = nodes.length + roots.length + conditions.count { |entry| !root_names.include?(entry.name) }
      raise ArgumentError, "DynamoDB transaction exceeds #{TRANSACTION_LIMIT} operations" if count > TRANSACTION_LIMIT
      by_name = conditions.to_h { |entry| [entry.name, entry] }
      transact_items = []
      conditions.each do |entry|
        next if root_names.include?(entry.name)
        transact_items << { condition_check: { table_name: @table, key: key_item(family('root:', entry.name)), **condition(entry.expected) } }
      end
      roots.each do |root|
        conditional = by_name.key?(root.name) ? condition(by_name[root.name].expected) : {}
        transact_items << if root.replacement.present
          { put: { table_name: @table, item: item(family('root:', root.name), root.replacement.value), **conditional } }
        else
          { delete: { table_name: @table, key: key_item(family('root:', root.name)), **conditional } }
        end
      end
      nodes.each do |node|
        key = family('node:', node.key)
        transact_items << if node.value.present
          { put: { table_name: @table, item: item(key, node.value.value) } }
        else
          { delete: { table_name: @table, key: key_item(key) } }
        end
      end
      synchronize do
        @client.transact_write_items(transact_items: transact_items) unless transact_items.empty?
      rescue Aws::DynamoDB::Errors::TransactionCanceledException
        conditions.each do |entry|
          current = get_unlocked(family('root:', entry.name))
          next if matches?(current, entry.expected)
          conflict = StoreTransactionConflictRecord.new(name: entry.name, expected: entry.expected, current: optional(current))
          return TransactionResultRecord.new(applied: false, conflict: conflict, error: nil)
        end
        raise
      end
      TransactionResultRecord.new(applied: true, conflict: nil, error: nil)
    rescue StandardError => error
      TransactionResultRecord.new(applied: false, conflict: nil, error: store_error(error))
    end

    private

    def synchronize(&block)
      raise 'DynamoDB store is closed' if @closed
      @mutex.synchronize(&block)
    end

    def family(name, suffix) = @prefix + name.b + suffix.b
    def hint_key(namespace, key) = @prefix + 'hint:'.b + [namespace.bytesize].pack('Q>') + namespace.b + key.b
    def key_item(key) = { 'pk' => StringIO.new(key.b) }
    def item(key, value) = key_item(key).merge('value' => StringIO.new(value.b))

    def binary(entry, name)
      value = entry[name]
      result = if value.respond_to?(:string)
        value.string
      elsif value.respond_to?(:b) && !value.is_a?(Hash)
        value.b
      else
        value&.fetch(:b, nil)
      end
      raise "DynamoDB item has invalid #{name}" unless result
      result.b
    end

    def get_unlocked(key)
      output = @client.get_item(table_name: @table, key: key_item(key), consistent_read: true, projection_expression: '#value', expression_attribute_names: { '#value' => 'value' })
      output.item.empty? ? nil : binary(output.item, 'value')
    end
    def put_unlocked(key, value) = @client.put_item(table_name: @table, item: item(key, value))
    def delete_unlocked(key) = @client.delete_item(table_name: @table, key: key_item(key))

    def batch_write_unlocked(requests)
      requests.each_slice(BATCH_WRITE_LIMIT) do |slice|
        pending = slice
        RETRY_LIMIT.times do |attempt|
          break if pending.empty?
          output = @client.batch_write_item(request_items: { @table => pending })
          pending = output.unprocessed_items.fetch(@table, [])
          raise 'DynamoDB batch write left requests unprocessed' if !pending.empty? && attempt + 1 == RETRY_LIMIT
          sleep(0.01 * (2**[attempt, 6].min)) unless pending.empty?
        end
      end
    end

    def scan_keys(prefix)
      keys = []
      start = nil
      loop do
        request = { table_name: @table, consistent_read: true, projection_expression: '#pk', filter_expression: 'begins_with(#pk, :prefix)', expression_attribute_names: { '#pk' => 'pk' }, expression_attribute_values: { ':prefix' => StringIO.new(prefix) } }
        request[:exclusive_start_key] = start if start
        output = @client.scan(**request)
        keys.concat(output.items.map { |entry| binary(entry, 'pk') })
        start = output.last_evaluated_key
        break if start.nil? || start.empty?
      end
      keys
    end

    def condition(expected)
      if expected.present
        { condition_expression: '#value = :expected', expression_attribute_names: { '#value' => 'value' }, expression_attribute_values: { ':expected' => StringIO.new(expected.value) } }
      else
        { condition_expression: 'attribute_not_exists(#pk)', expression_attribute_names: { '#pk' => 'pk' } }
      end
    end

    def optional(value) = OptionalBytesRecord.new(present: !value.nil?, value: value || ''.b)
    def unit = UnitResultRecord.new(error: nil)
    def matches?(current, expected) = expected.present ? current == expected.value : current.nil?

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

    def validate_table(table)
      valid_key = table.key_schema.length == 1 && table.key_schema[0].attribute_name == 'pk' && table.key_schema[0].key_type == 'HASH'
      valid_type = table.attribute_definitions.any? { |entry| entry.attribute_name == 'pk' && entry.attribute_type == 'B' }
      raise ArgumentError, 'DynamoDB table must use one binary HASH key named pk' unless valid_key && valid_type
    end

    def store_error(error)
      StoreErrorRecord.new(code: error.is_a?(ArgumentError) ? 'invalid_argument' : 'internal', message: 'DynamoDB provider operation failed', retryable: false, provider_code: "#{error.class.name}: #{error.message}")
    end
  end
end
