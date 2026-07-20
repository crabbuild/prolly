# frozen_string_literal: true

require 'google/cloud/spanner'
require 'prolly'
require 'stringio'

module Prolly
  class SpannerRemoteStore < ForeignRemoteStore
    DDL = [
      "CREATE TABLE ProllyNodes (\n  Cid BYTES(32) NOT NULL,\n  Node BYTES(MAX) NOT NULL\n) PRIMARY KEY (Cid)",
      "CREATE TABLE ProllyHints (\n  Namespace BYTES(MAX) NOT NULL,\n  HintKey BYTES(MAX) NOT NULL,\n  Value BYTES(MAX) NOT NULL\n) PRIMARY KEY (Namespace, HintKey)",
      "CREATE TABLE ProllyRoots (\n  Name BYTES(MAX) NOT NULL,\n  Manifest BYTES(MAX) NOT NULL\n) PRIMARY KEY (Name)"
    ].freeze

    def initialize(client, narrow_client: false)
      unless narrow_client || client.is_a?(Google::Cloud::Spanner::Client)
        raise ArgumentError, 'client must be Google::Cloud::Spanner::Client'
      end

      @client = narrow_client ? client : SdkClient.new(client)
      @closed = false
    end

    def self.from_client(client)
      raise ArgumentError, 'Cloud Spanner client is required' unless client
      new(client, narrow_client: true)
    end

    def close = @closed = true

    def descriptor
      value = StoreDescriptorRecord.new(
        protocol_major: STORE_PROTOCOL_MAJOR, adapter_name: 'spanner-v1', provider: 'spanner', schema_version: 1,
        capabilities: StoreCapabilitiesRecord.new(
          native_batch_reads: false, atomic_batch_writes: true, node_scan: true,
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

    def get_node(cid) = optional_call { @client.get_node(cid.b) }
    def put_node(cid, value) = unit_call { apply([[:upsert_node, cid.b, value.b]]) }
    def delete_node(cid) = unit_call { apply([[:delete_node, cid.b]]) }

    def batch_nodes(operations)
      mutations = operations.map do |item|
        item.value.present ? [:upsert_node, item.key.b, item.value.value.b] : [:delete_node, item.key.b]
      end
      unit_call { apply(mutations) }
    end

    def batch_get_nodes_ordered(cids)
      values = ensure_open { cids.map { |cid| optional(@client.get_node(cid.b)) } }
      OptionalBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      OptionalBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def list_node_cids
      values = ensure_open { @client.list_node_cids.filter { |cid| cid.bytesize == 32 }.sort }
      BytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      BytesListResultRecord.new(values: [], error: store_error(error))
    end

    def get_hint(namespace, hint_key) = optional_call { @client.get_hint(namespace.b, hint_key.b) }
    def put_hint(namespace, hint_key, value) = unit_call { apply([[:upsert_hint, namespace.b, hint_key.b, value.b]]) }

    def batch_put_nodes_with_hint(nodes, namespace, hint_key, value)
      mutations = nodes.map { |node| [:upsert_node, node.key.b, node.value.b] }
      mutations << [:upsert_hint, namespace.b, hint_key.b, value.b]
      unit_call { apply(mutations) }
    end

    def get_root_manifest(name) = optional_call { @client.get_root(name.b) }
    def put_root_manifest(name, manifest) = unit_call { apply([[:upsert_root, name.b, manifest.b]]) }
    def delete_root_manifest(name) = unit_call { apply([[:delete_root, name.b]]) }

    def compare_and_swap_root_manifest(name, expected, replacement)
      result = ensure_open do
        @client.read_write do |transaction|
          current = transaction.get_root(name.b)
          unless matches?(current, expected)
            next RootCasResultRecord.new(applied: false, current: optional(current), error: nil)
          end
          transaction.buffer([root_mutation(name.b, replacement)])
          RootCasResultRecord.new(applied: true, current: replacement, error: nil)
        end
      end
      result
    rescue StandardError => error
      RootCasResultRecord.new(applied: false, current: optional(nil), error: store_error(error))
    end

    def list_root_manifests
      values = ensure_open do
        @client.list_roots.sort_by(&:first).map do |name, manifest|
          NamedBytesRecord.new(name: name, value: manifest)
        end
      end
      NamedBytesListResultRecord.new(values: values, error: nil)
    rescue StandardError => error
      NamedBytesListResultRecord.new(values: [], error: store_error(error))
    end

    def commit_transaction(nodes, conditions, roots)
      result = ensure_open do
        @client.read_write do |transaction|
          conflict = conditions.filter_map do |condition|
            current = transaction.get_root(condition.name.b)
            next if matches?(current, condition.expected)
            StoreTransactionConflictRecord.new(
              name: condition.name.b, expected: condition.expected, current: optional(current)
            )
          end.first
          next TransactionResultRecord.new(applied: false, conflict: conflict, error: nil) if conflict

          mutations = nodes.map do |node|
            node.value.present ? [:upsert_node, node.key.b, node.value.value.b] : [:delete_node, node.key.b]
          end
          roots.each { |root| mutations << root_mutation(root.name.b, root.replacement) }
          transaction.buffer(mutations)
          TransactionResultRecord.new(applied: true, conflict: nil, error: nil)
        end
      end
      result
    rescue StandardError => error
      TransactionResultRecord.new(applied: false, conflict: nil, error: store_error(error))
    end

    private

    def apply(mutations) = ensure_open { @client.apply(mutations) }

    def ensure_open
      raise 'Cloud Spanner store is closed' if @closed
      yield
    end

    def optional(value) = OptionalBytesRecord.new(present: !value.nil?, value: value || ''.b)
    def matches?(current, expected) = expected.present ? current == expected.value : current.nil?
    def root_mutation(name, replacement) = replacement.present ? [:upsert_root, name, replacement.value.b] : [:delete_root, name]

    def optional_call
      OptionalBytesResultRecord.new(value: optional(ensure_open { yield }), error: nil)
    rescue StandardError => error
      OptionalBytesResultRecord.new(value: optional(nil), error: store_error(error))
    end

    def unit_call
      yield
      UnitResultRecord.new(error: nil)
    rescue StandardError => error
      UnitResultRecord.new(error: store_error(error))
    end

    def store_error(error)
      code = grpc_code(error)
      classification, retryable = case code
                                  when 8 then ['resource_exhausted', true]
                                  when 4, 10, 14 then ['unavailable', true]
                                  else ['internal', false]
                                  end
      StoreErrorRecord.new(
        code: classification, message: 'Cloud Spanner provider operation failed',
        retryable: retryable, provider_code: code ? "grpc:#{code}" : error.class.name
      )
    end

    def grpc_code(error)
      value = error.respond_to?(:code) ? error.code : nil
      return value if value.is_a?(Integer)
      return value.to_i if value.respond_to?(:to_i) && value.to_i.positive?

      names = { deadline_exceeded: 4, resource_exhausted: 8, aborted: 10, unavailable: 14 }
      names[value.to_s.downcase.to_sym]
    end

    class SdkClient
      def initialize(client) = @client = client

      def get_node(key) = read_value(@client, 'ProllyNodes', [:Node], [key])
      def get_hint(namespace, key) = read_value(@client, 'ProllyHints', [:Value], [namespace, key])
      def get_root(name) = read_value(@client, 'ProllyRoots', [:Manifest], [name])

      def list_node_cids
        @client.read('ProllyNodes', [:Cid], keys: []).rows.map { |row| binary(row[0]) }
      end

      def list_roots
        @client.read('ProllyRoots', %i[Name Manifest], keys: []).rows.map do |row|
          [binary(row[0]), binary(row[1])]
        end
      end

      def apply(mutations)
        @client.commit { |commit| buffer_sdk(commit, mutations) }
      end

      def read_write
        result = nil
        @client.transaction { |transaction| result = yield SdkTransaction.new(transaction) }
        result
      end

      private

      def read_value(reader, table, columns, key)
        row = reader.read(table, columns, keys: [key.map { |value| io(value) }]).rows.first
        row ? binary(row[0]) : nil
      end

      def buffer_sdk(target, mutations)
        mutations.each do |mutation|
          case mutation[0]
          when :upsert_node
            target.upsert 'ProllyNodes', { Cid: io(mutation[1]), Node: io(mutation[2]) }
          when :delete_node
            target.delete 'ProllyNodes', [[io(mutation[1])]]
          when :upsert_hint
            target.upsert 'ProllyHints', { Namespace: io(mutation[1]), HintKey: io(mutation[2]), Value: io(mutation[3]) }
          when :upsert_root
            target.upsert 'ProllyRoots', { Name: io(mutation[1]), Manifest: io(mutation[2]) }
          when :delete_root
            target.delete 'ProllyRoots', [[io(mutation[1])]]
          else
            raise ArgumentError, 'unknown Cloud Spanner mutation'
          end
        end
      end

      def io(value) = StringIO.new(value.b)
      def binary(value) = value.respond_to?(:string) ? value.string.b : value.b

      class SdkTransaction
        def initialize(transaction) = @transaction = transaction

        def get_root(name)
          row = @transaction.read('ProllyRoots', [:Manifest], keys: [[StringIO.new(name.b)]]).rows.first
          value = row&.[](0)
          value.respond_to?(:string) ? value.string.b : value&.b
        end

        def buffer(mutations)
          SdkClient.allocate.send(:buffer_sdk, @transaction, mutations)
        end
      end
    end
  end
end
