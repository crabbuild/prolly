# frozen_string_literal: true

require 'prolly'

class RubyMemoryRemoteStore < Prolly::ForeignRemoteStore
  attr_reader :nodes

  def initialize
    @nodes = {}
    @hints = {}
    @roots = {}
    @mutex = Mutex.new
  end

  def descriptor
    capabilities = Prolly::StoreCapabilitiesRecord.new(
      native_batch_reads: true, atomic_batch_writes: true, node_scan: true,
      hints: true, atomic_nodes_and_hint: true, root_scan: true,
      root_compare_and_swap: true, transactions: true, read_parallelism: 4
    )
    limits = Prolly::StoreLimitsRecord.new(
      max_batch_read_items: nil, max_batch_write_items: nil,
      max_transaction_operations: nil, max_node_bytes: nil
    )
    value = Prolly::StoreDescriptorRecord.new(
      protocol_major: 2, adapter_name: 'ruby-test-memory', provider: 'memory',
      schema_version: 1, capabilities: capabilities, limits: limits
    )
    Prolly::StoreDescriptorResultRecord.new(value: value, error: nil)
  end

  def get_node(cid) = optional_result(@nodes[cid])
  def put_node(cid, value) = unit { @nodes[cid.b] = value.b }
  def delete_node(cid) = unit { @nodes.delete(cid) }

  def batch_nodes(operations)
    unit do
      operations.each { |item| item.value.present ? @nodes[item.key.b] = item.value.value.b : @nodes.delete(item.key) }
    end
  end

  def batch_get_nodes_ordered(cids)
    Prolly::OptionalBytesListResultRecord.new(values: cids.map { |cid| optional(@nodes[cid]) }, error: nil)
  end

  def list_node_cids = Prolly::BytesListResultRecord.new(values: @nodes.keys.sort, error: nil)
  def get_hint(namespace, key) = optional_result(@hints[[namespace, key]])
  def put_hint(namespace, key, value) = unit { @hints[[namespace.b, key.b]] = value.b }

  def batch_put_nodes_with_hint(nodes, namespace, key, value)
    unit do
      nodes.each { |node| @nodes[node.key.b] = node.value.b }
      @hints[[namespace.b, key.b]] = value.b
    end
  end

  def get_root_manifest(name) = optional_result(@roots[name])
  def put_root_manifest(name, manifest) = unit { @roots[name.b] = manifest.b }
  def delete_root_manifest(name) = unit { @roots.delete(name) }

  def compare_and_swap_root_manifest(name, expected, replacement)
    @mutex.synchronize do
      current = @roots[name]
      matches = expected.present ? current == expected.value : current.nil?
      unless matches
        return Prolly::RootCasResultRecord.new(applied: false, current: optional(current), error: nil)
      end
      replacement.present ? @roots[name.b] = replacement.value.b : @roots.delete(name)
      Prolly::RootCasResultRecord.new(applied: true, current: optional(current), error: nil)
    end
  end

  def list_root_manifests
    values = @roots.sort.map { |name, value| Prolly::NamedBytesRecord.new(name: name, value: value) }
    Prolly::NamedBytesListResultRecord.new(values: values, error: nil)
  end

  def commit_transaction(nodes, conditions, roots)
    @mutex.synchronize do
      conditions.each do |condition|
        current = @roots[condition.name]
        matches = condition.expected.present ? current == condition.expected.value : current.nil?
        unless matches
          conflict = Prolly::StoreTransactionConflictRecord.new(
            name: condition.name, expected: condition.expected, current: optional(current)
          )
          return Prolly::TransactionResultRecord.new(applied: false, conflict: conflict, error: nil)
        end
      end
      nodes.each { |item| item.value.present ? @nodes[item.key.b] = item.value.value.b : @nodes.delete(item.key) }
      roots.each { |root| root.replacement.present ? @roots[root.name.b] = root.replacement.value.b : @roots.delete(root.name) }
      Prolly::TransactionResultRecord.new(applied: true, conflict: nil, error: nil)
    end
  end

  private

  def optional(value) = Prolly::OptionalBytesRecord.new(present: !value.nil?, value: value || ''.b)
  def optional_result(value) = Prolly::OptionalBytesResultRecord.new(value: optional(value), error: nil)

  def unit
    yield
    Prolly::UnitResultRecord.new(error: nil)
  end
end

store = RubyMemoryRemoteStore.new
engine = Prolly.open_remote_prolly_engine(store, Prolly.default_config)
tree = engine.put(engine.create, 'key'.b, 'value'.b)
raise 'Ruby remote store returned the wrong value' unless engine.get(tree, 'key'.b) == 'value'.b
raise 'Rust did not write nodes through the Ruby store' if store.nodes.empty?

puts 'Ruby async remote store bridge passed'
