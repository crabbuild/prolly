# frozen_string_literal: true

require 'google/cloud/spanner'
require 'prolly'
begin
  require 'prolly/store/spanner'
rescue LoadError
end

raise 'Cloud Spanner adapter is not implemented' unless defined?(Prolly::SpannerRemoteStore)

class FakeSpannerTransaction
  attr_reader :mutations

  def initialize(state)
    @state = state
    @mutations = []
  end

  def get_root(name) = @state[:roots][name.b]
  def buffer(mutations) = @mutations.concat(Marshal.load(Marshal.dump(mutations)))
end

class FakeSpannerClient
  attr_accessor :failure, :block_apply
  attr_reader :state, :last_mutations

  def initialize
    @state = { nodes: {}, hints: {}, roots: {} }
    @last_mutations = []
    @mutex = Mutex.new
  end

  def get_node(key) = @state[:nodes][key.b]
  def get_hint(namespace, key) = @state[:hints][[namespace.b, key.b]]
  def get_root(name) = @state[:roots][name.b]
  def list_node_cids = @state[:nodes].keys
  def list_roots = @state[:roots].to_a

  def apply(mutations)
    raise @failure.tap { @failure = nil } if @failure
    sleep if @block_apply
    @mutex.synchronize do
      next_state = Marshal.load(Marshal.dump(@state))
      apply_to(next_state, mutations)
      @state = next_state
      @last_mutations = Marshal.load(Marshal.dump(mutations))
    end
  end

  def read_write
    @mutex.synchronize do
      next_state = Marshal.load(Marshal.dump(@state))
      transaction = FakeSpannerTransaction.new(next_state)
      result = yield transaction
      apply_to(next_state, transaction.mutations)
      @state = next_state
      result
    end
  end

  private

  def apply_to(state, mutations)
    mutations.each do |mutation|
      case mutation[0]
      when :upsert_node then state[:nodes][mutation[1]] = mutation[2]
      when :delete_node then state[:nodes].delete(mutation[1])
      when :upsert_hint then state[:hints][[mutation[1], mutation[2]]] = mutation[3]
      when :upsert_root then state[:roots][mutation[1]] = mutation[2]
      when :delete_root then state[:roots].delete(mutation[1])
      end
    end
  end
end

client = FakeSpannerClient.new
store = Prolly::SpannerRemoteStore.from_client(client)
expected_ddl = [
  "CREATE TABLE ProllyNodes (\n  Cid BYTES(32) NOT NULL,\n  Node BYTES(MAX) NOT NULL\n) PRIMARY KEY (Cid)",
  "CREATE TABLE ProllyHints (\n  Namespace BYTES(MAX) NOT NULL,\n  HintKey BYTES(MAX) NOT NULL,\n  Value BYTES(MAX) NOT NULL\n) PRIMARY KEY (Namespace, HintKey)",
  "CREATE TABLE ProllyRoots (\n  Name BYTES(MAX) NOT NULL,\n  Manifest BYTES(MAX) NOT NULL\n) PRIMARY KEY (Name)"
]
raise 'Spanner DDL changed' unless Prolly::SpannerRemoteStore::DDL == expected_ddl
raise 'Spanner batch must be atomic' unless store.descriptor.value.capabilities.atomic_batch_writes

nodes = 4.times.map { |index| Prolly::NodeEntryRecord.new(key: index.chr.b * 32, value: 'value'.b) }
store.batch_put_nodes_with_hint(nodes, 'namespace'.b, 'key'.b, 'hint'.b)
raise 'Spanner nodes and hint were not one batch' unless client.last_mutations.length == 5

missing = Prolly::OptionalBytesRecord.new(present: false, value: ''.b)
contenders = 32.times.map do |index|
  Thread.new do
    store.compare_and_swap_root_manifest(
      'main'.b, missing, Prolly::OptionalBytesRecord.new(present: true, value: [index].pack('n'))
    )
  end
end.map(&:value)
raise 'Spanner CAS did not have one winner' unless contenders.count(&:applied) == 1

node = Prolly::NodeMutationRecord.new(
  key: 'rollback'.b, value: Prolly::OptionalBytesRecord.new(present: true, value: 'bad'.b)
)
condition = Prolly::RootConditionRecord.new(name: 'main'.b, expected: missing)
conflict = store.commit_transaction([node], [condition], [])
raise 'Spanner conflict applied' if conflict.applied
raise 'Spanner conflict wrote node' if store.get_node('rollback'.b).value.present

engine = Prolly.open_remote_prolly_engine(store, Prolly.default_config)
tree = engine.put(engine.create, 'key'.b, 'value'.b)
raise 'Rust/Ruby Spanner round trip failed' unless engine.get(tree, 'key'.b) == 'value'.b

secret = 'spanner-service-account-secret'
client.failure = Class.new(StandardError) { attr_reader :code; define_method(:initialize) { |message| @code = 14; super(message) } }.new(secret)
error = store.put_node('error'.b, 'value'.b).error
raise 'Spanner unavailable error not classified' unless error.code == 'unavailable' && error.retryable
raise 'Spanner error leaked credentials' if error.message.include?(secret)

if ENV['SPANNER_EMULATOR_HOST']
  project = Google::Cloud::Spanner.new(project_id: 'prolly-test')
  instance = project.instance('prolly-test')
  database_id = "prolly_rb_#{Process.pid}_#{rand(1_000_000)}"
  job = instance.create_database(database_id, statements: Prolly::SpannerRemoteStore::DDL)
  job.wait_until_done!
  raise "Spanner database creation failed: #{job.error}" if job.error?
  database = job.database
  sdk_client = project.client('prolly-test', database_id)
  live_store = Prolly::SpannerRemoteStore.new(sdk_client)
  begin
    live_engine = Prolly.open_remote_prolly_engine(live_store, Prolly.default_config)
    live_tree = live_engine.put(live_engine.create, 'key'.b, 'value'.b)
    raise 'Ruby Spanner emulator round trip failed' unless live_engine.get(live_tree, 'key'.b) == 'value'.b
    live_store.close
    sdk_client.read('ProllyNodes', [:Cid], keys: :all).rows.first
  ensure
    sdk_client.close
    database.drop
  end
end

store.close
raise 'Spanner adapter closed borrowed client' unless client.get_node('unused'.b).nil?
puts 'Ruby Cloud Spanner remote store passed'
