# frozen_string_literal: true

require 'pg'
require 'prolly'

begin
  require 'prolly/store/postgres'
rescue LoadError
  # The first red run intentionally reaches the assertion below.
end

url = ENV['PROLLY_POSTGRES_URL']
abort 'PROLLY_POSTGRES_URL is not set; PostgreSQL test skipped' unless url
raise 'PostgreSQL adapter is not implemented' unless defined?(Prolly::PostgresRemoteStore)

connection = PG.connect(url)
store = Prolly::PostgresRemoteStore.new(connection)
store.initialize_schema
connection.exec('TRUNCATE prolly_nodes, prolly_hints, prolly_roots')

engine = Prolly.open_remote_prolly_engine(store, Prolly.default_config)
tree = engine.put(engine.create, 'key'.b, 'value'.b)
raise 'Rust/Ruby PostgreSQL round trip failed' unless engine.get(tree, 'key'.b) == 'value'.b

missing = Prolly::OptionalBytesRecord.new(present: false, value: ''.b)
results = 32.times.map do |index|
  Thread.new do
    replacement = Prolly::OptionalBytesRecord.new(present: true, value: [index].pack('n'))
    store.compare_and_swap_root_manifest('main'.b, missing, replacement)
  end
end.map(&:value)
raise 'PostgreSQL CAS did not have exactly one winner' unless results.count(&:applied) == 1

node = Prolly::NodeMutationRecord.new(
  key: 'x'.b * 32,
  value: Prolly::OptionalBytesRecord.new(present: true, value: 'must-not-write'.b)
)
root = Prolly::RootWriteRecord.new(
  name: 'other'.b,
  replacement: Prolly::OptionalBytesRecord.new(present: true, value: 'must-not-publish'.b)
)
condition = Prolly::RootConditionRecord.new(name: 'main'.b, expected: missing)
result = store.commit_transaction([node], [condition], [root])
raise 'conflicting PostgreSQL transaction applied' if result.applied
raise 'conflicting PostgreSQL transaction wrote a node' if store.get_node('x'.b * 32).value.present

store.close
raise 'adapter closed caller-owned PostgreSQL connection' unless connection.exec('SELECT 1').getvalue(0, 0) == '1'
connection.close

puts 'Ruby PostgreSQL remote store passed'
