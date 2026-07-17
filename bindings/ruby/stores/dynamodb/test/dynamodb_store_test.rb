# frozen_string_literal: true
require 'aws-sdk-dynamodb'
require 'prolly'
require 'stringio'
begin
  require 'prolly/store/dynamodb'
rescue LoadError
end

endpoint = ENV['PROLLY_DYNAMODB_ENDPOINT']
abort 'PROLLY_DYNAMODB_ENDPOINT is not set; DynamoDB test skipped' unless endpoint
raise 'DynamoDB adapter is not implemented' unless defined?(Prolly::DynamoDbRemoteStore)

client = Aws::DynamoDB::Client.new(
  endpoint: endpoint, region: 'us-west-2',
  credentials: Aws::Credentials.new('local', 'local')
)
table = "prolly_ruby_#{Process.pid}_#{Time.now.to_i}_#{rand(1_000_000)}"
prefix = 'prolly:test:ruby:'.b
store = Prolly::DynamoDbRemoteStore.new(client, table_name: table, key_prefix: prefix)
store.initialize_table

probe = store.put_node('probe'.b, 'value'.b)
raise "DynamoDB direct write failed: #{probe.error&.provider_code}" if probe.error

engine = Prolly.open_remote_prolly_engine(store, Prolly.default_config)
tree = engine.put(engine.create, 'key'.b, 'value'.b)
raise 'Rust/Ruby DynamoDB round trip failed' unless engine.get(tree, 'key'.b) == 'value'.b

cid = [0, 127, 128, 255].pack('C*') * 8
store.put_node(cid, 'node'.b)
raw = client.get_item(table_name: table, key: { 'pk' => StringIO.new(prefix + 'node:'.b + cid) })
raise 'DynamoDB binary layout mismatch' unless raw.item['value'].string == 'node'.b

missing = Prolly::OptionalBytesRecord.new(present: false, value: ''.b)
results = 32.times.map do |index|
  Thread.new do
    store.compare_and_swap_root_manifest(
      'main'.b, missing,
      Prolly::OptionalBytesRecord.new(present: true, value: [index].pack('n'))
    )
  end
end.map(&:value)
raise 'DynamoDB CAS did not have one winner' unless results.count(&:applied) == 1

node = Prolly::NodeMutationRecord.new(key: 'rollback'.b, value: Prolly::OptionalBytesRecord.new(present: true, value: 'bad'.b))
condition = Prolly::RootConditionRecord.new(name: 'main'.b, expected: missing)
raise 'DynamoDB conflict applied' if store.commit_transaction([node], [condition], []).applied
raise 'DynamoDB conflict wrote node' if store.get_node('rollback'.b).value.present

store.close
raise 'adapter closed DynamoDB client' unless client.describe_table(table_name: table).table.table_name == table
client.delete_table(table_name: table)
puts 'Ruby DynamoDB remote store passed'
