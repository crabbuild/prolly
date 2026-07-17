# frozen_string_literal: true

require 'mysql2'
require 'prolly'
require 'uri'

begin
  require 'prolly/store/mysql'
rescue LoadError
end

url = ENV['PROLLY_MYSQL_URL']
abort 'PROLLY_MYSQL_URL is not set; MySQL test skipped' unless url
raise 'MySQL adapter is not implemented' unless defined?(Prolly::MysqlRemoteStore)

uri = URI(url)
client = Mysql2::Client.new(
  host: uri.host, port: uri.port, username: uri.user, password: uri.password,
  database: uri.path.delete_prefix('/')
)
store = Prolly::MysqlRemoteStore.new(client)
store.initialize_schema
%w[prolly_nodes prolly_hints prolly_roots].each { |table| client.query("TRUNCATE #{table}") }

engine = Prolly.open_remote_prolly_engine(store, Prolly.default_config)
tree = engine.put(engine.create, 'key'.b, 'value'.b)
raise 'Rust/Ruby MySQL round trip failed' unless engine.get(tree, 'key'.b) == 'value'.b

missing = Prolly::OptionalBytesRecord.new(present: false, value: ''.b)
results = 32.times.map do |index|
  Thread.new do
    replacement = Prolly::OptionalBytesRecord.new(present: true, value: [index].pack('n'))
    store.compare_and_swap_root_manifest('main'.b, missing, replacement)
  end
end.map(&:value)
unless results.count(&:applied) == 1
  details = results.map { |result| [result.applied, result.error&.provider_code] }.tally
  raise "MySQL CAS did not have exactly one winner: #{details.inspect}"
end

node = Prolly::NodeMutationRecord.new(
  key: 'x'.b * 32,
  value: Prolly::OptionalBytesRecord.new(present: true, value: 'must-not-write'.b)
)
condition = Prolly::RootConditionRecord.new(name: 'main'.b, expected: missing)
raise 'conflicting MySQL transaction applied' if store.commit_transaction([node], [condition], []).applied
raise 'conflicting MySQL transaction wrote a node' if store.get_node('x'.b * 32).value.present

invalid = store.put_node('x'.b * 33, 'value'.b)
raise 'MySQL CID limit was not enforced' unless invalid.error&.code == 'invalid_argument'

store.close
raise 'adapter closed caller-owned MySQL client' unless client.query('SELECT 1').first.values.first == 1
client.close

puts 'Ruby MySQL remote store passed'
