# frozen_string_literal: true

require 'redis'
require 'prolly'

begin
  require 'prolly/store/redis'
rescue LoadError
end

url = ENV['PROLLY_REDIS_URL']
abort 'PROLLY_REDIS_URL is not set; Redis test skipped' unless url
raise 'Redis adapter is not implemented' unless defined?(Prolly::RedisRemoteStore)

client = Redis.new(url: url)
prefix = "prolly:test:ruby:#{Process.pid}:".b
store = Prolly::RedisRemoteStore.new(client, key_prefix: prefix)
store.clear_namespace

engine = Prolly.open_remote_prolly_engine(store, Prolly.default_config)
tree = engine.put(engine.create, 'key'.b, 'value'.b)
raise 'Rust/Ruby Redis round trip failed' unless engine.get(tree, 'key'.b) == 'value'.b

cid = [0, 127, 128, 255].pack('C*') * 8
namespace = [0, 255, 1].pack('C*')
hint_key = [128, 0].pack('C*')
store.put_node(cid, 'node'.b)
store.put_hint(namespace, hint_key, 'hint'.b)
raise 'Redis node layout mismatch' unless client.get(prefix + 'node:'.b + cid) == 'node'.b
encoded_hint = prefix + 'hint:'.b + [namespace.bytesize].pack('Q>') + namespace + hint_key
raise 'Redis hint layout mismatch' unless client.get(encoded_hint) == 'hint'.b

missing = Prolly::OptionalBytesRecord.new(present: false, value: ''.b)
results = 32.times.map do |index|
  Thread.new do
    replacement = Prolly::OptionalBytesRecord.new(present: true, value: [index].pack('n'))
    store.compare_and_swap_root_manifest('main'.b, missing, replacement)
  end
end.map(&:value)
raise 'Redis CAS did not have exactly one winner' unless results.count(&:applied) == 1

node = Prolly::NodeMutationRecord.new(
  key: 'rollback'.b,
  value: Prolly::OptionalBytesRecord.new(present: true, value: 'must-not-write'.b)
)
condition = Prolly::RootConditionRecord.new(name: 'main'.b, expected: missing)
root = Prolly::RootWriteRecord.new(
  name: 'other'.b,
  replacement: Prolly::OptionalBytesRecord.new(present: true, value: 'must-not-publish'.b)
)
raise 'conflicting Redis transaction applied' if store.commit_transaction([node], [condition], [root]).applied
raise 'conflicting Redis transaction wrote a node' if store.get_node('rollback'.b).value.present
raise 'conflicting Redis transaction wrote a root' if store.get_root_manifest('other'.b).value.present

store.close
raise 'adapter closed caller-owned Redis client' unless client.ping == 'PONG'
keys = client.scan_each.select { |key| key.b.start_with?(prefix) }
client.del(*keys) unless keys.empty?
client.close

puts 'Ruby Redis remote store passed'
