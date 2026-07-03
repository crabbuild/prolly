# frozen_string_literal: true

require 'prolly'

def user_key(tenant, user_id)
  "source/tenant/#{tenant}/user/#{user_id}".b
end

def encode_user(tenant, user_id, status, display_name)
  [tenant, user_id, status, display_name].join('|').b
end

def decode_user(value)
  value.split('|', 4)
end

def status_index_prefix(tenant, status)
  "index/user-by-status/tenant/#{tenant}/status/#{status}/".b
end

def status_index_key(user)
  tenant, user_id, status, = user
  status_index_prefix(tenant, status) + user_id.b
end

def put_user(engine, tree, tenant, user_id, status, display_name)
  engine.put(tree, user_key(tenant, user_id), encode_user(tenant, user_id, status, display_name))
end

def build_status_index(engine, source)
  index = engine.create
  engine.range(source, 'source/'.b, 'source0'.b).each do |entry|
    index = engine.put(index, status_index_key(decode_user(entry.value)), '1'.b)
  end
  index
end

def apply_source_diff(engine, index, changes)
  changes.each do |change|
    case change.kind
    when Prolly::DiffKind::ADDED
      index = engine.put(index, status_index_key(decode_user(change.value)), '1'.b)
    when Prolly::DiffKind::REMOVED
      index = engine.delete(index, status_index_key(decode_user(change.value)))
    when Prolly::DiffKind::CHANGED
      old_key = status_index_key(decode_user(change.old_value))
      new_key = status_index_key(decode_user(change.new_value))
      next if old_key == new_key

      index = engine.delete(index, old_key)
      index = engine.put(index, new_key, '1'.b)
    end
  end
  index
end

def users_by_status(engine, index, tenant, status)
  start = status_index_prefix(tenant, status)
  engine.range(index, start, Prolly.prefix_end(start))
end

engine = Prolly::ProllyEngine.memory(Prolly.default_config)
empty = engine.create

source_v1 = put_user(engine, empty, 'acme', 'u001', 'active', 'Ada')
source_v1 = put_user(engine, source_v1, 'acme', 'u002', 'invited', 'Grace')
index_v1 = build_status_index(engine, source_v1)

source_v2 = put_user(engine, source_v1, 'acme', 'u002', 'active', 'Grace')
source_v2 = put_user(engine, source_v2, 'globex', 'u003', 'active', 'Linus')

source_changes = engine.diff(source_v1, source_v2)
raise 'expected two source changes' unless source_changes.length == 2

index_v2 = apply_source_diff(engine, index_v1, source_changes)
rebuilt_index_v2 = build_status_index(engine, source_v2)
raise 'incremental index does not match rebuilt index' unless index_v2 == rebuilt_index_v2

raise 'expected two acme active users' unless users_by_status(engine, index_v2, 'acme', 'active').length == 2
raise 'expected no acme invited users' unless users_by_status(engine, index_v2, 'acme', 'invited').empty?
raise 'expected one globex active user' unless users_by_status(engine, index_v2, 'globex', 'active').length == 1

puts "secondary_index: applied #{source_changes.length} source diffs"
