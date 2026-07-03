# frozen_string_literal: true

require 'tmpdir'

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))
require 'prolly'

def upsert(key, value)
  Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: key.b, value: value.b)
end

def delete(key)
  Prolly::MutationRecord.new(kind: Prolly::MutationKind::DELETE, key: key.b, value: nil)
end

def assert_equal(expected, actual, label = 'value')
  return if expected == actual

  raise "#{label}: expected #{expected.inspect}, got #{actual.inspect}"
end

Order = Struct.new(:tenant, :id, :status, :cents)

def order_key(order)
  "orders/source/tenant/#{order.tenant}/order/#{order.id}".b
end

def encode_order(order)
  "#{order.tenant}|#{order.id}|#{order.status}|#{order.cents}".b
end

def decode_order(value)
  tenant, id, status, cents = value.split('|'.b)
  Order.new(tenant, id, status, cents.to_i)
end

def view_key(tenant, status)
  "orders/view/by-status/tenant/#{tenant}/status/#{status}".b
end

def build_revenue_view(engine, source)
  totals = Hash.new(0)
  engine.range(source, 'orders/source/'.b, 'orders/source0'.b).each do |entry|
    order = decode_order(entry.value)
    totals[[order.tenant, order.status]] += order.cents
  end
  mutations = totals.sort.map do |(tenant, status), cents|
    Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: view_key(tenant, status), value: cents.to_s.b)
  end
  engine.batch(engine.create, mutations)
end

def materialized_view
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  o1 = Order.new('acme', 'o1', 'paid', 1200)
  o2 = Order.new('acme', 'o2', 'open', 500)
  source_v1 = engine.batch(
    engine.create,
    [
      Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: order_key(o1), value: encode_order(o1)),
      Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: order_key(o2), value: encode_order(o2))
    ]
  )
  paid_o2 = Order.new('acme', 'o2', 'paid', 500)
  source_v2 = engine.put(source_v1, order_key(paid_o2), encode_order(paid_o2))
  view_v2 = build_revenue_view(engine, source_v2)

  assert_equal '1700'.b, engine.get(view_v2, view_key('acme', 'paid')), 'paid revenue'
  assert_equal nil, engine.get(view_v2, view_key('acme', 'open')), 'open revenue'

  puts "materialized_view: folded #{engine.diff(source_v1, source_v2).length} source diff"
end

materialized_view
