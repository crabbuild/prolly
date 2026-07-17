# Prolly PostgreSQL store for Ruby

This provider gem implements the shared store protocol with the `pg` gem. It
borrows a caller-owned `PG::Connection`, serializes its use, and does not close
it. Missing-root publication is protected with PostgreSQL transaction advisory
locks.

```ruby
connection = PG.connect(ENV.fetch('PROLLY_POSTGRES_URL'))
store = Prolly::PostgresRemoteStore.new(connection)
store.initialize_schema
```
