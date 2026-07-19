use sqlx::{PgPool, Row};

use crate::measurement::{PgMetrics, PhysicalSize};

pub async fn initialize_benchmark_schema(pool: &PgPool) -> Result<(), sqlx::Error> {
    for statement in [
        "CREATE EXTENSION IF NOT EXISTS pg_stat_statements",
        "CREATE SCHEMA IF NOT EXISTS prolly_bench",
        "CREATE UNLOGGED TABLE IF NOT EXISTS prolly_bench.base_nodes (LIKE prolly_nodes INCLUDING ALL)",
        "CREATE UNLOGGED TABLE IF NOT EXISTS prolly_bench.base_hints (LIKE prolly_hints INCLUDING ALL)",
        "CREATE UNLOGGED TABLE IF NOT EXISTS prolly_bench.base_roots (LIKE prolly_roots INCLUDING ALL)",
    ] {
        sqlx::query(statement).execute(pool).await?;
    }
    Ok(())
}

pub async fn clear_production_tables(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("TRUNCATE prolly_hints, prolly_roots, prolly_nodes")
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn clear_all(pool: &PgPool) -> Result<(), sqlx::Error> {
    clear_production_tables(pool).await?;
    sqlx::query(
        "TRUNCATE prolly_bench.base_hints, prolly_bench.base_roots, prolly_bench.base_nodes",
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn snapshot_base(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "TRUNCATE prolly_bench.base_hints, prolly_bench.base_roots, prolly_bench.base_nodes",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("INSERT INTO prolly_bench.base_nodes SELECT * FROM prolly_nodes")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO prolly_bench.base_hints SELECT * FROM prolly_hints")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO prolly_bench.base_roots SELECT * FROM prolly_roots")
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

pub async fn restore_base(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("TRUNCATE prolly_hints, prolly_roots, prolly_nodes")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO prolly_nodes SELECT * FROM prolly_bench.base_nodes")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO prolly_hints SELECT * FROM prolly_bench.base_hints")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO prolly_roots SELECT * FROM prolly_bench.base_roots")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    for table in ["prolly_nodes", "prolly_hints", "prolly_roots"] {
        sqlx::query(&format!("ANALYZE {table}"))
            .execute(pool)
            .await?;
    }
    Ok(())
}

pub async fn production_counts(pool: &PgPool) -> Result<(i64, i64, i64), sqlx::Error> {
    let row = sqlx::query(
        "SELECT (SELECT count(*) FROM prolly_nodes) AS nodes, \
                (SELECT count(*) FROM prolly_hints) AS hints, \
                (SELECT count(*) FROM prolly_roots) AS roots",
    )
    .fetch_one(pool)
    .await?;
    Ok((
        row.try_get("nodes")?,
        row.try_get("hints")?,
        row.try_get("roots")?,
    ))
}

pub async fn reset_pg_stats(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_stat_statements_reset()")
        .execute(pool)
        .await?;
    sqlx::query("SELECT pg_stat_reset()").execute(pool).await?;
    Ok(())
}

pub async fn read_pg_metrics(pool: &PgPool) -> Result<PgMetrics, sqlx::Error> {
    let statements = sqlx::query(
        "SELECT COALESCE(sum(calls), 0)::bigint AS calls, \
                COALESCE(sum(total_exec_time), 0)::double precision AS execution_ms, \
                COALESCE(sum(shared_blks_hit), 0)::bigint AS shared_blks_hit, \
                COALESCE(sum(shared_blks_read), 0)::bigint AS shared_blks_read, \
                COALESCE(sum(shared_blks_dirtied), 0)::bigint AS shared_blks_dirtied, \
                COALESCE(sum(shared_blks_written), 0)::bigint AS shared_blks_written, \
                COALESCE(sum(temp_blks_read), 0)::bigint AS temp_blks_read, \
                COALESCE(sum(temp_blks_written), 0)::bigint AS temp_blks_written, \
                COALESCE(sum(wal_bytes), 0)::bigint AS wal_bytes \
         FROM pg_stat_statements \
         WHERE query NOT ILIKE '%pg_stat_statements%' \
           AND (query ILIKE '%prolly_nodes%' OR query ILIKE '%prolly_hints%' OR query ILIKE '%prolly_roots%')",
    )
    .fetch_one(pool)
    .await?;
    let database = sqlx::query(
        "SELECT xact_commit::bigint AS commits, xact_rollback::bigint AS rollbacks \
         FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await?;
    Ok(PgMetrics {
        statement_calls: nonnegative(statements.try_get::<i64, _>("calls")?),
        execution_ms: statements.try_get("execution_ms")?,
        shared_blks_hit: nonnegative(statements.try_get::<i64, _>("shared_blks_hit")?),
        shared_blks_read: nonnegative(statements.try_get::<i64, _>("shared_blks_read")?),
        shared_blks_dirtied: nonnegative(statements.try_get::<i64, _>("shared_blks_dirtied")?),
        shared_blks_written: nonnegative(statements.try_get::<i64, _>("shared_blks_written")?),
        temp_blks_read: nonnegative(statements.try_get::<i64, _>("temp_blks_read")?),
        temp_blks_written: nonnegative(statements.try_get::<i64, _>("temp_blks_written")?),
        wal_bytes: nonnegative(statements.try_get::<i64, _>("wal_bytes")?),
        commits: nonnegative(database.try_get::<i64, _>("commits")?),
        rollbacks: nonnegative(database.try_get::<i64, _>("rollbacks")?),
    })
}

pub async fn read_physical_size(pool: &PgPool) -> Result<PhysicalSize, sqlx::Error> {
    let row = sqlx::query(
        "SELECT pg_database_size(current_database())::bigint AS database_bytes, \
                (pg_total_relation_size('prolly_nodes') + \
                 pg_total_relation_size('prolly_hints') + \
                 pg_total_relation_size('prolly_roots'))::bigint AS table_bytes, \
                (pg_indexes_size('prolly_nodes') + \
                 pg_indexes_size('prolly_hints') + \
                 pg_indexes_size('prolly_roots'))::bigint AS index_bytes",
    )
    .fetch_one(pool)
    .await?;
    Ok(PhysicalSize {
        database_bytes: nonnegative(row.try_get::<i64, _>("database_bytes")?),
        prolly_table_bytes: nonnegative(row.try_get::<i64, _>("table_bytes")?),
        prolly_index_bytes: nonnegative(row.try_get::<i64, _>("index_bytes")?),
    })
}

pub async fn postgres_metadata(pool: &PgPool) -> Result<String, sqlx::Error> {
    let row = sqlx::query(
        "SELECT version() AS version, current_setting('shared_preload_libraries') AS preload, \
                current_setting('track_io_timing') AS track_io, \
                current_setting('max_connections') AS max_connections, \
                current_setting('shared_buffers') AS shared_buffers, \
                current_setting('work_mem') AS work_mem, \
                current_setting('synchronous_commit') AS synchronous_commit",
    )
    .fetch_one(pool)
    .await?;
    Ok(format!(
        "version={}\nshared_preload_libraries={}\ntrack_io_timing={}\nmax_connections={}\nshared_buffers={}\nwork_mem={}\nsynchronous_commit={}\n",
        row.try_get::<String, _>("version")?,
        row.try_get::<String, _>("preload")?,
        row.try_get::<String, _>("track_io")?,
        row.try_get::<String, _>("max_connections")?,
        row.try_get::<String, _>("shared_buffers")?,
        row.try_get::<String, _>("work_mem")?,
        row.try_get::<String, _>("synchronous_commit")?,
    ))
}

fn nonnegative(value: i64) -> u64 {
    value.max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use prolly_store_postgres::PostgresBackend;

    #[tokio::test]
    #[ignore = "requires PROLLY_STORE_POSTGRES_URL"]
    async fn snapshot_restore_round_trip() {
        let url = std::env::var("PROLLY_STORE_POSTGRES_URL").unwrap();
        let backend = PostgresBackend::connect(&url).await.unwrap();
        backend.initialize_schema().await.unwrap();
        initialize_benchmark_schema(backend.pool()).await.unwrap();
        clear_all(backend.pool()).await.unwrap();

        sqlx::query("INSERT INTO prolly_nodes(cid,node) VALUES($1,$2)")
            .bind(b"cid".as_slice())
            .bind(b"node".as_slice())
            .execute(backend.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO prolly_hints(namespace,key,value) VALUES($1,$2,$3)")
            .bind(b"ns".as_slice())
            .bind(b"key".as_slice())
            .bind(b"value".as_slice())
            .execute(backend.pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO prolly_roots(name,manifest) VALUES($1,$2)")
            .bind(b"root".as_slice())
            .bind(b"manifest".as_slice())
            .execute(backend.pool())
            .await
            .unwrap();

        snapshot_base(backend.pool()).await.unwrap();
        clear_production_tables(backend.pool()).await.unwrap();
        restore_base(backend.pool()).await.unwrap();

        let counts = production_counts(backend.pool()).await.unwrap();
        assert_eq!(counts, (1, 1, 1));
    }

    #[tokio::test]
    #[ignore = "requires PROLLY_STORE_POSTGRES_URL"]
    async fn statement_statistics_are_readable() {
        let url = std::env::var("PROLLY_STORE_POSTGRES_URL").unwrap();
        let backend = PostgresBackend::connect(&url).await.unwrap();
        backend.initialize_schema().await.unwrap();
        initialize_benchmark_schema(backend.pool()).await.unwrap();
        reset_pg_stats(backend.pool()).await.unwrap();
        sqlx::query("SELECT count(*) FROM prolly_nodes")
            .execute(backend.pool())
            .await
            .unwrap();
        let metrics = read_pg_metrics(backend.pool()).await.unwrap();
        assert!(metrics.statement_calls >= 1);
        let size = read_physical_size(backend.pool()).await.unwrap();
        assert!(size.database_bytes > 0);
    }
}
