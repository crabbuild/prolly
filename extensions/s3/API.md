# Prolly S3 API guide

The application-facing type is `prolly_s3_client::Client`.

| Goal | API |
|---|---|
| Create a repository | `Client::builder().initialize().await` |
| Reopen a repository | `Client::builder().open().await` |
| Write one file | `put_object`, `put_object_with_operation` |
| Read current or historical data | `get_object`, `get_object_at` |
| Read metadata or a byte range | `head_object`, `get_object_range` |
| Copy without uploading the payload | `copy_object` |
| Delete one or many files | `delete_object`, `delete_objects` |
| List data and common prefixes | `list_objects`, `list_objects_at`, `list_objects_delimited` |
| Inspect one file's history | `list_object_versions` |
| Atomically ingest many files | `ingest_objects`, `begin_commit`, `resume_commit` |
| Work on another branch | `create_branch`, `for_branch`, `delete_branch` |
| Pin a commit | `create_tag`, `tag`, `delete_tag` |
| Add a GC retention root | `create_retention_pin`, `delete_retention_pin` |
| Read history or compare snapshots | `log_bounded`, `diff_bounded` |
| Inspect or recover ref movement | `open_reflog`, `read_reflog_page`, `recover_branch` |
| Reset or restore | `reset_branch`, `start_restore`, `advance_restore` |
| Merge branches | `start_merge`, `advance_merge`, `publish_merge` |
| Inspect a merge | `merge_bases_page`, `merge_changes_page`, `merge_conflicts_page` |
| Check repository integrity | `start_fsck`, `advance_fsck` |
| Repair or transfer a snapshot | `start_repair_from`, `start_fetch_from`, `start_push_to` |
| Verify a logical backup | `start_backup_verification`, `advance_backup_verification` |
| Prewarm/inspect node caching | `prewarm_node_cache`, `node_cache_snapshot` |
| Check index health | `branch_index_health`, `wait_for_branch_indexes` |
| Observe S3 requests | `s3_operation_metrics`, `reset_s3_operation_metrics` |

## Write choices

Use `put_object` for an interactive single-file update. Use
`put_object_with_operation` when a request may be retried after an ambiguous
network response; persist and reuse the same `OperationId` with identical input.

Use a commit session for ingestion:

```rust
let mut commit = client
    .begin_commit()
    .message("daily import")
    .checkpoint_every(256)
    .start()
    .await?;

commit.put_object("reports/a.csv", a).await?;
commit.put_object("reports/b.csv", b).await?;
let receipt = commit.publish().await?;
```

Durable sessions checkpoint by default and can be reopened with
`resume_commit(batch_id)`. Use `.ephemeral()` only when restartability is
unnecessary and minimizing requests matters more.

## Pagination

`list_objects` returns `(snapshot, items, truncated)`. When `truncated` is
true, pass the last returned key as `after`. Merge and catalog APIs return
typed cursors; persist the returned cursor and resume from it.

## Consistency

A branch ref conditional write is the commit point. Payloads, nodes, commits,
and publication events are immutable and may be safely cached. Reads at an
explicit `CommitId` remain stable. Different branches have independent
publication lanes; writers to the same branch serialize through that branch's
authority and ref compare-and-swap.
