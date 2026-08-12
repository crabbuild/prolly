# Prolly S3 API guide

The supported public API is documented with executable-shaped examples in
[client/README.md](client/README.md). That guide covers initialization, whole
object writes, exact historical reads, atomic multi-object commits, listing,
branches, diffs, conditional publication, and idempotency.

## Main entry points

| Goal | API |
|---|---|
| Create repository | `Client::builder().initialize()` |
| Open repository | `Client::builder().open()` |
| Open read-only | `Client::builder().read_only(true).open()` |
| Write whole object | `client.put_object().send()` |
| Read current object | `client.get_object().send()` |
| Read commit snapshot | `client.at(commit).await?` |
| Atomic multi-key commit | `client.begin_commit().start()` |
| Create multipart upload | `client.create_multipart_upload().send()` |
| List current objects | `client.list_objects_v2().send()` |
| List logical versions | `client.list_object_versions().send()` |
| Bounded history | `log_bounded`, `first_parent_ancestor_bounded` |
| Scalable ref listing | `list_branch_catalog_page`, `list_tag_catalog_page` |
| Bounded structural diff | `diff_bounded` |
| Maintain derived indexes | `advance_scale_indexes` |
| Persistent node cache | `FoyerNodeCache` plus `ClientBuilder::node_cache` |
| Partitioned GC | `start_gc_epoch`, `advance_gc_epoch`, `sweep_gc_epoch` |
| Merge/restore | `merge`, `restore` |
| Verify repository | `fsck` |
| Explicit branch-writer handoff | `takeover_branch_writer` |
| SDK request counters | `s3_operation_metrics` |
| Publication queue/wait counters | `performance_snapshot` |

## Semantics to remember

- `Versioned<T>::snapshot` is the Prolly commit used by the response.
- S3-shaped `version_id` fields contain logical `ObjectVersionId` values.
- Provider `VersionId` values remain an internal physical binding.
- A branch-ref CAS makes a prepared commit visible.
- Branch conflicts are returned; they are not retried automatically.
- Commit sessions are atomic, process-local, non-resumable, and bounded by
  `max_staged_batch_bytes`.
- AWS-shaped object-list pagination tokens are commit-pinned and
  cryptographically signed. Core history/diff cursor structs have private
  fields; services should authenticate them when crossing a trust boundary.
- Cache bytes and derived indexes are never write authority; every node cache
  hit is verified by CID.
- Catalog pages disclose freshness and a selected ref is resolved through its
  authoritative object before mutation.
- Whole-result compatibility APIs retain finite limits. Use bounded history,
  structural diff, catalog, and GC APIs for growing repositories.
