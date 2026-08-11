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
| Branch/tag/history | `create_branch`, `create_tag`, `log_page` |
| Compare commits | `diff_page` |
| Merge/restore | `merge`, `restore` |
| Verify repository | `fsck` |

## Semantics to remember

- `Versioned<T>::snapshot` is the Prolly commit used by the response.
- S3-shaped `version_id` fields contain logical `ObjectVersionId` values.
- Provider `VersionId` values remain an internal physical binding.
- A branch-ref CAS makes a prepared commit visible.
- Branch conflicts are returned; they are not retried automatically.
- Commit sessions are atomic but process-local and non-resumable.
- Pagination tokens are commit-pinned and cryptographically signed.
