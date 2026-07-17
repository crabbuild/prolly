# Cloud Spanner store for Go

This module implements the shared asynchronous store protocol with the official
Google Cloud Spanner Go client. Its `ProllyNodes`, `ProllyHints`, and
`ProllyRoots` GoogleSQL tables exactly match the Rust schema.

Mutation batches, nodes-plus-hint writes, root compare-and-swap, and strict
node-plus-root commits use Spanner atomic commits or read/write transactions.
`DDLStatements` and `ApplyDDL` initialize an existing database; applications
remain responsible for projects, instances, databases, credentials, and
resource deletion.
