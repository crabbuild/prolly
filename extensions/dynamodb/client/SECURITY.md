# Versioned DynamoDB Client Security Model

This document defines the security boundary for the in-process Rust client. It
is a deployment contract, not a claim that a library can authorize its caller.

## Non-negotiable boundary

The client, Prolly core, node CIDs, version history, leases, and audit records
protect correctness against bugs, races, retries, crashes, and accidental
corruption. They are not a boundary against a principal that can call DynamoDB
directly with the same physical-table credentials.

A compromised runtime principal can read confidential physical records, delete
reachable content, replace roots, manufacture internally consistent history,
or bypass application authorization. Content hashes can detect random or
partial corruption; they cannot make a malicious writer trustworthy.

Use an application-owned authenticated API when callers, tenants, or devices
are not trusted with the entire physical database. Do not distribute physical
DynamoDB credentials to browsers, mobile applications, customer processes, or
untrusted plugin code.

## Trust-domain isolation

Use a dedicated node table and roots table for every mutually distrustful
security/retention domain. A non-empty `key_prefix` prevents accidental
namespace collision inside a trusted domain, but it is not an IAM boundary:

- the provider encodes physical partition keys as DynamoDB Binary values;
- DynamoDB's `dynamodb:LeadingKeys` condition is defined over partition-key
  values, but AWS explicitly excludes `Scan` from leading-key isolation because
  Scan can return all items;
- the client and maintenance workflows require physical Scan access.

Consequently, do not use `key_prefix` alone for hostile multi-tenancy. This is
an implementation-and-IAM inference, not an AWS guarantee. See AWS's
[fine-grained access-control documentation](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/specifying-conditions.html)
and [DynamoDB service authorization reference](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazondynamodb.html).

## Least-privilege identities

The checked-in examples under `deploy/aws` use exact table ARNs and only the
physical APIs currently invoked by `prolly-store-dynamodb`.

| Identity | Allowed | Explicitly outside its role |
| --- | --- | --- |
| Runtime client | BatchGetItem, BatchWriteItem, DeleteItem, GetItem, PutItem, Query, Scan, TransactWriteItems on the two exact tables | Create/Update/DeleteTable, backup/KMS/IAM administration |
| Provisioner | CreateTable and DescribeTable for the two exact tables | Runtime item access and DeleteTable |
| Maintenance process | A separately deployed copy of the runtime data-plane policy, with application change-control authority | Table deletion, KMS changes, lease break without operator approval |
| Recovery operator | Separately approved backup/restore and isolated-target provisioning | Routine request-process credentials |

The runtime and maintenance policies have similar provider actions because
ordinary logical writes and physical GC both ultimately use item writes and
transactions. Process identity, deployment separation, the durable maintenance
fence, and change approval provide the semantic separation; IAM cannot inspect
whether a physical delete came from a reviewed GC plan.

Test policies in the target account with IAM Access Analyzer and a deny-path
integration test. Do not grant `dynamodb:*`, wildcard table resources, or
`dynamodb:DeleteTable` to application roles. Apply an organization SCP or
permissions boundary when preventing accidental privilege expansion is a
compliance requirement.

## Data protection

1. Require the normal AWS SDK HTTPS endpoint. Do not configure plaintext custom
   endpoints outside isolated local tests.
2. Use DynamoDB encryption at rest. For regulated deployments, select and
   govern an approved customer-managed KMS key; disabling or deleting that key
   makes the table unavailable and can lead to archival. AWS documents the
   behavior in [DynamoDB encryption at rest](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/encryption.howitworks.html).
3. Use application-level envelope encryption before the client when threat
   models require payload secrecy from DynamoDB operators, backups, or raw-table
   readers. Encrypt searchable/indexed attributes only with a reviewed scheme;
   ordinary randomized encryption changes equality and ordering semantics.
4. Keep credentials in the caller-owned AWS provider chain. The client never
   exports credentials, but process memory, crash dumps, environment variables,
   and debug tooling remain part of the deployment boundary.
5. Prefer a DynamoDB gateway endpoint for in-VPC traffic or an interface endpoint
   when private IP/on-premises connectivity is required. AWS documents both in
   [AWS PrivateLink for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/privatelink-interface-endpoints.html).

## Audit and evidentiary limits

Enable CloudTrail DynamoDB **data events** for both physical tables. Management
events alone do not record item-level calls by default. AWS lists BatchGetItem,
BatchWriteItem, DeleteItem, GetItem, PutItem, Query, Scan,
TransactGetItems/WriteItems, and UpdateItem as DynamoDB data events in
[CloudTrail logging for DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/logging-using-cloudtrail.html).

Protect CloudTrail destinations with an independently administered account,
retention controls, alerting, and access logging. Treat event contents and table
ARNs as sensitive operational data.

Client commits and operator audits are useful application evidence, but they
are mutable by a principal with raw write authority. Version history is not a
WORM store, a qualified electronic signature, a trusted timestamp, litigation
hold, or jurisdiction-specific records-management system. Financial or legal
deployments needing those properties must export signed canonical evidence to
an independently controlled immutable archive and validate that workflow with
their compliance and legal owners.

## Backup, deletion, and recovery controls

- Enable DynamoDB deletion protection on both physical tables; it is off by
  default. AWS documents that protected tables cannot be deleted until the
  setting is disabled in [DynamoDB table operations](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.Basics.html).
- Enable PITR on both physical tables with the approved 1–35-day window. PITR
  restores to a new table. Because the node and roots tables are restored as
  separate resources, PITR alone does not prove an application-consistent
  two-table snapshot; restore into an isolated namespace and run the client's
  reachability, commit, and archive checks before cutover. See
  [DynamoDB PITR behavior](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery_Howitworks.html).
- Retain independently verified client archives for application-consistent
  recovery points. Test restore, not just backup creation.
- Table deletion, PITR disablement, KMS disable/delete, IAM policy changes,
  expired-lease break, restore, retention apply, and GC apply require attributed
  change approval and alerting.

## Logging and telemetry

Normal client spans omit keys, items, expressions, credentials, table names,
and raw result bodies. That is not a guarantee about the caller's AWS SDK
interceptors, HTTP tracing, panic output, or application logs. Production log
review must verify redaction end to end.

Never log request tokens together with sensitive business identifiers unless
the log is authorized for the same data classification. Tokens provide replay
identity, not authorization, and commit/version IDs are not secrets.

## Deployment gate

Before production, record evidence for every item:

- two dedicated physical tables per trust domain, non-empty prefix, and exact
  namespace inventory;
- exact runtime/provisioner roles with no wildcard data tables or DeleteTable;
- denial tests for a third table, table deletion, and unapproved control-plane
  APIs;
- deletion protection, PITR, approved KMS key, TLS endpoint, and network route;
- CloudTrail data events for both table ARNs and alerts for privileged changes;
- credential rotation, break-glass ownership, incident contacts, and revocation
  drill;
- isolated restore plus logical verification drill;
- application log/redaction test using production tracing configuration;
- documented decision whether application-level encryption and immutable
  external evidence are required.

Failure or missing evidence on any applicable item is a deployment blocker.
