# AWS policy templates

Replace `REGION`, `ACCOUNT_ID`, `NODE_TABLE`, and `ROOT_TABLE` before deployment.
These are identity-policy examples for the exact physical APIs used by the
current provider; they are not CloudFormation templates and do not create
tables, KMS keys, backups, trails, endpoints, alarms, or roles.

Validate the rendered policies with AWS IAM Access Analyzer and exercise both
allow and deny paths in the target account. Attach the runtime policy only to a
trusted server-side process. The provisioner policy intentionally excludes
`DeleteTable`; production lifecycle administration should remain separately
approved.

Read [`../../SECURITY.md`](../../SECURITY.md) before using either policy.
