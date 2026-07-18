package dynamodb

// SchemaVersion is the physical DynamoDB layout version shared with Rust.
const SchemaVersion uint32 = 1

// TableSchema documents the provider-native schema used by CreateTable.
const TableSchema = "single binary HASH partition key pk; binary value payload"
