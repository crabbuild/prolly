package spanner

import (
	"context"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

const SchemaVersion uint32 = 1

var DDLStatements = []string{
	`CREATE TABLE ProllyNodes (
  Cid BYTES(32) NOT NULL,
  Node BYTES(MAX) NOT NULL
) PRIMARY KEY (Cid)`,
	`CREATE TABLE ProllyHints (
  Namespace BYTES(MAX) NOT NULL,
  HintKey BYTES(MAX) NOT NULL,
  Value BYTES(MAX) NOT NULL
) PRIMARY KEY (Namespace, HintKey)`,
	`CREATE TABLE ProllyRoots (
  Name BYTES(MAX) NOT NULL,
  Manifest BYTES(MAX) NOT NULL
) PRIMARY KEY (Name)`,
}

// ApplyDDL applies the exact Rust-compatible schema to an existing database.
// The caller owns project, instance, database, and credential lifecycle.
func ApplyDDL(ctx context.Context, admin *database.DatabaseAdminClient, databaseName string) error {
	operation, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: databaseName, Statements: append([]string(nil), DDLStatements...)})
	if err != nil {
		return spannerError("apply_ddl", err)
	}
	return spannerError("apply_ddl", operation.Wait(ctx))
}
