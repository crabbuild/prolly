package sqlite

const createSchemaSQL = `
CREATE TABLE IF NOT EXISTS prolly_nodes (
    cid  BLOB PRIMARY KEY NOT NULL,
    node BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS prolly_hints (
    namespace BLOB NOT NULL,
    key       BLOB NOT NULL,
    value     BLOB NOT NULL,
    PRIMARY KEY (namespace, key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS prolly_roots (
    name     BLOB PRIMARY KEY NOT NULL,
    manifest BLOB NOT NULL
) WITHOUT ROWID;
`

const (
	selectNodeSQL = `SELECT node FROM prolly_nodes WHERE cid = ?`
	upsertNodeSQL = `INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node`
	deleteNodeSQL = `DELETE FROM prolly_nodes WHERE cid = ?`

	selectHintSQL = `SELECT value FROM prolly_hints WHERE namespace = ? AND key = ?`
	upsertHintSQL = `INSERT INTO prolly_hints (namespace, key, value) VALUES (?, ?, ?)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value`

	selectRootSQL = `SELECT manifest FROM prolly_roots WHERE name = ?`
	upsertRootSQL = `INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)
ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest`
	deleteRootSQL = `DELETE FROM prolly_roots WHERE name = ?`
)
