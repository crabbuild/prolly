package mysql

const Schema = `
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid VARBINARY(32) PRIMARY KEY,
  node LONGBLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace VARBINARY(255) NOT NULL,
  ` + "`key`" + ` VARBINARY(255) NOT NULL,
  value LONGBLOB NOT NULL,
  PRIMARY KEY(namespace, ` + "`key`" + `)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name VARBINARY(255) PRIMARY KEY,
  manifest LONGBLOB NOT NULL
);`

const (
	selectNode = `SELECT node FROM prolly_nodes WHERE cid = ?`
	upsertNode = `INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)
ON DUPLICATE KEY UPDATE node = VALUES(node)`
	deleteNode          = `DELETE FROM prolly_nodes WHERE cid = ?`
	selectHint          = "SELECT value FROM prolly_hints WHERE namespace = ? AND `key` = ?"
	upsertHint          = "INSERT INTO prolly_hints (namespace, `key`, value) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)"
	selectRoot          = `SELECT manifest FROM prolly_roots WHERE name = ?`
	selectRootForUpdate = `SELECT manifest FROM prolly_roots WHERE name = ? FOR UPDATE`
	upsertRoot          = `INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)
ON DUPLICATE KEY UPDATE manifest = VALUES(manifest)`
	deleteRoot = `DELETE FROM prolly_roots WHERE name = ?`
)
