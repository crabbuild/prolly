package postgres

const Schema = `
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid bytea PRIMARY KEY,
  node bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace bytea NOT NULL,
  key bytea NOT NULL,
  value bytea NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name bytea PRIMARY KEY,
  manifest bytea NOT NULL
);`

const (
	selectNode = `SELECT node FROM prolly_nodes WHERE cid = $1`
	upsertNode = `INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node`
	deleteNode = `DELETE FROM prolly_nodes WHERE cid = $1`
	selectHint = `SELECT value FROM prolly_hints WHERE namespace = $1 AND key = $2`
	upsertHint = `INSERT INTO prolly_hints (namespace, key, value) VALUES ($1, $2, $3)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value`
	selectRoot = `SELECT manifest FROM prolly_roots WHERE name = $1`
	upsertRoot = `INSERT INTO prolly_roots (name, manifest) VALUES ($1, $2)
ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest`
	deleteRoot = `DELETE FROM prolly_roots WHERE name = $1`
)
