import assert from "node:assert/strict";

import { loadNative, type NativeDiffRecord, type NativeTreeRecord } from "../src/native.ts";

const native = await loadNative();
const bytes = (value: string): Buffer => Buffer.from(value);

type User = [tenant: string, userId: string, status: string, displayName: string];

function userKey(tenant: string, userId: string): Buffer {
  return bytes(`source/tenant/${tenant}/user/${userId}`);
}

function encodeUser(...user: User): Buffer {
  return bytes(user.join("|"));
}

function decodeUser(value: Uint8Array): User {
  return Buffer.from(value).toString().split("|", 4) as User;
}

function statusIndexPrefix(tenant: string, status: string): Buffer {
  return bytes(`index/user-by-status/tenant/${tenant}/status/${status}/`);
}

function statusIndexKey(user: User): Buffer {
  const [tenant, userId, status] = user;
  return Buffer.concat([statusIndexPrefix(tenant, status), bytes(userId)]);
}

function putUser(tree: NativeTreeRecord, user: User): NativeTreeRecord {
  return engine.put(tree, userKey(user[0], user[1]), encodeUser(...user));
}

function buildStatusIndex(source: NativeTreeRecord): NativeTreeRecord {
  let index = engine.create();
  for (const entry of engine.range(source, bytes("source/"), bytes("source0"))) {
    index = engine.put(index, statusIndexKey(decodeUser(entry.value)), bytes("1"));
  }
  return index;
}

function applySourceDiff(index: NativeTreeRecord, changes: NativeDiffRecord[]): NativeTreeRecord {
  for (const change of changes) {
    if (change.kind === "added" && change.value) {
      index = engine.put(index, statusIndexKey(decodeUser(change.value)), bytes("1"));
    } else if (change.kind === "removed" && change.value) {
      index = engine.delete(index, statusIndexKey(decodeUser(change.value)));
    } else if (change.kind === "changed" && change.old && change.newValue) {
      const oldKey = statusIndexKey(decodeUser(change.old));
      const newKey = statusIndexKey(decodeUser(change.newValue));
      if (Buffer.compare(oldKey, newKey) !== 0) {
        index = engine.delete(index, oldKey);
        index = engine.put(index, newKey, bytes("1"));
      }
    }
  }
  return index;
}

function usersByStatus(index: NativeTreeRecord, tenant: string, status: string) {
  const start = statusIndexPrefix(tenant, status);
  return engine.range(index, start, native.prefixEnd(start));
}

const engine = native.NativeProllyEngine.memory();
const empty = engine.create();

let sourceV1 = putUser(empty, ["acme", "u001", "active", "Ada"]);
sourceV1 = putUser(sourceV1, ["acme", "u002", "invited", "Grace"]);
const indexV1 = buildStatusIndex(sourceV1);

let sourceV2 = putUser(sourceV1, ["acme", "u002", "active", "Grace"]);
sourceV2 = putUser(sourceV2, ["globex", "u003", "active", "Linus"]);

const sourceChanges = engine.diff(sourceV1, sourceV2);
assert.equal(sourceChanges.length, 2);

const indexV2 = applySourceDiff(indexV1, sourceChanges);
const rebuiltIndexV2 = buildStatusIndex(sourceV2);
assert.deepEqual(indexV2.root, rebuiltIndexV2.root);

assert.equal(usersByStatus(indexV2, "acme", "active").length, 2);
assert.equal(usersByStatus(indexV2, "acme", "invited").length, 0);
assert.equal(usersByStatus(indexV2, "globex", "active").length, 1);

console.log(`secondary_index: applied ${sourceChanges.length} source diffs`);
