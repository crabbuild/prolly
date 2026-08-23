import { readFileSync } from 'node:fs';
import { loadProllyWasm } from 'prollydb-wasm';

const wasmBytes = readFileSync(new URL('../../../bindings/wasm/pkg/prolly_wasm_bg.wasm', import.meta.url));
const prolly = await loadProllyWasm(undefined, wasmBytes);
const engine = prolly.WasmProllyEngine.memory();
let tree = engine.create();
const encoder = new TextEncoder();
const rowCount = Number(process.env.SMOKE_ROWS ?? 240);

try {
  const entries = Array.from({ length: rowCount }, (_, index) => {
    const key = index + 1;
    return { key: prolly.i64Key(String(key)), value: encoder.encode(`value-${key}`) };
  });
  const built = engine.buildFromSortedEntries(entries);
  tree.free();
  tree = built;

  const root = Buffer.from(tree.root).toString('hex');
  const debug = engine.debugTree(tree);
  const bundle = engine.exportSnapshot(tree);
  const rows = engine.range(tree, new Uint8Array());
  try {
    if (!/^[0-9a-f]{64}$/.test(root)) throw new Error(`invalid root CID: ${root}`);
    if (rows.length !== rowCount) throw new Error(`expected ${rowCount} rows, got ${rows.length}`);
    if (debug.levels.length === 0) throw new Error('debug tree has no levels');
    if (bundle.nodeCount === 0 || bundle.byteCount === 0) throw new Error('snapshot has no nodes');
    console.log('engine=prolly-map');
    console.log(`root=${root}`);
    console.log(`rows=${rows.length}`);
    console.log(`nodes=${bundle.nodeCount}`);
    console.log(`node_bytes=${bundle.byteCount}`);
  } finally {
    bundle.free();
  }
} finally {
  tree.free();
  engine.free();
}
