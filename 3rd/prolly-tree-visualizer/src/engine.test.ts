import { readFileSync } from 'node:fs';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { ProllyEngine } from './engine';

const wasmBytes = Uint8Array.from(readFileSync(
  new URL('../../../bindings/wasm/pkg/prolly_wasm_bg.wasm', import.meta.url),
)).buffer;

describe('random mutations', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('reports the exact inserted key and value for the controls', async () => {
    const engine = await ProllyEngine.create(wasmBytes);
    try {
      const seeded = engine.seed();
      const maxKey = seeded.rows.at(-1)!.key;
      vi.spyOn(Math, 'random')
        .mockReturnValueOnce(0.75)
        .mockReturnValueOnce(0);

      const result = engine.addRandom();

      expect(result.key).toBe(maxKey + 1);
      expect(result.value).toBe(`random-${maxKey + 1}`);
      expect(result.snapshot.rows).toContainEqual({
        key: result.key,
        value: result.value,
      });
    } finally {
      engine.close();
    }
  });

  it('reports the exact updated key and value for the controls', async () => {
    const engine = await ProllyEngine.create(wasmBytes);
    try {
      const seeded = engine.seed();
      const firstKey = seeded.rows[0].key;
      vi.spyOn(Math, 'random')
        .mockReturnValueOnce(0.25)
        .mockReturnValueOnce(0);

      const result = engine.addRandom();

      expect(result.key).toBe(firstKey);
      expect(result.value).toBe(`random-update-${firstKey}-1`);
      expect(result.snapshot.rows).toContainEqual({
        key: result.key,
        value: result.value,
      });
    } finally {
      engine.close();
    }
  });
});
