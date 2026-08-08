import { describe, expect, it } from 'vitest';
import { bootstrapKeys, deterministicShuffle } from './bootstrap';

describe('bootstrap keys', () => {
  it('builds a reproducible, ordered, sparse key set', () => {
    const first = bootstrapKeys(240);
    const second = bootstrapKeys(240);
    const gaps = first.slice(1).map((key, index) => key - first[index]);

    expect(first).toEqual(second);
    expect(new Set(first).size).toBe(240);
    expect(gaps.every((gap) => gap > 0)).toBe(true);
    expect(new Set(gaps).size).toBeGreaterThan(5);
    expect(first.at(-1)).toBeGreaterThan(1_000);
  });

  it('reorders values reproducibly without changing the set', () => {
    const values = bootstrapKeys(40);
    const shuffled = deterministicShuffle(values);

    expect(shuffled).toEqual(deterministicShuffle(values));
    expect(shuffled).not.toEqual(values);
    expect([...shuffled].sort((left, right) => left - right)).toEqual(values);
  });
});
