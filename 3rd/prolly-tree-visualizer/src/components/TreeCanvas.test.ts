import { describe, expect, it } from 'vitest';
import { missingKeyLabel, rangeKeysForNode, routeKeyForLookup, shownKeys } from './TreeCanvas';
import type { ProllyNode } from '../types';

function leaf(keys: number[]): ProllyNode {
  return {
    hash: '1'.repeat(40),
    level: 0,
    size: 100,
    flags: 1,
    entries: keys.map((key) => ({ key, keyHex: '', valueHex: '' })),
    children: [],
    minKey: keys[0],
    maxKey: keys.at(-1)!,
  };
}

describe('tree key labels', () => {
  it('keeps a looked-up key visible inside a compressed leaf', () => {
    expect(shownKeys(leaf([1, 2, 3, 4, 5, 6, 7, 8, 9]), [6])).toEqual([
      { text: '1', match: false },
      { text: '…', match: false },
      { text: '6', match: true },
      { text: '…', match: false },
      { text: '9', match: false },
    ]);
  });

  it('compresses wide keys even when the leaf has six entries', () => {
    expect(shownKeys(leaf([12347, 12348, 12349, 12350, 12351, 12352]))).toEqual([
      { text: '12347', match: false },
      { text: '12348', match: false },
      { text: '…', match: false },
      { text: '12351', match: false },
      { text: '12352', match: false },
    ]);
  });

  it('places a missing key between its surrounding keys', () => {
    expect(missingKeyLabel(leaf([1, 4, 8, 12]), 6)).toEqual([
      { text: '4', match: false },
      { text: 'key 6 not found', match: false, missing: true },
      { text: '8', match: false },
    ]);
  });

  it('selects the last lower-bound delimiter at or below the lookup key', () => {
    expect(routeKeyForLookup(leaf([10, 20, 30]), 17)).toBe(10);
    expect(routeKeyForLookup(leaf([10, 20, 30]), 40)).toBe(30);
  });

  it('selects the first and last returned keys in a leaf range', () => {
    expect(rangeKeysForNode(leaf([1, 4, 8, 12, 16]), 3, 13)).toEqual([4, 12]);
  });
});
