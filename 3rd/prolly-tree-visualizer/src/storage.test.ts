import { describe, expect, it } from 'vitest';
import { calculateVersionStorage, countHistoricalTreeChunks } from './storage';

describe('calculateVersionStorage', () => {
  it('counts shared addresses once across versions', () => {
    const metrics = calculateVersionStorage([
      { nodes: new Map([['root-a', null], ['leaf-a', null], ['leaf-shared', null]]) },
      { nodes: new Map([['root-b', null], ['leaf-b', null], ['leaf-shared', null]]) },
      { nodes: new Map([['root-c', null], ['leaf-b', null], ['leaf-shared', null]]) },
    ]);

    expect(metrics).toEqual({
      versions: 3,
      withoutSharing: 9,
      withSharing: 6,
      savedChunks: 3,
      savedFraction: 1 / 3,
    });
  });

  it('handles an empty history', () => {
    expect(calculateVersionStorage([])).toEqual({
      versions: 0,
      withoutSharing: 0,
      withSharing: 0,
      savedChunks: 0,
      savedFraction: 0,
    });
  });

  it('counts every older root, internal, and leaf address not shared with HEAD', () => {
    const versions = [
      { nodes: new Map([['root-a', null], ['internal-a', null], ['leaf-shared', null]]) },
      { nodes: new Map([['root-b', null], ['internal-a', null], ['leaf-old', null], ['leaf-shared', null]]) },
      { nodes: new Map([['root-c', null], ['internal-c', null], ['leaf-shared', null]]) },
    ];

    expect(countHistoricalTreeChunks(versions)).toBe(4);
  });
});
