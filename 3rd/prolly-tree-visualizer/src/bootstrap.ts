export function bootstrapKeys(count: number) {
  const amount = Math.max(1, Math.trunc(count));
  const keys: number[] = [];
  let state = 0x5eed1234;
  let key = 0;
  for (let index = 0; index < amount; index += 1) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    key += 1 + state % 11;
    keys.push(key);
  }
  return keys;
}

export function deterministicShuffle<T>(values: T[]) {
  const shuffled = [...values];
  let state = 0x0d1ce5ed;
  for (let index = shuffled.length - 1; index > 0; index -= 1) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    const swapIndex = state % (index + 1);
    [shuffled[index], shuffled[swapIndex]] = [shuffled[swapIndex], shuffled[index]];
  }
  return shuffled;
}
