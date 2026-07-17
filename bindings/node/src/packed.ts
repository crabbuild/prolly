export class ViewExpiredError extends Error {
  constructor() {
    super("scoped prolly view has expired");
    this.name = "ViewExpiredError";
  }
}

export interface ViewScope {
  alive: boolean;
}

// Native Buffers own their Rust allocation. The proxy adds the scoped borrow
// contract without copying that allocation into a second Uint8Array.
export function scopedBytes(value: Uint8Array, scope: ViewScope): Uint8Array {
  return new Proxy(value, {
    get(target, property) {
      if (!scope.alive) throw new ViewExpiredError();
      const member = Reflect.get(target, property, target);
      return typeof member === "function" ? member.bind(target) : member;
    },
    set() {
      throw new TypeError("prolly scoped views are read-only");
    },
  });
}

export function ownedBytes(value: Uint8Array): Buffer {
  return Buffer.from(value);
}

export function abortError(): Error {
  const error = new Error("prolly operation aborted");
  error.name = "AbortError";
  return error;
}

export async function nativePromise<T>(
  signal: AbortSignal | undefined,
  operation: () => T,
): Promise<T> {
  if (signal?.aborted) throw abortError();
  await Promise.resolve();
  if (signal?.aborted) throw abortError();
  const result = operation();
  if (signal?.aborted) throw abortError();
  return result;
}
