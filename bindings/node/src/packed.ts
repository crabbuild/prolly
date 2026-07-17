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
  const mutators = new Set<PropertyKey>(["copyWithin", "fill", "reverse", "set", "sort"]);
  const iterators = new Set<PropertyKey>([Symbol.iterator, "entries", "keys", "values"]);
  return new Proxy(value, {
    get(target, property) {
      if (!scope.alive) throw new ViewExpiredError();
      if (property === "buffer") {
        throw new TypeError("the backing buffer of a scoped prolly view is not exposed; copy the view instead");
      }
      if (mutators.has(property)) {
        return () => { throw new TypeError("prolly scoped views are read-only"); };
      }
      if (property === "subarray") {
        return (begin?: number, end?: number) => scopedBytes(target.subarray(begin, end), scope);
      }
      if (iterators.has(property)) {
        return (...args: []) => {
          const iteratorFactory = Reflect.get(target, property, target) as (...values: []) => Iterator<unknown>;
          const iterator = iteratorFactory.apply(target, args);
          return {
            next(): IteratorResult<unknown> {
              if (!scope.alive) throw new ViewExpiredError();
              return iterator.next();
            },
            [Symbol.iterator]() { return this; },
          };
        };
      }
      const member = Reflect.get(target, property, target);
      return typeof member === "function"
        ? (...args: unknown[]) => {
          if (!scope.alive) throw new ViewExpiredError();
          return member.apply(target, args);
        }
        : member;
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
