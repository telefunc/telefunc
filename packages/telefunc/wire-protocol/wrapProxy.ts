export { wrapProxy }

import { isObjectOrFunction } from '../utils/isObjectOrFunction.js'

/** Keeps the wrapper reachable as long as any object derived from it (e.g. a
 *  ReadableStreamReader obtained via `stream.getReader()`, a Promise chain, a
 *  Subscription) is still alive. Without this, method calls on the wrapper would
 *  return objects that reference `target` directly — the wrapper itself would
 *  become unreachable and GC would fire close() prematurely, closing the
 *  underlying resource while the user is still consuming it.
 *
 *  WeakMap semantics: as long as the derived object (key) is reachable, the
 *  wrapper (value) is held strongly, so FinalizationRegistry won't collect it. */
const keepWrapperAlive = new WeakMap<object, unknown>()

/** Wrap a value in a transparent proxy so it can be GC'd independently.
 *
 *  For objects: creates a Proxy that forwards all operations and tethers any
 *  object returned by a method call to the wrapper (see keepWrapperAlive).
 *  For functions: creates a wrapper function that forwards calls and copies properties. */
function wrapProxy<T extends object>(target: T): T {
  if (typeof target === 'function') {
    const wrapper = (...args: unknown[]) => {
      const result = (target as Function)(...args)
      tether(result, wrapper)
      return result
    }
    Object.assign(wrapper, target)
    return wrapper as unknown as T
  }

  const wrapper: T = new Proxy({} as T, {
    get(_proxy, prop) {
      const property = Reflect.get(target, prop, target)
      if (typeof property !== 'function') return property
      // Return a forwarding function that tethers any returned object to the wrapper.
      return (...args: unknown[]) => {
        const result = property.apply(target, args)
        tether(result, wrapper)
        return result
      }
    },
    set(_proxy, prop, value) {
      return Reflect.set(target, prop, value, target)
    },
    has(_proxy, prop) {
      return Reflect.has(target, prop)
    },
    ownKeys() {
      return Reflect.ownKeys(target)
    },
    getOwnPropertyDescriptor(_proxy, prop) {
      const descriptor = Reflect.getOwnPropertyDescriptor(target, prop)
      if (!descriptor) return descriptor
      return { ...descriptor, configurable: true }
    },
    getPrototypeOf() {
      return Reflect.getPrototypeOf(target)
    },
  })
  return wrapper
}

/** Pin `wrapper` to live as long as `derived` does (via WeakMap). */
function tether(derived: unknown, wrapper: unknown): void {
  if (isObjectOrFunction(derived)) keepWrapperAlive.set(derived, wrapper)
}
