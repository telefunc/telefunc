export { wrapProxy, untether, makeDisposer }

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
const untethered = new WeakSet<object>()

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

  const forwarders = new Map<PropertyKey, { property: Function; forward: (...args: unknown[]) => unknown }>()
  const wrapper: T = new Proxy({} as T, {
    get(_proxy, prop) {
      const property = Reflect.get(target, prop, target)
      if (typeof property !== 'function') return property
      // One forwarder per method, so identity holds like on the target; it tethers any returned object to the wrapper.
      const cached = forwarders.get(prop)
      if (cached?.property === property) return cached.forward
      const forward = (...args: unknown[]) => {
        const result = property.apply(target, args)
        tether(result, wrapper)
        return result
      }
      forwarders.set(prop, { property, forward })
      return forward
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
  if (!isObjectOrFunction(derived)) return
  if (untethered.has(derived)) return
  keepWrapperAlive.set(derived, wrapper)
  // A synchronous array return, such as `tee()`'s branches, hands out each element.
  if (Array.isArray(derived)) for (const value of derived) tether(value, wrapper)
}

/** `derived` never pins a wrapper: a terminal child no longer owns its parent's lifetime. */
function untether(derived: object): void {
  untethered.add(derived)
  keepWrapperAlive.delete(derived)
}

/** A one-shot cleanup handle; no action creates an already-terminal handle. */
function makeDisposer(dispose?: () => void, group?: Set<() => void>): () => void {
  let action = dispose
  const token = () => {
    const current = action
    action = undefined
    group?.delete(token)
    untether(token)
    current?.()
  }
  if (action) group?.add(token)
  else untether(token)
  return token
}
