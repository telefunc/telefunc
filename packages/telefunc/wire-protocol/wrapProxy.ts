export { wrapProxy }

/** Wrap a channel in a transparent proxy so that the proxy is a separate object
 *  from the channel. This allows FinalizationRegistry to detect when the user
 *  drops the proxy (the public handle) while the channel (the internal transport
 *  peer) stays alive — held by the transport connection.
 *
 *  Methods are bound to the real channel so `this` is correct inside them. */
function wrapProxy<T extends object>(target: T): T {
  return new Proxy({} as T, {
    get(_proxy, prop) {
      const property = Reflect.get(target, prop, target)
      return typeof property === 'function' ? property.bind(target) : property
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
}
