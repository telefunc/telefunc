// Workaround: Node 22.21, 24.9 and later upgrade a request with `Upgrade` only where `shouldUpgradeCallback` says so,
// and the server's own default says so wherever it has an `upgrade` listener (`listenerCount('upgrade') > 0`). Without
// Telefunc's callback in front of it, Telefunc's listener makes the server upgrade every request with `Upgrade`, and an
// app's own route that answers them as plain HTTP breaks.
// TODO: remove once Node supports per-listener upgrade negotiation (https://github.com/nodejs/node/issues/6339; see https://github.com/nodejs/node/pull/59824).
export { upgradeOthersOnlyForTheirListeners }

import type { IncomingMessage, Server } from 'node:http'
import type { Http2SecureServer } from 'node:http2'
import { hasProp } from '../utils/hasProp.js'

/** Puts Telefunc's `shouldUpgradeCallback` in front of the server's own, where Node reads one: it upgrades Telefunc's
 *  handshake (`isTelefuncs`) and leaves every other request to the server's own, which sees a `listenerCount` without
 *  Telefunc's `listener`. */
function upgradeOthersOnlyForTheirListeners(
  server: Server | Http2SecureServer,
  listener: (...args: never[]) => void,
  isTelefuncs: (req: IncomingMessage) => boolean,
): void {
  if (!hasProp(server, 'shouldUpgradeCallback', 'function')) return
  const own = server.shouldUpgradeCallback
  server.shouldUpgradeCallback = (req: IncomingMessage) => {
    if (isTelefuncs(req)) return true
    const before = Object.getOwnPropertyDescriptor(server, 'listenerCount')
    const count = server.listenerCount
    const withoutTelefunc = (...args: [event: string | symbol, listener?: (...args: never[]) => void]) => {
      const [event, counted] = args
      if (event !== 'upgrade' || (counted !== undefined && counted !== listener)) return count.call(server, ...args)
      if (counted === listener) return 0
      return count.call(server, 'upgrade') - count.call(server, 'upgrade', listener)
    }
    Object.defineProperty(server, 'listenerCount', { value: withoutTelefunc, configurable: true, writable: true })
    try {
      return Boolean(own.call(server, req))
    } finally {
      if (before === undefined) Reflect.deleteProperty(server, 'listenerCount')
      else Object.defineProperty(server, 'listenerCount', before)
    }
  }
}
