// Workaround: Node 22.21, 24.9 and later upgrade a request with `Upgrade` only where `shouldUpgradeCallback` says so,
// and the server's own default says so wherever it has an `upgrade` listener (`listenerCount('upgrade') > 0`). Without
// Telefunc's callback in front of it, Telefunc's listener makes the server upgrade every request with `Upgrade`, and an
// app's own route that answers them as plain HTTP breaks (serve/node.upgradeOthers.spec.ts). Remove once Node asks each
// `upgrade` listener, or Telefunc no longer listens on the app's server.
export { upgradeOthersOnlyForTheirListeners, putBackOwnCallback }

import type { IncomingMessage } from 'node:http'
import type { TakenServer } from '../node/server/runTelefunc/takesUpgrades.js'
import type { HttpServer } from '../serve/nodeUpgrades.js'
import { hasProp } from '../utils/hasProp.js'

/** Puts Telefunc's `shouldUpgradeCallback` in front of the server's own, where Node reads one: it upgrades Telefunc's
 *  handshake (`isTelefuncs`) and leaves every other request to the server's own, which sees a `listenerCount` without
 *  Telefunc's listener. */
function upgradeOthersOnlyForTheirListeners(
  server: HttpServer,
  taken: TakenServer,
  isTelefuncs: (req: IncomingMessage) => boolean,
): void {
  if (!hasProp(server, 'shouldUpgradeCallback', 'function')) return
  const own = server.shouldUpgradeCallback
  const callback = (req: IncomingMessage) => {
    if (isTelefuncs(req)) return true
    const before = Object.getOwnPropertyDescriptor(server, 'listenerCount')
    const count = server.listenerCount
    const withoutTelefunc = (...args: [event: string | symbol, listener?: (...args: never[]) => void]) => {
      const [event, listener] = args
      if (event !== 'upgrade' || (listener !== undefined && listener !== taken.listener))
        return count.call(server, ...args)
      if (listener === taken.listener) return 0
      return count.call(server, 'upgrade') - count.call(server, 'upgrade', taken.listener)
    }
    Object.defineProperty(server, 'listenerCount', { value: withoutTelefunc, configurable: true, writable: true })
    try {
      return Boolean(own.call(server, req))
    } finally {
      if (before === undefined) Reflect.deleteProperty(server, 'listenerCount')
      else Object.defineProperty(server, 'listenerCount', before)
    }
  }
  server.shouldUpgradeCallback = callback
  taken.callback = callback
  taken.own = own
}

/** Puts the server's own callback back where Telefunc's is still the server's: one the app set since stays. */
function putBackOwnCallback(server: HttpServer, taken: TakenServer): void {
  if (taken.own === undefined || !hasProp(server, 'shouldUpgradeCallback', 'function')) return
  if (server.shouldUpgradeCallback === taken.callback) server.shouldUpgradeCallback = taken.own
}
