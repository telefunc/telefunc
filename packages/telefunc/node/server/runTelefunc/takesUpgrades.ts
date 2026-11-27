export { noUpgrades, telefuncsUpgradesOn, upgradesTakenHere, upgradeServers, upgradesOfServer, upgradesOfSocketServer }
export type { TakenServer, TakesUpgrades }

import type { IncomingMessage } from 'node:http'
import type { Readable } from 'node:stream'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { hasProp } from '../../../utils/hasProp.js'
import type { TakesUpgrades } from '../../../wire-protocol/channel/server/ChannelMux.js'

/** A `Request` handed over without its server, as Bun's and Deno's generic `serve({ request })` and the primitives form
 *  get it: its page is offered SSE, which works, and never an upgrade that fails. */
const noUpgrades: TakesUpgrades = () => false

/** An adapter that answers the upgrade itself, in the handler that serves the page's other requests: `telefunc/bun`,
 *  `telefunc/deno`, a Cloudflare Durable Object. */
const upgradesTakenHere: TakesUpgrades = () => true

/** Telefunc's take of a Node server, kept for the server's life: the `upgrade` listener Telefunc put on it and, where
 *  Node reads one, the `shouldUpgradeCallback` it put in front of the server's own (`wrapped`), as the server can't tell
 *  which of its functions are Telefunc's. `holdsRegistry`: the server is in 0.3.1's registry for Telefunc's listener.
 *  A removal of the listener that Telefunc's `removeListener` listener didn't see leaves it there, so 0.3.1's next
 *  install skips the server, until this copy's next `installWebSocket()` takes it again. */
type TakenServer = {
  server: {
    listenerCount(event: 'upgrade', listener: (...args: never[]) => void): number
    shouldUpgradeCallback?: unknown
  }
  listener: (...args: never[]) => void
  wrapped: { callback: (req: IncomingMessage) => boolean; own: (req: IncomingMessage) => unknown } | null
  holdsRegistry: boolean
}

const { upgradeServers } = getGlobalObject('node/server/runTelefunc/takesUpgrades.ts', {
  /** Every Node server Telefunc took (serve/nodeUpgrades.ts), whether or not its listener is still there. */
  upgradeServers: new WeakMap<object, TakenServer>(),
})

/** A server that has begun to close takes no new connection, so no upgrade, though it still serves those it has. */
function upgradesOfServer(server: object): TakesUpgrades {
  return () =>
    telefuncsUpgradesOn(server) === 'listener and callback' && 'listening' in server && server.listening === true
}

/** What of Telefunc's take is on the server now, asked of the server itself, which the app may have changed in any way
 *  since: nothing (never taken, or Telefunc's listener is gone), Telefunc's listener with the app's own
 *  `shouldUpgradeCallback` in place of Telefunc's (which then decides for Telefunc's handshake too), or both. */
function telefuncsUpgradesOn(server: object): 'nothing' | 'listener' | 'listener and callback' {
  const taken = upgradeServers.get(server)
  if (taken === undefined || taken.server.listenerCount('upgrade', taken.listener) === 0) return 'nothing'
  if (taken.wrapped !== null && taken.server.shouldUpgradeCallback !== taken.wrapped.callback) return 'listener'
  return 'listener and callback'
}

/** Node, through the request's socket: the server it arrived on. */
function upgradesOfSocketServer(readable: Readable): TakesUpgrades {
  const socket = hasProp(readable, 'socket', 'object') ? readable.socket : null
  return hasProp(socket, 'server', 'object') ? upgradesOfServer(socket.server) : noUpgrades
}
