export { noUpgrades, upgradesTakenHere, upgradeServers, upgradesOfServer, upgradesOfSocketServer }
export type { TakesUpgrades }

import type { Readable } from 'node:stream'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { hasProp } from '../../../utils/hasProp.js'

/** Whether the server a request arrived on takes WebSocket upgrades, asked each time the page that the request connected
 *  is offered its transports: a Node server may get its upgrade listener, or lose it, later. Only the adapter that was
 *  handed the request can say, as for `ClientLeft`. */
type TakesUpgrades = () => boolean

/** A `Request` handed over without its server, as Bun's and Deno's generic `serve({ request })` and the primitives form
 *  get it: its page is offered SSE, which works, and never an upgrade that fails. */
const noUpgrades: TakesUpgrades = () => false

/** An adapter that answers the upgrade itself, in the handler that serves the page's other requests: `telefunc/bun`,
 *  `telefunc/deno`, a Cloudflare Durable Object. */
const upgradesTakenHere: TakesUpgrades = () => true

/** A Node server Telefunc took, with the `upgrade` listener it put on it and, where Node reads one, the
 *  `shouldUpgradeCallback` it put in front of the server's own. */
type TakenServer = {
  server: { listeners(event: 'upgrade'): unknown[]; shouldUpgradeCallback?: unknown }
  listener: unknown
  callback: unknown
}

const { upgradeServers } = getGlobalObject('node/server/runTelefunc/takesUpgrades.ts', {
  /** The Node servers Telefunc put its `upgrade` listener on (serve/nodeUpgrades.ts). */
  upgradeServers: new WeakMap<object, TakenServer>(),
})

/** A server that has begun to close takes no new connection, so no upgrade, though it still serves those it has. */
function upgradesOfServer(server: object): TakesUpgrades {
  return () => upgradesReachTelefunc(server) && 'listening' in server && server.listening === true
}

/** Asked of the server itself, so that it holds however the app removed the listener or replaced the callback: an
 *  app's own callback in place of Telefunc's decides for Telefunc's handshake too. */
function upgradesReachTelefunc(server: object): boolean {
  const taken = upgradeServers.get(server)
  if (taken === undefined || !taken.server.listeners('upgrade').includes(taken.listener)) return false
  return taken.callback === undefined || taken.server.shouldUpgradeCallback === taken.callback
}

/** Node, through the request's socket: the server it arrived on. */
function upgradesOfSocketServer(readable: Readable): TakesUpgrades {
  const socket = hasProp(readable, 'socket', 'object') ? readable.socket : null
  return hasProp(socket, 'server', 'object') ? upgradesOfServer(socket.server) : noUpgrades
}
