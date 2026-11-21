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

const { upgradeServers } = getGlobalObject('node/server/runTelefunc/takesUpgrades.ts', {
  /** The Node servers that have Telefunc's `upgrade` listener (`installWebSocket()`). */
  upgradeServers: new WeakSet<object>(),
})

function upgradesOfServer(server: object): TakesUpgrades {
  return () => upgradeServers.has(server)
}

/** Node, through the request's socket: the server it arrived on. */
function upgradesOfSocketServer(readable: Readable): TakesUpgrades {
  const socket = hasProp(readable, 'socket', 'object') ? readable.socket : null
  return hasProp(socket, 'server', 'object') ? upgradesOfServer(socket.server) : noUpgrades
}
