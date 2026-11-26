export { markRequestsOfServer, upgradesOfRequest }

import type { EventEmitter } from 'node:events'
import { IncomingMessage } from 'node:http'
import { Http2ServerRequest } from 'node:http2'
import type { HttpServer } from './nodeUpgrades.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { noUpgrades, upgradesOfServer, type TakesUpgrades } from '../node/server/runTelefunc/takesUpgrades.js'
import { isTelefuncUrl } from './shared.js'

/** The header in which a server names itself to `serve({ request })`, on each request to Telefunc's URL it gets. */
const SERVER_HEADER = 'x-telefunc-server'

const { serverOfToken, markingServers } = getGlobalObject('serve/requestServer.ts', {
  /** Each marking server, by its token, from its `listening` event until its `close` event. */
  serverOfToken: new Map<string, HttpServer>(),
  markingServers: new WeakSet<HttpServer>(),
})

/** Marks each request to Telefunc's URL the server gets with its token, once per server: every event, with its
 *  arguments and return value, passes through as it is. */
function markRequestsOfServer(server: HttpServer): void {
  if (markingServers.has(server)) return
  markingServers.add(server)
  const token = crypto.randomUUID()
  if (server.listening) serverOfToken.set(token, server)
  // The two server classes overload `emit` apart; as the `EventEmitter` each one is, it has one signature.
  const events: EventEmitter = server
  const emit = events.emit
  events.emit = function (this: EventEmitter, event: string | symbol, ...args: unknown[]): boolean {
    if (event === 'request') markRequest(args[0], token)
    else if (event === 'listening') serverOfToken.set(token, server)
    else if (event === 'close') serverOfToken.delete(token)
    return emit.call(this, event, ...args)
  }
}

/** After whatever the client sent under that name, as frameworks build a `Request` off `rawHeaders` or `headers`. */
function markRequest(req: unknown, token: string): void {
  if (!(req instanceof IncomingMessage || req instanceof Http2ServerRequest)) return
  if (req.url === undefined || !isTelefuncUrl(req.url)) return
  req.rawHeaders.push(SERVER_HEADER, token)
  req.headers[SERVER_HEADER] = token
}

/** The upgrades of the server whose token the request carries; a token proves nothing: a refused upgrade uses SSE. */
function upgradesOfRequest(request: Request): TakesUpgrades {
  const value = request.headers.get(SERVER_HEADER)
  if (value === null) return noUpgrades
  // `Headers` joins what the client sent under that name before the token, with ", ".
  const server = serverOfToken.get(value.slice(value.lastIndexOf(' ') + 1))
  return server ? upgradesOfServer(server) : noUpgrades
}
