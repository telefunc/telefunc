export { markRequestsOfServer, serverOfRequest }

import type { EventEmitter } from 'node:events'
import { IncomingMessage } from 'node:http'
import { Http2ServerRequest } from 'node:http2'
import type { HttpServer } from './nodeUpgrades.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
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

/** The server the request came in on, where it marks its requests. A token names its server from the server's
 *  `listening` event until its `close` event, so also while it closes; what the page is offered is that server's answer
 *  at each offer, which has no WebSocket once the server stopped listening, lost Telefunc's listener or has another
 *  callback in place of Telefunc's. The token names a server and proves nothing, as the app's handler for Telefunc's
 *  URL sees it: a request that carries another taken server's token gets that server's answer, so where that server
 *  offers WebSocket, a page of a server that takes no upgrades is offered it too, its upgrade is refused, and its
 *  channel goes on over SSE. */
function serverOfRequest(request: Request): HttpServer | undefined {
  const value = request.headers.get(SERVER_HEADER)
  if (value === null) return undefined
  // `Headers` joins what the client sent under that name before the token, with ", ".
  return serverOfToken.get(value.slice(value.lastIndexOf(' ') + 1))
}
