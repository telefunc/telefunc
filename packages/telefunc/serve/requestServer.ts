export { markRequestsOfServer, serverOfRequest }

import { IncomingMessage } from 'node:http'
import { Http2ServerRequest } from 'node:http2'
import type { HttpServer } from './nodeUpgrades.js'
import { getTelefuncUrl } from '../node/server/serverConfig.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { getRequestPathname } from '../utils/getUrlPathname.js'

/** The header in which a server names itself to `serve({ request })`, on each request to Telefunc's URL it gets. */
const SERVER_HEADER = 'x-telefunc-server'

const { serverOfToken, markingServers } = getGlobalObject('serve/requestServer.ts', {
  /** Each marking server, by its token, while it listens. */
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
  const emit = server.emit
  Reflect.set(server, 'emit', function (this: unknown, ...args: unknown[]): unknown {
    if (args[0] === 'request') markRequest(args[1], token)
    else if (args[0] === 'listening') serverOfToken.set(token, server)
    else if (args[0] === 'close') serverOfToken.delete(token)
    return Reflect.apply(emit, this, args)
  })
}

/** After whatever the client sent under that name, as frameworks build a `Request` off `rawHeaders` or `headers`. */
function markRequest(req: unknown, token: string): void {
  if (!(req instanceof IncomingMessage || req instanceof Http2ServerRequest)) return
  if (req.url === undefined || !isTelefuncUrl(req.url)) return
  req.rawHeaders.push(SERVER_HEADER, token)
  req.headers[SERVER_HEADER] = token
}

/** A Node request's URL names Telefunc's: its path, or the path of the absolute URL a proxy request names. */
function isTelefuncUrl(url: string): boolean {
  const telefuncUrl = getTelefuncUrl()
  if (!url.startsWith(telefuncUrl)) return !url.startsWith('/') && getRequestPathname(url) === telefuncUrl
  const next = url[telefuncUrl.length]
  return next === undefined || next === '?' || next === '#'
}

/** The server the request came in on, where it marks its requests: a token no listening server has names none. */
function serverOfRequest(request: Request): HttpServer | undefined {
  const value = request.headers.get(SERVER_HEADER)
  if (value === null) return undefined
  // `Headers` joins what the client sent under that name before the token, with ", ".
  return serverOfToken.get(value.slice(value.lastIndexOf(' ') + 1))
}
