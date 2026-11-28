export { Telefunc }

import crossws from 'crossws/adapters/node'
import { serve as serveTelefunc, serveNode } from '../node/server/telefunc.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig, getTelefuncUrl } from '../node/server/serverConfig.js'
import { getTelefuncChannelOptions } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { getRequestPathname, readRequestUrl } from '../utils/getUrlPathname.js'
import { normalizeHeaders } from '../utils/nodeReadableToWebRequest.js'
import { assert } from '../utils/assert.js'
import { upgradeOthersOnlyForTheirListeners } from '../workarounds/nodeShouldUpgradeCallback.js'
import { isTelefuncRequest, toResponse } from './shared.js'
import type { IncomingMessage, Server, ServerResponse } from 'node:http'
import type { Http2SecureServer } from 'node:http2'
import type { Duplex } from 'node:stream'
import type { Peer } from 'crossws'

declare module 'crossws' {
  interface PeerContext {
    socket?: Duplex
  }
}

type HttpServer = Server | Http2SecureServer

type HttpServerOrWrapper = HttpServer | { node?: { server?: HttpServer } }
type NodeRequest = IncomingMessage & { originalUrl?: string }

type ServeInputNode<Req extends NodeRequest, Res extends ServerResponse> = {
  req: Req
  res: Res
  context?: TelefuncNamespace.Context
}

type ServeInputRequest = {
  request: Request
  context?: TelefuncNamespace.Context
}

type ServeInput<Req extends NodeRequest, Res extends ServerResponse> = ServeInputNode<Req, Res> | ServeInputRequest

interface TelefuncServe<Req extends NodeRequest, Res extends ServerResponse> {
  installWebSocket(server: HttpServerOrWrapper): void
  serve(input: ServeInputNode<Req, Res>): Promise<boolean>
  serve(input: ServeInputRequest): Promise<Response | undefined>
}

const globalObject = getGlobalObject('serve/node.ts', {
  registeredServers: new WeakSet<HttpServer>(),
  sockets: new WeakMap<Request, Duplex>(),
  ws: undefined as ReturnType<typeof crossws> | undefined,
})

interface Telefunc<Req extends NodeRequest = NodeRequest, Res extends ServerResponse = ServerResponse>
  extends TelefuncServe<Req, Res> {}
class Telefunc<Req extends NodeRequest = NodeRequest, Res extends ServerResponse = ServerResponse> {
  constructor() {
    return telefunc<Req, Res>()
  }
}

function telefunc<Req extends NodeRequest = NodeRequest, Res extends ServerResponse = ServerResponse>(): TelefuncServe<
  Req,
  Res
> {
  function isNodeServeInput(input: ServeInput<Req, Res>): input is ServeInputNode<Req, Res> {
    return 'req' in input
  }

  async function serve(input: ServeInputNode<Req, Res>): Promise<boolean>
  async function serve(input: ServeInputRequest): Promise<Response | undefined>
  async function serve(input: ServeInput<Req, Res>): Promise<boolean | Response | undefined> {
    if (!isNodeServeInput(input)) {
      if (!isTelefuncRequest(input.request)) return undefined
      const { request, context } = input
      const httpResponse = await serveTelefunc(context ? { request, context } : { request })
      return toResponse(httpResponse)
    }

    const { req, res, context } = input
    const server = (req.socket as NodeRequest['socket'] & { server?: HttpServer }).server
    if (server) installWebSocket(server)

    if (res.headersSent) return false

    const url = req.originalUrl || req.url
    if (!url) return false
    if (getRequestPathname(url) !== getTelefuncUrl()) return false

    const serveInput = {
      url,
      method: req.method || 'GET',
      readable: req,
      headers: req.headers,
    }
    const httpResponse = await serveNode(context ? { ...serveInput, context } : serveInput, res)

    httpResponse.headers.forEach(([name, value]) => res.setHeader(name, value))
    res.statusCode = httpResponse.statusCode
    res.socket?.setNoDelay(true)
    res.flushHeaders()
    await httpResponse.pipe(res)
    return true
  }

  return { installWebSocket, serve }
}

function installWebSocket(server: HttpServerOrWrapper): void {
  const httpServer = (server as { node?: { server?: HttpServer } })?.node?.server ?? (server as HttpServer)
  if (typeof httpServer?.on !== 'function') {
    throw new Error(
      'installWebSocket() received an unsupported server object. Pass a Node.js `http.Server` or a srvx-compatible wrapper.',
    )
  }
  if (globalObject.registeredServers.has(httpServer)) return
  const ws = (globalObject.ws ??= crossws(
    getTelefuncChannelOptions({
      upgrade: (request) => ({ context: { socket: globalObject.sockets.get(request) } }),
      send: sendInTaskWrite,
    }),
  ))
  const onUpgrade = (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    const url = readRequestUrl(req.url ?? '')
    if (url?.pathname !== getTelefuncUrl()) {
      // Older Node versions emit 'upgrade' for every Upgrade request once a listener is installed.
      if (httpServer.listenerCount('upgrade') === 1) {
        socket.once('finish', socket.destroy)
        socket.end('HTTP/1.1 404 Not Found\r\nConnection: close\r\nContent-Length: 0\r\n\r\n')
      }
      return
    }
    if (!getServerConfig().channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
      socket.once('finish', socket.destroy)
      socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n')
      return
    }
    const request = new Request(url, { headers: normalizeHeaders(req.headers) })
    globalObject.sockets.set(request, socket)
    void ws.handleUpgrade(req, socket, head, request)
  }
  httpServer.on('upgrade', onUpgrade)
  upgradeOthersOnlyForTheirListeners(
    httpServer,
    onUpgrade,
    (req) => getRequestPathname(req.url ?? '') === getTelefuncUrl(),
  )
  globalObject.registeredServers.add(httpServer)
}

/** Sends `frame` in one write with the task's other frames, up to the socket's mark: written alone, each frame costs a
 *  syscall while the socket has room. Past the mark it goes at once, or the page would wait for the whole task. */
function sendInTaskWrite(peer: Peer, frame: Uint8Array<ArrayBuffer>): void {
  const { socket } = peer.context
  assert(socket)
  if (!socket.writableCorked) {
    socket.cork()
    process.nextTick(() => socket.uncork())
  }
  peer.send(frame)
  if (socket.writableLength >= socket.writableHighWaterMark) socket.uncork()
}
