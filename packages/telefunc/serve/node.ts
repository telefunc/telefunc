export { Telefunc }

import crossws from 'crossws/adapters/node'
import { serve as serveTelefunc, serveNode } from '../node/server/telefunc.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig, enableChannelTransports } from '../node/server/serverConfig.js'
import { getTelefuncChannelHooks } from '../wire-protocol/channel/server/ws.js'
import { CHANNEL_TRANSPORT } from '../wire-protocol/constants.js'
import { isTelefuncRequest, toResponse } from './shared.js'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import { getRequestPathname } from '../utils/getUrlPathname.js'
import type { IncomingMessage, ServerResponse, Server } from 'node:http'
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

const { registeredServers } = getGlobalObject('serve/node.ts', {
  registeredServers: new WeakSet<HttpServer>(),
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
  // Each upgrade's socket, by the request its upgrade hook gets.
  const sockets = new WeakMap<Request, Duplex>()
  const ws = crossws({
    hooks: getTelefuncChannelHooks({
      upgrade: (request) => ({ context: { socket: sockets.get(request) } }),
      send: sendInTaskWrite,
    }),
  })

  function installWebSocket(server: HttpServerOrWrapper): void {
    enableChannelTransports([CHANNEL_TRANSPORT.WS])
    const httpServer: HttpServer = (server as any)?.node?.server ?? (server as HttpServer)
    if (typeof (httpServer as any)?.on !== 'function') {
      throw new Error(
        'installWebSocket() received an unsupported server object. Pass a Node.js `http.Server` or a srvx-compatible wrapper.',
      )
    }
    if (registeredServers.has(httpServer)) return
    registeredServers.add(httpServer)

    httpServer.on('upgrade', (req, socket, head) => {
      const config = getServerConfig()
      if (getRequestPathname(req.url ?? '') !== config.telefuncUrl) return
      if (!config.channel.transports.includes(CHANNEL_TRANSPORT.WS)) {
        socket.once('finish', socket.destroy)
        socket.end('HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n')
        return
      }
      const request = new Request(`http://${req.headers.host ?? 'localhost'}${req.url}`, {
        headers: req.headers as HeadersInit,
      })
      sockets.set(request, socket)
      ws.handleUpgrade(req, socket, head, request)
    })
  }

  function inferServer(req: Req): HttpServer | undefined {
    return (req.socket as NodeRequest['socket'] & { server?: HttpServer }).server
  }

  function isNodeServeInput(input: ServeInput<Req, Res>): input is ServeInputNode<Req, Res> {
    return 'req' in input
  }

  async function serve(input: ServeInputNode<Req, Res>): Promise<boolean>
  async function serve(input: ServeInputRequest): Promise<Response | undefined>
  async function serve(input: ServeInput<Req, Res>): Promise<boolean | Response | undefined> {
    if (!isNodeServeInput(input)) {
      if (!isTelefuncRequest(input.request)) return undefined
      const httpResponse = await serveTelefunc(
        input.context ? { request: input.request, context: input.context } : { request: input.request },
      )
      return toResponse(httpResponse)
    }

    const { req, res, context } = input
    const server = inferServer(req)
    if (server) installWebSocket(server)

    if (res.headersSent) return false

    const url = req.originalUrl || req.url
    if (!url) return false
    if (getRequestPathname(url) !== getServerConfig().telefuncUrl) return false

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

/** Sends `frame` in its socket's one write for the task: written alone, each frame costs a syscall while the socket has room. */
function sendInTaskWrite(peer: Peer, frame: Uint8Array<ArrayBuffer>): void {
  const socket = peer.context.socket!
  if (!socket.writableCorked) {
    socket.cork()
    process.nextTick(() => socket.uncork())
  }
  peer.send(frame)
}
