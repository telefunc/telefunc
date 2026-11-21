export { Telefunc }

import { serveNode, serveRuntimeRequest } from '../node/server/telefunc.js'
import { noTransportEvidence } from '../node/server/runTelefunc/clientLeft.js'
import { noUpgrades, upgradesOfServer } from '../node/server/runTelefunc/takesUpgrades.js'
import type { Telefunc as TelefuncNamespace } from '../node/server/context/getContext.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { takeUpgrades, takeUpgradesOfNewServer, type HttpServer } from './nodeUpgrades.js'
import { markRequestsOfServer, serverOfRequest } from './requestServer.js'
import { getTelefuncRequestUrl, toResponse } from './shared.js'
import { getRequestPathname } from '../utils/getUrlPathname.js'
import type { IncomingMessage, ServerResponse } from 'node:http'

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
  function installWebSocket(server: HttpServerOrWrapper): void {
    const httpServer: HttpServer = (server as any)?.node?.server ?? (server as HttpServer)
    if (typeof (httpServer as any)?.on !== 'function') {
      throw new Error(
        'installWebSocket() received an unsupported server object. Pass a Node.js `http.Server` or a srvx-compatible wrapper.',
      )
    }
    takeUpgrades(httpServer)
    // The app's own call, which an app on `serve({ request })` makes to get WebSocket: that form reads the server off
    // the header this marks. The others read it off the request's socket, so `serve({ req, res })` marks none.
    markRequestsOfServer(httpServer)
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
      if (getTelefuncRequestUrl(input.request) === null) return undefined
      const server = serverOfRequest(input.request)
      const httpResponse = await serveRuntimeRequest(
        input.context ? { request: input.request, context: input.context } : { request: input.request },
        noTransportEvidence,
        server ? upgradesOfServer(server) : noUpgrades,
      )
      return toResponse(httpResponse)
    }

    const { req, res, context } = input
    const server = inferServer(req)
    if (server) takeUpgradesOfNewServer(server)

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
