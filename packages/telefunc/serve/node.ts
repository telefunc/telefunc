export { telefunc }

import { serve as serveTelefunc } from '../node/server/telefunc.js'
import type { Telefunc } from '../node/server/getContext.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { telefuncWebSocket } from '../wire-protocol/server/adapter/node.js'
import { nodeReadableToWebRequest } from '../utils/nodeReadableToWebRequest.js'
import { isTelefuncRequest, toResponse } from './shared.js'
import type { IncomingMessage, ServerResponse } from 'node:http'
import type { Http2SecureServer } from 'node:http2'

type HttpServerOrWrapper = Parameters<ReturnType<typeof telefuncWebSocket>['install']>[0]
type HttpServer = Exclude<HttpServerOrWrapper, { node?: { server?: unknown } }>
type NodeRequest = IncomingMessage & { originalUrl?: string }

type ServeInputNode<Req extends NodeRequest, Res extends ServerResponse> = {
  req: Req
  res: Res
  context?: Telefunc.Context
}

type ServeInputRequest = {
  request: Request
  context?: Telefunc.Context
}

type ServeInput<Req extends NodeRequest, Res extends ServerResponse> = ServeInputNode<Req, Res> | ServeInputRequest

interface TelefuncServe<Req extends NodeRequest, Res extends ServerResponse> {
  installWebSocket(server: HttpServerOrWrapper): void
  serve(input: ServeInputNode<Req, Res>): Promise<boolean>
  serve(input: ServeInputRequest): Promise<Response | undefined>
}

function telefunc<Req extends NodeRequest = NodeRequest, Res extends ServerResponse = ServerResponse>(): TelefuncServe<
  Req,
  Res
> {
  const adapter = telefuncWebSocket()

  function inferServer(req: Req): HttpServer | undefined {
    const server = (req.socket as NodeRequest['socket'] & { server?: HttpServer | Http2SecureServer }).server
    return server
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
    if (server) adapter.install(server)

    if (res.headersSent) return false

    const url = req.originalUrl || req.url
    if (!url) return false
    if (new URL(url, 'http://localhost').pathname !== getServerConfig().telefuncUrl) return false

    const request = await nodeReadableToWebRequest(req, 'http://localhost' + url, req.method || 'GET', req.headers)
    const httpResponse = await serveTelefunc(context ? { request, context } : { request })

    httpResponse.headers.forEach(([name, value]) => res.setHeader(name, value))
    res.statusCode = httpResponse.statusCode
    res.socket?.setNoDelay(true)
    res.flushHeaders()
    httpResponse.pipe(res)
    return true
  }

  return {
    installWebSocket(server: HttpServerOrWrapper): void {
      adapter.install(server)
    },
    serve,
  }
}
