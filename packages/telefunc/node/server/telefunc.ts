export { serve, serveNode, telefunc }

import { runTelefunc, HttpResponse } from './runTelefunc.js'
import { Telefunc } from './context/getContext.js'
import { assertUsage, assertWarning } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { isObject } from '../../utils/isObject.js'
import { nodeReadableToWebRequest, normalizeHeaders } from '../../utils/nodeReadableToWebRequest.js'
import type { Readable, Writable } from 'node:stream'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { getTelefuncUrl } from './serverConfig.js'
import { getRequestPathname, readRequestUrl } from '../../utils/getUrlPathname.js'
import { REQUEST_KIND, REQUEST_KIND_HEADER } from '../../wire-protocol/wire/requestKind.js'

type HttpRequestResolved = {
  request: Request
  /** Node `Readable` source for the request body, when the entry adapter has one
   *  (currently: `serve/node.ts` passes its `IncomingMessage`). Downstream body
   *  consumers read it directly for binary and SSE requests, which have no Web body.
   *  `undefined` on Web-runtime adapters (`{ request }` input from Bun / Deno / edge). */
  readable?: Readable
  context?: Telefunc.Context
  /** Signals a disconnect even if a read of the request body stays pending. */
  clientGone: AbortSignal
}

type HttpRequest =
  | {
      /** The URL of the HTTP Request */
      url: string
      /** The method of the HTTP Request ('GET', 'POST', ...) */
      method: string
      /** The body of HTTP Request. */
      body: string
      /** The HTTP request headers */
      headers: Record<string, string | string[] | undefined> | [string, string][] | Headers
      /** The context object, see https://telefunc.com/getContext  */
      context?: Telefunc.Context
    }
  | {
      /** The request object. */
      request: Request
      /** The context object, see https://telefunc.com/getContext  */
      context?: Telefunc.Context
    }
  | {
      /** The URL of the HTTP Request */
      url: string
      /** The method of the HTTP Request ('GET', 'POST', ...) */
      method: string
      /** The Node.js `req` readable stream. */
      readable: Readable
      /** The HTTP request headers */
      headers: Record<string, string | string[] | undefined> | [string, string][] | Headers
      /** The context object, see https://telefunc.com/getContext  */
      context?: Telefunc.Context
    }

/** Get HTTP Response for a telefunction remote call HTTP Request.
 * @returns HTTP Response
 */
async function serve(httpRequest: HttpRequest): Promise<HttpResponse> {
  assertHttpRequest(httpRequest, arguments.length)
  assertRequestSignalFlag()
  return runRequest(httpRequest)
}

async function runRequest(httpRequest: HttpRequest, response?: Writable): Promise<HttpResponse> {
  const httpRequestResolved = await resolveHttpRequest(httpRequest, response)
  let releaseClientGone = true
  try {
    const httpResponse = await runTelefunc(httpRequestResolved)
    // A 2xx call can return a channel/stream that keeps reading its upload after answering.
    if (httpResponse.statusCode < 400) releaseClientGone = false
    return httpResponse
  } finally {
    // Past this, `httpResponse.pipe(res)` ends a streaming response's body once the client is gone.
    httpRequestResolved.unwatch?.()
    if (releaseClientGone) httpRequestResolved.unwatchClientGone?.()
  }
}

/** Without the flag a Worker's `request.signal` never aborts, and nothing else tells a telefunction that hasn't answered. */
function assertRequestSignalFlag(): void {
  const { Cloudflare } = globalThis as { Cloudflare?: { compatibilityFlags?: Record<string, boolean> } }
  assertWarning(
    Cloudflare?.compatibilityFlags?.enable_request_signal !== false,
    'Add `enable_request_signal` to `compatibility_flags` in your Wrangler configuration: without it, `context.signal` and `onClose()` miss a client that disconnects before its telefunction answers. See https://telefunc.com/onClose#context-signal',
    { onlyOnce: true },
  )
}

/** Handles matching requests for the Node adapter and Vite's middleware. */
async function serveNode(
  req: IncomingMessage & { originalUrl?: string },
  res: ServerResponse,
  context?: Telefunc.Context,
): Promise<boolean> {
  if (res.headersSent) return false
  const url = req.originalUrl || req.url
  if (!url || getRequestPathname(url) !== getTelefuncUrl()) return false

  const input = { url, method: req.method || 'GET', readable: req, headers: req.headers }
  const httpRequest = context ? { ...input, context } : input
  assertHttpRequest(httpRequest, 1)
  const httpResponse = await runRequest(httpRequest, res)
  httpResponse.headers.forEach(([name, value]) => res.setHeader(name, value))
  res.statusCode = httpResponse.statusCode
  res.socket?.setNoDelay(true)
  // An HTTP/1.1 400 response that refused an unread body cannot reuse the keep-alive socket (RFC 9112 §9.6).
  if (req.httpVersionMajor < 2 && httpResponse.statusCode === 400 && !req.readableEnded) {
    res.setHeader('Connection', 'close')
  }
  res.flushHeaders()
  await httpResponse.pipe(res)
  return true
}

/** @deprecated `telefunc()` is deprecated, use `new Telefunc()` instead, see https://telefunc.com/Telefunc */
async function telefunc(httpRequest: HttpRequest): Promise<HttpResponse> {
  // TO-DO/next-major-release: remove
  assertWarning(false, '`telefunc()` is deprecated, use `new Telefunc()` instead, see https://telefunc.com/Telefunc', {
    onlyOnce: true,
  })
  return serve(httpRequest)
}

async function resolveHttpRequest(
  httpRequest: HttpRequest,
  response?: Writable,
): Promise<HttpRequestResolved & { unwatch?: () => void; unwatchClientGone?: () => void }> {
  if ('request' in httpRequest) {
    // A `Request` handed over as it is: its signal says its client left, as the Fetch standard has it.
    return {
      request: httpRequest.request,
      context: httpRequest.context,
      clientGone: httpRequest.request.signal,
    }
  }
  // `url` is either the full URL (e.g. `request.url`) or only its pathname (e.g. `req.originalUrl`)
  const url = readRequestUrl(httpRequest.url)
  assertUsage(url, '`serve({ url })`: argument `url` should be a valid request URL or path.')
  if ('readable' in httpRequest) {
    const headers = new Headers(normalizeHeaders(httpRequest.headers))
    const kind = headers.get(REQUEST_KIND_HEADER)
    const nativeBody = kind === REQUEST_KIND.BINARY || kind === REQUEST_KIND.SSE
    const { request, unwatch, unwatchClientGone, clientGone } = await nodeReadableToWebRequest(
      httpRequest.readable,
      url.href,
      httpRequest.method,
      headers,
      nativeBody,
      response,
    )
    return {
      request,
      readable: httpRequest.readable,
      context: httpRequest.context,
      clientGone,
      unwatch,
      unwatchClientGone,
    }
  }
  // Backward compat: construct a Request from primitives
  const request = new Request(url, {
    method: httpRequest.method,
    body: httpRequest.body,
    headers: normalizeHeaders(httpRequest.headers),
  })
  return {
    request,
    context: httpRequest.context,
    clientGone: request.signal,
  }
}

function assertHttpRequest(httpRequest: unknown, numberOfArguments: number) {
  assertUsage(numberOfArguments === 1, '`serve()`: all arguments should be passed as a single argument object.')
  assertUsage(httpRequest, '`serve(httpRequest)`: argument `httpRequest` is missing.')
  assertUsage(isObject(httpRequest), '`serve(httpRequest)`: argument `httpRequest` should be an object.')
  const hasRequest = hasProp(httpRequest, 'request')
  const hasReadable = hasProp(httpRequest, 'readable')
  if (hasRequest) {
    assertUsage(
      httpRequest.request instanceof Request,
      '`serve({ request })`: argument `request` should be a Web Request object.',
    )
  } else if (hasReadable) {
    assertUsage(hasProp(httpRequest, 'url'), '`serve({ url })`: argument `url` is missing.')
    assertUsage(hasProp(httpRequest, 'url', 'string'), '`serve({ url })`: argument `url` should be a string.')
    assertUsage(hasProp(httpRequest, 'method'), '`serve({ method })`: argument `method` is missing.')
    assertUsage(hasProp(httpRequest, 'method', 'string'), '`serve({ method })`: argument `method` should be a string.')
    assertUsage(hasProp(httpRequest, 'headers'), '`serve({ headers })`: argument `headers` is missing.')
  } else {
    assertUsage(hasProp(httpRequest, 'url'), '`serve({ url })`: argument `url` is missing.')
    assertUsage(hasProp(httpRequest, 'url', 'string'), '`serve({ url })`: argument `url` should be a string.')
    assertUsage(hasProp(httpRequest, 'method'), '`serve({ method })`: argument `method` is missing.')
    assertUsage(hasProp(httpRequest, 'method', 'string'), '`serve({ method })`: argument `method` should be a string.')
    assertUsage(hasProp(httpRequest, 'body'), '`serve({ body })`: argument `body` is missing.')
    assertUsage(hasProp(httpRequest, 'headers'), '`serve({ headers })`: argument `headers` is missing.')
    assertUsage(
      hasProp(httpRequest, 'body', 'string'),
      '`serve({ body })`: argument `body` should be a string. Make sure `body` is the HTTP body string of the request. Note that with some server frameworks, such as Express.js, a server middleware is needed to parse the HTTP body of `Content-Type: text/plain` requests.',
    )
  }
  assertUsage(
    !('context' in httpRequest) || hasProp(httpRequest, 'context', 'object'),
    '`serve({ context })`: argument `context` should be an object.',
  )
  const allowedKeys = hasRequest
    ? ['request', 'context']
    : hasReadable
      ? ['url', 'method', 'readable', 'headers', 'context']
      : ['url', 'method', 'body', 'headers', 'context']
  Object.keys(httpRequest).forEach((key) => {
    assertUsage(allowedKeys.includes(key), '`serve({ ' + key + ' })`: Unknown argument `' + key + '`.')
  })
  // We further assert the `httpRequest` in `./runTelefunc/parseHttpRequest.ts`
}
