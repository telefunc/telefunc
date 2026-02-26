export { telefunc }

import { runTelefunc, HttpResponse } from './runTelefunc.js'
import { Telefunc } from './getContext.js'
import { assertUsage } from '../../utils/assert.js'
import { hasProp } from '../../utils/hasProp.js'
import { isObject } from '../../utils/isObject.js'
import { nodeReadableToWebRequest } from '../../utils/nodeReadableToWebRequest.js'
import type { Readable } from 'node:stream'

type HttpRequestResolved = {
  request: Request
  context?: Telefunc.Context
}

type HttpRequest =
  | {
      /** The URL of the HTTP Request */
      url: string
      /** The method of the HTTP Request ('GET', 'POST', ...) */
      method: string
      /** The body of HTTP Request. */
      body: string
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
      headers: Record<string, string | string[] | undefined> | [string, string][]
      /** The context object, see https://telefunc.com/getContext  */
      context?: Telefunc.Context
    }

/** Get HTTP Response for a telefunction remote call HTTP Request.
 * @returns HTTP Response
 */
async function telefunc(httpRequest: HttpRequest): Promise<HttpResponse> {
  assertHttpRequest(httpRequest, arguments.length)
  const httpRequestResolved = await resolveHttpRequest(httpRequest)
  const httpResponse = await runTelefunc(httpRequestResolved)
  return httpResponse
}

async function resolveHttpRequest(httpRequest: HttpRequest): Promise<HttpRequestResolved> {
  if ('request' in httpRequest) {
    return { request: httpRequest.request, context: httpRequest.context }
  }
  if ('readable' in httpRequest) {
    const request = await nodeReadableToWebRequest(
      httpRequest.readable,
      'http://localhost' + httpRequest.url,
      httpRequest.method,
      httpRequest.headers,
    )
    return { request, context: httpRequest.context }
  }
  // Backward compat: construct a Request from primitives
  const request = new Request('http://localhost' + httpRequest.url, {
    method: httpRequest.method,
    body: httpRequest.body,
    headers: { 'content-type': 'text/plain' },
  })
  return { request, context: httpRequest.context }
}

function assertHttpRequest(httpRequest: unknown, numberOfArguments: number) {
  assertUsage(numberOfArguments === 1, '`telefunc()`: all arguments should be passed as a single argument object.')
  assertUsage(httpRequest, '`telefunc(httpRequest)`: argument `httpRequest` is missing.')
  assertUsage(isObject(httpRequest), '`telefunc(httpRequest)`: argument `httpRequest` should be an object.')
  const hasRequest = hasProp(httpRequest, 'request')
  const hasReadable = hasProp(httpRequest, 'readable')
  if (hasRequest) {
    assertUsage(
      httpRequest.request instanceof Request,
      '`telefunc({ request })`: argument `request` should be a Web Request object.',
    )
  } else if (hasReadable) {
    assertUsage(hasProp(httpRequest, 'url'), '`telefunc({ url })`: argument `url` is missing.')
    assertUsage(hasProp(httpRequest, 'url', 'string'), '`telefunc({ url })`: argument `url` should be a string.')
    assertUsage(hasProp(httpRequest, 'method'), '`telefunc({ method })`: argument `method` is missing.')
    assertUsage(
      hasProp(httpRequest, 'method', 'string'),
      '`telefunc({ method })`: argument `method` should be a string.',
    )
    assertUsage(hasProp(httpRequest, 'headers'), '`telefunc({ headers })`: argument `headers` is missing.')
  } else {
    assertUsage(hasProp(httpRequest, 'url'), '`telefunc({ url })`: argument `url` is missing.')
    assertUsage(hasProp(httpRequest, 'url', 'string'), '`telefunc({ url })`: argument `url` should be a string.')
    assertUsage(hasProp(httpRequest, 'method'), '`telefunc({ method })`: argument `method` is missing.')
    assertUsage(
      hasProp(httpRequest, 'method', 'string'),
      '`telefunc({ method })`: argument `method` should be a string.',
    )
    assertUsage(hasProp(httpRequest, 'body'), '`telefunc({ body })`: argument `body` is missing.')
    assertUsage(
      hasProp(httpRequest, 'body', 'string'),
      '`telefunc({ body })`: argument `body` should be a string. Make sure `body` is the HTTP body string of the request. Note that with some server frameworks, such as Express.js, a server middleware is needed to parse the HTTP body of `Content-Type: text/plain` requests.',
    )
  }
  assertUsage(
    !('context' in httpRequest) || hasProp(httpRequest, 'context', 'object'),
    '`telefunc({ context })`: argument `context` should be an object.',
  )
  const allowedKeys = hasRequest
    ? ['request', 'context']
    : hasReadable
      ? ['url', 'method', 'readable', 'headers', 'context']
      : ['url', 'method', 'body', 'context']
  Object.keys(httpRequest).forEach((key) => {
    assertUsage(allowedKeys.includes(key), '`telefunc({ ' + key + ' })`: Unknown argument `' + key + '`.')
  })
  // We further assert the `httpRequest` in `./runTelefunc/parseHttpRequest.ts`
}
