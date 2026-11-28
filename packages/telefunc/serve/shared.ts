export { isTelefuncRequest, toResponse }

import type { HttpResponse } from '../node/server/runTelefunc.js'
import { getTelefuncUrl } from '../node/server/serverConfig.js'
import { getRequestPathname } from '../utils/getUrlPathname.js'

function isTelefuncRequest(request: Request): boolean {
  return getRequestPathname(request.url) === getTelefuncUrl()
}

function toResponse(httpResponse: HttpResponse, extraHeaders?: readonly [string, string][]): Response {
  const headers = new Headers(httpResponse.headers)
  if (extraHeaders) for (const [name, value] of extraHeaders) headers.set(name, value)
  return new Response(httpResponse.getReadableWebStream(), {
    status: httpResponse.statusCode,
    headers,
  })
}
