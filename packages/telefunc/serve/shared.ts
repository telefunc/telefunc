export { isTelefuncRequest, toResponse }

import type { HttpResponse } from '../node/server/runTelefunc.js'
import { getServerConfig } from '../node/server/serverConfig.js'

function isTelefuncRequest(request: Request, options?: { allowPrefix?: boolean }): boolean {
  const pathname = new URL(request.url).pathname
  const telefuncUrl = getServerConfig().telefuncUrl
  return options?.allowPrefix ? pathname.startsWith(telefuncUrl) : pathname === telefuncUrl
}

function toResponse(httpResponse: HttpResponse): Response {
  return new Response(httpResponse.getReadableWebStream(), {
    status: httpResponse.statusCode,
    headers: new Headers(httpResponse.headers as HeadersInit),
  })
}
