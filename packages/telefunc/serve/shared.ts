export { getTelefuncRequestUrl, toResponse }

import type { HttpResponse } from '../node/server/runTelefunc.js'
import { getServerConfig } from '../node/server/serverConfig.js'
import { readRequestUrl } from '../utils/getUrlPathname.js'

/** The request's URL where it is Telefunc's (at `config.telefuncUrl`); null for any other request. */
function getTelefuncRequestUrl(request: Request): URL | null {
  const url = readRequestUrl(request.url)
  return url?.pathname === getServerConfig().telefuncUrl ? url : null
}

function toResponse(httpResponse: HttpResponse): Response {
  return new Response(httpResponse.getReadableWebStream(), {
    status: httpResponse.statusCode,
    headers: new Headers(httpResponse.headers as HeadersInit),
  })
}
