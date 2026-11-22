export { getTelefuncRequestUrl, isTelefuncUrl, toResponse }

import type { HttpResponse } from '../node/server/runTelefunc.js'
import { getTelefuncUrl } from '../node/server/serverConfig.js'
import { getRequestPathname, readRequestUrl } from '../utils/getUrlPathname.js'

/** The request's URL where it is Telefunc's (at `config.telefuncUrl`); null for any other request. */
function getTelefuncRequestUrl(request: Request): URL | null {
  const url = readRequestUrl(request.url)
  return url?.pathname === getTelefuncUrl() ? url : null
}

/** A path, up to its query or fragment, that the URL parser keeps as it is: segments of characters it doesn't escape,
 *  none empty or starting with a dot. */
const PLAIN_PATH = /^(?:\/(?![./])[\w\-.~!$&'()*+,;=:@]*)+(?:[?#]|$)/

/** The URL a Node request names, its path or the absolute URL of a proxy request, is Telefunc's, as for a `Request`:
 *  its pathname is `config.telefuncUrl`. */
function isTelefuncUrl(url: string): boolean {
  const telefuncUrl = getTelefuncUrl()
  // A path that is its own pathname is Telefunc's only if it starts with Telefunc's URL.
  if (!url.startsWith(telefuncUrl) && PLAIN_PATH.test(url)) return false
  return getRequestPathname(url) === telefuncUrl
}

function toResponse(httpResponse: HttpResponse): Response {
  return new Response(httpResponse.getReadableWebStream(), {
    status: httpResponse.statusCode,
    headers: new Headers(httpResponse.headers as HeadersInit),
  })
}
