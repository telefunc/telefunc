export { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl, getRequestKind }
export type { RequestKind }

const REQUEST_KIND_PARAM = '_telefunc'
const REQUEST_KIND_HEADER = 'x-telefunc-request'

const REQUEST_KIND = {
  TEXT: 'txt',
  SSE: 'sse',
  BINARY: 'bin',
  INVALID: 'invalid',
} as const

type RequestKind = Exclude<(typeof REQUEST_KIND)[keyof typeof REQUEST_KIND], 'invalid'>

function getMarkedRequestUrl(telefuncUrl: string, requestKind: RequestKind): string {
  const base = typeof window === 'undefined' ? undefined : window.location.href
  const url = new URL(telefuncUrl, base)
  url.searchParams.set(REQUEST_KIND_PARAM, requestKind)
  return url.href
}

function getRequestKind(
  request: Pick<Request, 'url' | 'headers'>,
  telefuncUrl: string,
): RequestKind | typeof REQUEST_KIND.INVALID {
  const url = new URL(request.url)
  const telefunc = new URL(telefuncUrl, url)
  if (url.pathname !== telefunc.pathname) return REQUEST_KIND.INVALID

  // Required, never inferred from the URL: a cross-site page can't send a custom header without a CORS preflight.
  const headerKind = request.headers.get(REQUEST_KIND_HEADER)
  if (headerKind !== REQUEST_KIND.TEXT && headerKind !== REQUEST_KIND.SSE && headerKind !== REQUEST_KIND.BINARY) {
    return REQUEST_KIND.INVALID
  }
  const urlKind = url.searchParams.get(REQUEST_KIND_PARAM)
  if (urlKind !== null && urlKind !== headerKind) return REQUEST_KIND.INVALID
  return headerKind
}
