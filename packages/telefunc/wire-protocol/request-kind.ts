export { REQUEST_KIND, REQUEST_KIND_HEADER, getMarkedRequestUrl, getRequestKind }
export type { RequestKind }

const REQUEST_KIND_PARAM = '_telefunc'
const REQUEST_KIND_HEADER = 'x-telefunc-request'

const REQUEST_KIND = {
  TEXT: 'txt',
  SSE: 'sse',
  BINARY: 'bin',
  MISMATCH: 'mismatch',
} as const

type RequestKind = Exclude<(typeof REQUEST_KIND)[keyof typeof REQUEST_KIND], 'mismatch'>

function getMarkedRequestUrl(telefuncUrl: string, requestKind: RequestKind): string {
  const base = typeof window === 'undefined' ? undefined : window.location.href
  const url = new URL(telefuncUrl, base)
  url.searchParams.set(REQUEST_KIND_PARAM, requestKind)
  return url.href
}

function getRequestKind(
  request: Pick<Request, 'url' | 'headers'>,
  telefuncUrl: string,
): RequestKind | 'mismatch' | null {
  const url = new URL(request.url)
  const telefunc = new URL(telefuncUrl, url)
  if (url.pathname !== telefunc.pathname) return null

  const urlKind = parseRequestKind(url.searchParams.get(REQUEST_KIND_PARAM))
  const headerKind = parseRequestKind(request.headers.get(REQUEST_KIND_HEADER))
  if (urlKind === REQUEST_KIND.MISMATCH || headerKind === REQUEST_KIND.MISMATCH) return REQUEST_KIND.MISMATCH
  if (urlKind && headerKind && urlKind !== headerKind) return REQUEST_KIND.MISMATCH
  return urlKind ?? headerKind ?? null
}

function parseRequestKind(value: string | null): RequestKind | 'mismatch' | null {
  if (value === null) return null
  if (value === REQUEST_KIND.TEXT || value === REQUEST_KIND.SSE || value === REQUEST_KIND.BINARY) return value
  return REQUEST_KIND.MISMATCH
}
