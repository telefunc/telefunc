export { upgradeRefusal }

import { getAllowedOrigins } from '../../../node/server/serverConfig.js'
import { originOf } from './allowedOrigin.js'
import { assertWarning } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

/** How many refused origins the log names in a window. An app whose own pages are refused finds their origin among the
 *  first it names; past this many, a refusal prints nothing until the window ends, however many origins clients send. */
const REFUSED_ORIGINS_NAMED_MAX = 20
/** How long a window of refusal names lasts: the next one names refused origins afresh, so an app's own is never left out
 *  of the log for good. */
const REFUSED_ORIGINS_WINDOW_MS = 10 * 60 * 1000

const globalObject = getGlobalObject('wire-protocol/channel/server/upgradeOrigin.ts', {
  /** The refused origins the log named in this window, by the name it gave them. One past the bound stands for the
   *  window's closing line, printed once. */
  named: new Set<string>(),
  /** When this window began, by `Date.now()`. */
  windowStart: 0,
})

/** The answer to a WebSocket upgrade that a page of another origin, which the app didn't allow, makes; undefined where
 *  it may open. `Sec-Fetch-Site` can only refuse. `same-origin` and `none` open nothing by themselves, so the checks
 *  decide as in Chromium, which sends no Fetch Metadata on a WebSocket handshake. Any other value, the header sent twice
 *  too, keeps the server's own host from opening the page, as for an `http://` page of that host opening `wss://`: only
 *  a listed origin does. A request with no `Origin` comes from no browser and goes on, unless its `Sec-Fetch-Site` says
 *  another origin. `host` is the handshake's `Host`, which the caller validated. Never throws: the upgrade hook that
 *  calls it has no catch, and on Node a throw there would end the process. */
function upgradeRefusal(request: Request, host: string): Response | undefined {
  const origin = request.headers.get('origin')
  const site = request.headers.get('sec-fetch-site')
  const otherOrigin = site !== null && site !== 'same-origin' && site !== 'none'
  if (origin === null) {
    if (!otherOrigin) return undefined
    warnRefusal(
      'a page that sent no Origin',
      'it sent no Origin header, so no entry of `config.channel.allowedOrigins` can let it open',
    )
    return new Response(null, { status: 403 })
  }
  const url = URL.canParse(origin) ? new URL(origin) : null
  if (url !== null && !otherOrigin && isServersHost(origin, url.protocol, request, host)) return undefined
  // A browser writes a page's origin in one form (`originOf()`): a header written any other way names none.
  const page = url !== null && originOf(url) === origin ? url : null
  if (page !== null && isAllowed(page, origin)) return undefined
  if (page === null)
    warnRefusal(
      'a page whose Origin header names no origin',
      "its Origin header isn't an origin as a browser writes one, or is the `null` of a page that has none, so no entry of `config.channel.allowedOrigins` can let it open",
    )
  // A page of an app shell's own scheme is never the server's (`isServersHost()`): only listing its origin can help.
  else if (page.origin === 'null')
    warnRefusal(
      origin,
      "`config.channel.allowedOrigins` doesn't list it: add that origin to `config.channel.allowedOrigins`",
    )
  // Neither can a proxy's host help where the browser says the page is of another origin.
  else if (otherOrigin)
    warnRefusal(
      origin,
      "its browser says it is a page of another origin, and `config.channel.allowedOrigins` doesn't list it: add that origin to `config.channel.allowedOrigins`",
    )
  else
    warnRefusal(
      origin,
      "it isn't this server's origin, and `config.channel.allowedOrigins` doesn't list it. Behind a proxy that rewrites `Host`, have it forward the public host in `X-Forwarded-Host`; for a page of another origin, add that origin to `config.channel.allowedOrigins`",
    )
  return new Response(null, { status: 403 })
}

/** Once per name and window, for the first `REFUSED_ORIGINS_NAMED_MAX` names; the next one says that the rest of the
 *  window's are not logged. `reason` says why the page was refused and what can let it open. */
function warnRefusal(name: string, reason: string): void {
  const now = Date.now()
  if (now - globalObject.windowStart >= REFUSED_ORIGINS_WINDOW_MS) {
    globalObject.named.clear()
    globalObject.windowStart = now
  }
  if (globalObject.named.size > REFUSED_ORIGINS_NAMED_MAX || globalObject.named.has(name)) return
  globalObject.named.add(name)
  if (globalObject.named.size > REFUSED_ORIGINS_NAMED_MAX) {
    assertWarning(
      false,
      `WebSocket upgrades from more than ${REFUSED_ORIGINS_NAMED_MAX} origins were refused within ${REFUSED_ORIGINS_WINDOW_MS / 60_000} minutes: the upgrades of further origins are refused without a warning until those ${REFUSED_ORIGINS_WINDOW_MS / 60_000} minutes have passed.`,
      { onlyOnce: false },
    )
    return
  }
  const pageAfter =
    "A page that also takes SSE stays on SSE; one with WebSocket alone can't connect, and its channels end as when the server is unreachable."
  assertWarning(false, `A WebSocket upgrade from ${name} was refused: ${reason}. ${pageAfter}`, { onlyOnce: false })
}

/** Whether `origin`, the `Origin` header, is the origin of the host the client reached, on the page's `protocol`: the
 *  first `X-Forwarded-Host`, which a proxy sets to the public one, else `host`. A page of an app shell's own scheme
 *  never is: the URL standard gives that scheme no origin to compare. */
function isServersHost(origin: string, protocol: string, request: Request, host: string): boolean {
  const reached = `${protocol}//${request.headers.get('x-forwarded-host')?.split(',')[0]?.trim() || host}`
  return URL.canParse(reached) && new URL(reached).origin === origin
}

/** Whether `config.channel.allowedOrigins` lists the page at `page`, whose origin is `origin`. */
function isAllowed(page: URL, origin: string): boolean {
  return getAllowedOrigins().some((allowed) => {
    if (allowed.kind === 'exact') return allowed.origin === origin
    const label = page.hostname.slice(0, -(allowed.parent.length + 1))
    return (
      page.protocol === allowed.protocol &&
      page.port === allowed.port &&
      page.hostname.endsWith(`.${allowed.parent}`) &&
      label !== '' &&
      !label.includes('.')
    )
  })
}
