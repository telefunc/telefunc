export { upgradeRefusal }

import { getChannelConfig } from '../../../node/server/serverConfig.js'
import { assertWarning } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'

/** How many refused origins a process names in its log. An app whose own pages are refused finds their origin among the
 *  first it names; past this many, a refusal keeps and prints nothing, however many origins clients send. */
const REFUSED_ORIGINS_NAMED_MAX = 20
/** The longest hostname DNS has: a longer one is no page's origin. */
const HOSTNAME_MAX = 253

const globalObject = getGlobalObject('wire-protocol/channel/server/upgradeOrigin.ts', {
  /** The refused origins the log named, by the name it gave them. */
  named: new Set<string>(),
  /** Past the bound: no refusal is logged any more. */
  quiet: false,
})

/** The answer to a WebSocket upgrade that a page of another origin, which the app didn't allow, makes; undefined where
 *  it may open. A request with neither `Origin` nor `Sec-Fetch-Site` comes from no browser, and goes on. */
function upgradeRefusal(request: Request): Response | undefined {
  const origin = request.headers.get('origin')
  const site = request.headers.get('sec-fetch-site')
  if (origin === null && site === null) return undefined
  if (site === 'same-origin') return undefined
  const url = origin !== null && URL.canParse(origin) ? new URL(origin) : null
  if (url && (isServersHost(url, request) || isAllowed(url))) return undefined
  warnRefusal(origin === null ? 'a page that sent no Origin' : nameOf(url))
  return new Response(null, { status: 403 })
}

/** The refused page's origin as the log names it: the parsed origin, never the header's own text. */
function nameOf(url: URL | null): string {
  if (url === null || url.origin === 'null' || url.hostname.length > HOSTNAME_MAX)
    return 'a page whose Origin header names no origin'
  return url.origin
}

/** Once per name, for the first `REFUSED_ORIGINS_NAMED_MAX` names; the next one says that the rest are not logged. */
function warnRefusal(name: string): void {
  if (globalObject.quiet || globalObject.named.has(name)) return
  if (globalObject.named.size === REFUSED_ORIGINS_NAMED_MAX) {
    globalObject.quiet = true
    assertWarning(
      false,
      `WebSocket upgrades from more than ${REFUSED_ORIGINS_NAMED_MAX} origins were refused: the upgrades of further origins are refused without a warning.`,
      { onlyOnce: false },
    )
    return
  }
  globalObject.named.add(name)
  assertWarning(
    false,
    `A WebSocket upgrade from ${name} was refused: it isn't this server's origin, and \`config.channel.allowedOrigins\` doesn't list it. A page that also takes SSE stays on SSE; one with WebSocket alone can't connect, and its channels end as when the server is unreachable. Behind a proxy that rewrites \`Host\`, have it forward the public host in \`X-Forwarded-Host\`; for a page of another origin, add that origin to \`config.channel.allowedOrigins\`.`,
    { onlyOnce: false },
  )
}

/** The host a client reached: the first `X-Forwarded-Host`, which a proxy sets to the public one, else `Host`. */
function isServersHost(origin: URL, request: Request): boolean {
  const host = request.headers.get('x-forwarded-host')?.split(',')[0]?.trim() || request.headers.get('host')
  const reached = `${origin.protocol}//${host}`
  return host !== null && URL.canParse(reached) && new URL(reached).host === origin.host
}

function isAllowed(origin: URL): boolean {
  return getChannelConfig().allowedOrigins.some((allowed) => {
    if ('origin' in allowed) return allowed.origin === origin.origin
    const label = origin.hostname.slice(0, -(allowed.parent.length + 1))
    return (
      origin.protocol === allowed.protocol &&
      origin.port === allowed.port &&
      origin.hostname.endsWith(`.${allowed.parent}`) &&
      label !== '' &&
      !label.includes('.')
    )
  })
}
