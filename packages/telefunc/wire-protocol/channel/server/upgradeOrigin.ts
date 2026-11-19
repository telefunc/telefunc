export { upgradeRefusal, parseAllowedOrigin }

import { getChannelConfig } from '../../../node/server/serverConfig.js'
import { assertWarning } from '../../../utils/assert.js'

/** An entry of `config.channel.allowedOrigins`: one origin, or any one subdomain label under `parent`. */
type AllowedOrigin = { origin: string } | { protocol: string; port: string; parent: string }

/** The entry as an origin a browser sends (`'https://app.example.com'`), where a leading `*.` label stands for any one
 *  subdomain label (`'https://*.example.com'`); null for anything else. */
function parseAllowedOrigin(entry: string): AllowedOrigin | null {
  const anySubdomain = entry.includes('://*.')
  const concrete = anySubdomain ? entry.replace('://*.', '://x.') : entry
  if (concrete.includes('*') || !URL.canParse(concrete)) return null
  const url = new URL(concrete)
  if (url.origin !== concrete) return null
  if (!anySubdomain) return { origin: url.origin }
  return { protocol: url.protocol, port: url.port, parent: url.hostname.slice('x.'.length) }
}

/** The answer to a WebSocket upgrade that a page of another origin, which the app didn't allow, makes; undefined where
 *  it may open. A request with neither `Origin` nor `Sec-Fetch-Site` comes from no browser, and goes on. */
function upgradeRefusal(request: Request): Response | undefined {
  const origin = request.headers.get('origin')
  const site = request.headers.get('sec-fetch-site')
  if (origin === null && site === null) return undefined
  if (site === 'same-origin') return undefined
  const url = origin !== null && URL.canParse(origin) ? new URL(origin) : null
  if (url && (isServersHost(url, request) || isAllowed(url))) return undefined
  assertWarning(
    false,
    `A WebSocket upgrade from ${origin ?? 'a page that sent no Origin'} was refused: it isn't this server's origin, and \`config.channel.allowedOrigins\` doesn't list it. Its page stays on SSE.`,
    { onlyOnce: `upgradeRefusal:${origin}` },
  )
  return new Response(null, { status: 403 })
}

/** The host a client reached: the first `X-Forwarded-Host`, which a proxy sets to the public one, else `Host`. */
function isServersHost(origin: URL, request: Request): boolean {
  const host = request.headers.get('x-forwarded-host')?.split(',')[0]?.trim() || request.headers.get('host')
  const reached = `${origin.protocol}//${host}`
  return host !== null && URL.canParse(reached) && new URL(reached).host === origin.host
}

function isAllowed(origin: URL): boolean {
  return getChannelConfig().allowedOrigins.some((entry) => {
    const allowed = parseAllowedOrigin(entry)
    if (allowed === null) return false
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
