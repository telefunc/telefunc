export { parseAllowedOrigin }
export type { AllowedOrigin }

/** A scheme and a host with nothing a URL parser would take for credentials, a path, a query or a fragment after it,
 *  and nothing it would drop: no space or control character. */
const ORIGIN_AS_WRITTEN = /^[a-z][a-z\d+.-]*:\/\/[^/\\?#@*\s\p{Cc}]+$/iu

/** An entry of `config.channel.allowedOrigins`: one origin, or any one subdomain label under `parent`. */
type AllowedOrigin = { origin: string } | { protocol: string; port: string; parent: string }

/** The entry as an origin (`'https://app.example.com'`), where a leading `*.` label stands for any one subdomain label
 *  (`'https://*.example.com'`), in the form a browser sends: a host written as a person writes it
 *  (`'https://bücher.example'`) is compared as the URL parser gives it (`https://xn--bcher-kva.example`). Null for
 *  anything else. */
function parseAllowedOrigin(entry: string): AllowedOrigin | null {
  const anySubdomain = entry.includes('://*.')
  const concrete = anySubdomain ? entry.replace('://*.', '://x.') : entry
  if (!ORIGIN_AS_WRITTEN.test(concrete) || !URL.canParse(concrete)) return null
  const url = new URL(concrete)
  if (url.origin === 'null') return null
  if (!anySubdomain) return { origin: url.origin }
  return { protocol: url.protocol, port: url.port, parent: url.hostname.slice('x.'.length) }
}
