export { parseAllowedOrigin, originOf, NOT_AN_ORIGIN }
export type { AllowedOrigin }

/** A scheme and a host with nothing a URL parser would take for credentials, a path, a query or a fragment after it,
 *  and nothing it would drop: no space or control character. */
const ORIGIN_AS_WRITTEN = /^[a-z][a-z\d+.-]*:\/\/[^/\\?#@*\s\p{Cc}]+$/iu

/** Why an entry that isn't written as an origin can't be one. */
const NOT_AN_ORIGIN =
  "an entry is an origin as a browser sends it, a scheme and a host with no path, such as 'https://app.example.com', where a leading '*.' stands for any one subdomain label, as in 'https://*.example.com'"

/** An entry of `config.channel.allowedOrigins`: one origin, or any one subdomain label under `parent`. */
type AllowedOrigin = { origin: string } | { protocol: string; port: string; parent: string }

/** The origin of `url` in the one form the upgrade rule compares: the URL standard's serialization where it gives one
 *  (`http`, `https`, `ws`, `wss`, `ftp`); for the scheme of an app shell's own (`capacitor://localhost`), whose origin
 *  the standard leaves opaque, the scheme and the host in lower case. Null for a URL whose page has no origin to list:
 *  a `file:` one, or one with no host. */
function originOf(url: URL): string | null {
  if (url.origin !== 'null') return url.origin
  if (url.protocol === 'file:' || url.host === '') return null
  return `${url.protocol}//${url.host.toLowerCase()}`
}

/** The entry as an origin (`'https://app.example.com'`, `'capacitor://localhost'`), where a leading `*.` label stands
 *  for any one subdomain label on `http` and `https` (`'https://*.example.com'`), in the form a browser sends: a host
 *  written as a person writes it (`'https://bücher.example'`) is compared as the URL parser gives it
 *  (`https://xn--bcher-kva.example`). Otherwise why it can't be one. The `x.` stand-in asks the URL parser whether a
 *  host with one label in the `*`'s place is valid, which is the wildcard's own question. */
function parseAllowedOrigin(entry: string): AllowedOrigin | string {
  if (entry === 'null')
    return "a page that has no origin sends `null`, and any site can make its page send it, as a sandboxed frame does, so it can't be allowed"
  if (/^file:/i.test(entry))
    return "a page opened from a file has no origin of its own, as the URL standard leaves it opaque, and one entry would stand for every local file, so it can't be allowed"
  const anySubdomain = entry.includes('://*.')
  const concrete = anySubdomain ? entry.replace('://*.', '://x.') : entry
  if (!ORIGIN_AS_WRITTEN.test(concrete) || !URL.canParse(concrete)) return NOT_AN_ORIGIN
  const url = new URL(concrete)
  const origin = originOf(url)
  if (origin === null) return NOT_AN_ORIGIN
  if (!anySubdomain) return { origin }
  if (url.protocol !== 'http:' && url.protocol !== 'https:')
    return "a leading '*.' stands for a subdomain on http and https only: list an app shell's origin as it is, such as 'capacitor://localhost'"
  return { protocol: url.protocol, port: url.port, parent: url.hostname.slice('x.'.length) }
}
