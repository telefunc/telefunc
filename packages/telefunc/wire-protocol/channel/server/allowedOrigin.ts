export { parseAllowedOrigin, originOf, NOT_AN_ORIGIN }
export type { AllowedOrigin }

/** A scheme and a host with nothing a URL parser would take for credentials, a path, a query or a fragment after it,
 *  and nothing it would drop: no space or control character. One trailing `/` is taken, as an address bar shows a site. */
const ORIGIN_AS_WRITTEN = /^[a-z][a-z\d+.-]*:\/\/[^/\\?#@*\s\p{Cc}]+\/?$/iu

/** Why an entry that isn't written as an origin can't be one. */
const NOT_AN_ORIGIN =
  "an entry is an origin as a browser sends it, a scheme and a host with no path, such as 'https://app.example.com', where a leading '*.' stands for any one subdomain label, as in 'https://*.example.com'"

/** The longest origin a page can have: the longest scheme in IANA's URI scheme registry (36 characters,
 *  `microsoft.windows.camera.multipicker`), `://`, the longest hostname DNS has (253 characters) and a port (`:65535`). */
const ORIGIN_MAX = 36 + '://'.length + 253 + ':65535'.length

/** An entry of `config.channel.allowedOrigins`: one origin, or any one subdomain label under `parent`. */
type AllowedOrigin =
  | { kind: 'exact'; origin: string }
  | { kind: 'subdomain'; protocol: string; port: string; parent: string }

/** The origin of `url` in the one form the upgrade rule compares: the URL standard's serialization where it gives one
 *  (`http`, `https`, `ws`, `wss`, `ftp`); for the scheme of an app shell's own (`capacitor://localhost`), whose origin
 *  the standard leaves opaque, the scheme and the host in lower case. Null for a URL whose page has no origin to list:
 *  a `file:` one, one with no host, or one longer than any origin (`ORIGIN_MAX`). */
function originOf(url: URL): string | null {
  const opaque = url.origin === 'null'
  if (opaque && (url.protocol === 'file:' || url.host === '')) return null
  const origin = opaque ? `${url.protocol}//${url.host.toLowerCase()}` : url.origin
  return origin.length <= ORIGIN_MAX ? origin : null
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
  if (!anySubdomain) return { kind: 'exact', origin }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    const why = "a leading '*.' stands for a subdomain on http and https only"
    // A scheme the URL standard gives no origin is an app shell's own (`capacitor:`); `ws:`, `wss:` and `ftp:` are none.
    return url.origin === 'null' ? `${why}: list an app shell's origin as it is, such as 'capacitor://localhost'` : why
  }
  return { kind: 'subdomain', protocol: url.protocol, port: url.port, parent: url.hostname.slice('x.'.length) }
}
