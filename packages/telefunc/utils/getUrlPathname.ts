export { readRequestUrl, getRequestPathname }

/** A request's URL as Telefunc reads it, from the full URL a runtime gives or the path a Node request names; null for one
 *  that isn't a URL, such as `//`, which Node accepts, or one that names credentials, which no request's URL carries. */
function readRequestUrl(url: string): URL | null {
  if (!URL.canParse(url, 'http://localhost')) return null
  const parsed = new URL(url, 'http://localhost')
  return parsed.username === '' && parsed.password === '' ? parsed : null
}

function getRequestPathname(url: string): string | null {
  return readRequestUrl(url)?.pathname ?? null
}
