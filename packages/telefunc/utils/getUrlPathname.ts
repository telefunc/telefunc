export { getUrlPathname, getRequestPathname }

function getUrlPathname(url: string): string {
  return new URL(url, 'http://fake-domain.com').pathname
}

/** A raw request path's pathname, or null for one that isn't a URL, such as `//`, which Node accepts. */
function getRequestPathname(url: string): string | null {
  return URL.canParse(url, 'http://fake-domain.com') ? getUrlPathname(url) : null
}
