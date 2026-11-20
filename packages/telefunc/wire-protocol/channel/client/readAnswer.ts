export { readAnswer }
export type { HttpAnswer }

import { STATUS_CODE_WIRE_VERSION } from '../../constants.js'

/** What the answer to a request of the page's SSE transport means. */
type HttpAnswer =
  | { tag: 'expected' }
  /** The server runs another `WIRE_VERSION`. */
  | { tag: 'otherWireVersion' }
  /** A 413: something in front refused the request for its size (a proxy's limit on a request's body), so a smaller one
   *  may pass. `answer` and `retryAfterMs` as for `refused` (RFC 9110, section 15.5.14: a 413 may ask to wait). */
  | { tag: 'tooLarge'; answer: string; retryAfterMs: number }
  /** Anything else: `answer` names its status and type, `retryAfterMs` is the wait its `Retry-After` asks for. */
  | { tag: 'refused'; status: number; answer: string; retryAfterMs: number }

/** Every request of the SSE transport reads its answer here, so that a status means one thing wherever it comes: its
 *  open expects a 200 event stream, as a browser's EventSource does (HTML, "the EventSource interface"), and a POST an
 *  answer that is ok. */
function readAnswer(response: Response, expects: 'event-stream' | 'ok'): HttpAnswer {
  if (response.status === STATUS_CODE_WIRE_VERSION) return { tag: 'otherWireVersion' }
  const expected =
    expects === 'ok'
      ? response.ok
      : response.status === 200 && mediaTypeOf(response) === 'text/event-stream' && response.body !== null
  if (expected) return { tag: 'expected' }
  if (response.status === 413) {
    return { tag: 'tooLarge', answer: describeAnswer(response), retryAfterMs: retryAfterMsOf(response) }
  }
  return {
    tag: 'refused',
    status: response.status,
    answer: describeAnswer(response),
    retryAfterMs: retryAfterMsOf(response),
  }
}

/** The media type a response carries, without its parameters (`text/event-stream; charset=utf-8` is
 *  `text/event-stream`), as Fetch extracts it from its `Content-Type` values
 *  (https://fetch.spec.whatwg.org/#concept-header-extract-mime-type): the last that parses and isn't `*\/*` wins. Empty
 *  where none does. Written out, since a page can't ask the platform: Fetch types a body's `blob()` by this extraction,
 *  but Chromium 158 types it with the header as it came. */
function mediaTypeOf(response: Response): string {
  let essence = ''
  for (const value of splitHeaderValue(response.headers.get('content-type') ?? '')) {
    const parsed = essenceOf(value)
    if (parsed !== null && parsed !== '*/*') essence = parsed
  }
  return essence
}

/** A header's values: split at each comma outside a quoted string, each without the spaces and tabs around it
 *  (https://fetch.spec.whatwg.org/#header-value-get-decode-and-split). */
function splitHeaderValue(value: string): string[] {
  const values: string[] = []
  let current = ''
  let quoted = false
  for (let i = 0; i < value.length; i++) {
    const char = value.charAt(i)
    if (!quoted && char === ',') {
      values.push(current.replace(/^[\t ]+|[\t ]+$/g, ''))
      current = ''
      continue
    }
    current += char
    if (char === '"') quoted = !quoted
    // Inside a quoted string, a backslash takes the character after it as it is.
    else if (quoted && char === '\\' && i + 1 < value.length) current += value.charAt(++i)
  }
  values.push(current.replace(/^[\t ]+|[\t ]+$/g, ''))
  return values
}

/** A MIME type's `type/subtype`, lowercased; null where it doesn't parse: either is empty or holds more than HTTP token
 *  code points (https://mimesniff.spec.whatwg.org/#parse-a-mime-type, whose parameters never make it fail). */
function essenceOf(value: string): string | null {
  const match = /^[\t\n\r ]*([-!#$%&'*+.^_`|~0-9A-Za-z]+)\/([-!#$%&'*+.^_`|~0-9A-Za-z]+)[\t\n\r ]*(?:;|$)/.exec(value)
  return match === null ? null : `${match[1]}/${match[2]}`.toLowerCase()
}

/** A response's status and `Content-Type`, as an error message names them. */
function describeAnswer(response: Response): string {
  const contentType = response.headers.get('content-type')
  const type = contentType === null ? ' and no Content-Type' : ` ${contentType}`
  return `${response.status}${type}${response.body ? '' : ' and no body'}`
}

/** The wait a `Retry-After` asks for, in seconds or as a date (RFC 9110, section 10.2.3); 0 without one, or where a
 *  server of another origin doesn't expose it. */
function retryAfterMsOf(response: Response): number {
  const value = response.headers.get('retry-after')?.trim()
  if (!value) return 0
  const ms = /^\d+$/.test(value) ? Number(value) * 1000 : Date.parse(value) - Date.now()
  return Number.isFinite(ms) ? Math.max(0, ms) : 0
}
