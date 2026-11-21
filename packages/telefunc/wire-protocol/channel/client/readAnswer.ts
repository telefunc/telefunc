export { readAnswer, asksLater, endOfWire, telefuncAnswer, telefuncEndOf }
export type { HttpAnswer, TelefuncAnswer }

import { REFUSAL_MEDIA_TYPE } from '../../refusal.js'
import type { TransportEnd } from './WireLifecycle.js'

/** What the answer to a request of the page's SSE transport means. */
type HttpAnswer =
  | { tag: 'expected' }
  /** The server runs another `WIRE_VERSION`, as Telefunc's own refusal of kind `reload` says. */
  | { tag: 'otherWireVersion' }
  /** Telefunc's own refusals (`refusal.ts`), read at the headers: the server no longer has the request's wire, a loss;
   *  it ended `channel` for the cause `cause` (`END_CAUSE`'s code), or the page's session where it names none; it
   *  doesn't serve the page's SSE requests so. */
  | { tag: 'retry'; reason: string }
  | { tag: 'frames'; channel: number | null; cause: number }
  | { tag: 'route'; reason: string }
  /** A 413: something in front refused the request for its size (a proxy's limit on a request's body), so a smaller one
   *  may pass. `answer` and `retryAfterMs` as for `refused` (RFC 9110, section 15.5.14: a 413 may ask to wait). */
  | { tag: 'tooLarge'; answer: string; retryAfterMs: number }
  /** Anything else: `answer` names its status and type, `retryAfterMs` is the wait its `Retry-After` asks for. */
  | { tag: 'refused'; status: number; answer: string; retryAfterMs: number }

/** Every request of the SSE transport reads its answer here, so that a status means one thing wherever it comes: its
 *  open expects a 200 event stream, as a browser's EventSource does (HTML, "the EventSource interface"), and a POST an
 *  answer that is ok. */
function readAnswer(response: Response, expects: 'event-stream' | 'ok'): HttpAnswer {
  const type = mimeTypeOf(response)
  const expected =
    expects === 'ok'
      ? response.ok
      : response.status === 200 && type?.essence === 'text/event-stream' && response.body !== null
  if (expected) return { tag: 'expected' }
  if (type?.essence === REFUSAL_MEDIA_TYPE) return readRefusal(response.status, type.parameters)
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

type TelefuncAnswer = Extract<HttpAnswer, { tag: 'otherWireVersion' | 'retry' | 'frames' | 'route' }>

/** One of Telefunc's own refusals (`refusal.ts`). */
function telefuncAnswer(answer: HttpAnswer | null | 'left'): answer is TelefuncAnswer {
  return (
    answer !== null &&
    answer !== 'left' &&
    (answer.tag === 'otherWireVersion' || answer.tag === 'retry' || answer.tag === 'frames' || answer.tag === 'route')
  )
}

/** Why a wire ends whose request Telefunc refused with `answer`, which alone says what ended: a `frames` refusal ends
 *  the channel it names, or the page's session where it names none, with its cause. */
function telefuncEndOf(answer: TelefuncAnswer): TransportEnd {
  if (answer.tag === 'otherWireVersion') return { otherWireVersion: true }
  if (answer.tag === 'retry') return { lost: answer.reason }
  if (answer.tag === 'route') return { route: answer.reason }
  if (answer.channel === null) return { sessionEnded: answer.cause }
  return { framesRefused: { channel: answer.channel, cause: answer.cause } }
}

/** The end the server decided for a wire, as the wire's own end carries it (a WebSocket's close reason, the event
 *  stream's last event: `refusal.ts`'s `wireEnd`), read as an answer's refusal is; undefined where it carries none this
 *  page reads, and the wire ends as a loss. A told end is a `frames` refusal, which an answer carries with 400. */
function endOfWire(value: string): TransportEnd | undefined {
  const type = parseMimeType(value)
  if (type?.essence !== REFUSAL_MEDIA_TYPE) return undefined
  const answer = readRefusal(400, type.parameters)
  return telefuncAnswer(answer) ? telefuncEndOf(answer) : undefined
}

/** Whether a refusal asks the page to come back later rather than refusing what it sent (RFC 9110 sections 15.5.9 and
 *  15.6, RFC 8470 section 5.2, RFC 6585 section 4): a 408, 425, 429 or 5xx. Such an answer is a loss, judged by the time
 *  the data waits (`WireLifecycle.endStalled`) and never by its count. */
function asksLater(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || (status >= 500 && status <= 599)
}

/** What Telefunc's own refusal says, by its `kind` (`refusal.ts`). A kind the page doesn't know, or none, ends SSE for
 *  this connection rather than loops, naming the status; so does a `frames` refusal that names no cause. */
function readRefusal(status: number, parameters: ReadonlyMap<string, string>): HttpAnswer {
  const kind = parameters.get('kind')
  const reason = parameters.get('reason') ?? `The server refused the request with ${status}.`
  const cause = decimalOf(parameters.get('cause'))
  if (kind === 'reload') return { tag: 'otherWireVersion' }
  if (kind === 'retry') return { tag: 'retry', reason }
  if (kind === 'frames' && cause !== null)
    return { tag: 'frames', channel: decimalOf(parameters.get('channel')), cause }
  if (kind === 'route') return { tag: 'route', reason }
  return { tag: 'route', reason: `${reason} (refused with ${status}, of a kind this page doesn't know)` }
}

/** A parameter's value read as a decimal number; null where it isn't one, or there is none. */
function decimalOf(value: string | undefined): number | null {
  return value !== undefined && /^\d+$/.test(value) ? Number(value) : null
}

/** The MIME type a response carries, as Fetch extracts it from its `Content-Type` values
 *  (https://fetch.spec.whatwg.org/#concept-header-extract-mime-type): the last that parses and isn't `*\/*` wins. Null
 *  where none does. Written out, since a page can't ask the platform: Fetch types a body's `blob()` by this extraction,
 *  but Chromium 158 types it with the header as it came. */
function mimeTypeOf(response: Response): MimeType | null {
  let type: MimeType | null = null
  for (const value of splitHeaderValue(response.headers.get('content-type') ?? '')) {
    const parsed = parseMimeType(value)
    if (parsed !== null && parsed.essence !== '*/*') type = parsed
  }
  return type
}

/** `essence`: `type/subtype`, lowercased (`text/event-stream; charset=utf-8` is `text/event-stream`). `parameters`: by
 *  name, lowercased, each the first of its name. */
type MimeType = { essence: string; parameters: ReadonlyMap<string, string> }

/** A MIME type, as MIME Sniffing parses one (https://mimesniff.spec.whatwg.org/#parse-a-mime-type); null where its
 *  type or subtype is empty or holds more than HTTP token code points. A parameter that doesn't parse is skipped, never
 *  failing the type. */
function parseMimeType(value: string): MimeType | null {
  const match = /^[\t\n\r ]*([-!#$%&'*+.^_`|~0-9A-Za-z]+)\/([-!#$%&'*+.^_`|~0-9A-Za-z]+)[\t\n\r ]*(?:;|$)/.exec(value)
  if (match === null) return null
  const parameters = new Map<string, string>()
  let i = match[0].length
  while (i < value.length) {
    while (i < value.length && /[\t\n\r ]/.test(value.charAt(i))) i++
    let name = ''
    while (i < value.length && value.charAt(i) !== ';' && value.charAt(i) !== '=') name += value.charAt(i++)
    name = name.toLowerCase()
    if (value.charAt(i) === ';') {
      i++
      continue
    }
    i++
    let parameterValue = ''
    if (value.charAt(i) === '"') {
      i++
      while (i < value.length && value.charAt(i) !== '"') {
        if (value.charAt(i) === '\\' && i + 1 < value.length) i++
        parameterValue += value.charAt(i++)
      }
      while (i < value.length && value.charAt(i) !== ';') i++
    } else {
      while (i < value.length && value.charAt(i) !== ';') parameterValue += value.charAt(i++)
      parameterValue = parameterValue.replace(/[\t\n\r ]+$/, '')
      if (parameterValue === '') {
        i++
        continue
      }
    }
    i++
    if (/^[-!#$%&'*+.^_`|~0-9A-Za-z]+$/.test(name) && !parameters.has(name)) parameters.set(name, parameterValue)
  }
  return { essence: `${match[1]}/${match[2]}`.toLowerCase(), parameters }
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
