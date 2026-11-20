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

/** The media type a response says it carries, without its parameters (`text/event-stream; charset=utf-8` is
 *  `text/event-stream`), as the Fetch standard extracts it; null without a `Content-Type`. */
function mediaTypeOf(response: Response): string | null {
  const contentType = response.headers.get('content-type')
  return contentType === null ? null : (contentType.split(';', 1)[0] ?? '').trim().toLowerCase()
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
