export { REFUSAL_MEDIA_TYPE, REQUEST_KIND_REFUSAL, WIRE_VERSION_REFUSAL, frameRefusal, refusalContentType }
export type { Refusal, RefusalKind, RefusalContentType }

import { STATUS_BODY_MALFORMED_REQUEST } from '../shared/constants.js'
import { END_CAUSE, type RefusalCause } from './channel/endCause.js'

/** The media type of Telefunc's own refusals of the page's SSE requests, so that the page tells them from an answer of
 *  anything in front of the server, at the headers, from any origin (`Content-Type` is a CORS-safelisted response
 *  header). Used unregistered until its owner registers it (RFC 6838 section 3.2, the vendor tree); a registration
 *  states UTF-8 text, the parameter `kind` and its four values, `channel`, `cause` and `reason`. */
const REFUSAL_MEDIA_TYPE = 'text/vnd.telefunc.refusal'

/** What the page does about a refusal:
 *  - `retry`: the server no longer has the request's wire, a loss; the frames go again on the next wire.
 *  - `frames`: the server can't take a frame; the channel whose frame it is ends, `channel` where the server knows it.
 *  - `route`: the server doesn't serve the page's SSE requests so; the page leaves SSE for this connection.
 *  - `reload`: the server runs another wire version. */
type RefusalKind = 'retry' | 'frames' | 'route' | 'reload'

/** `cause`: the code of the channel's end (`END_CAUSE`), where the refusal ends one. `reason`: a sentence for the
 *  page's error, in printable ASCII. */
type Refusal = { kind: RefusalKind; channel?: number; cause?: number; reason: string }

type RefusalContentType = `${typeof REFUSAL_MEDIA_TYPE}; kind=${RefusalKind}${string}`

/** A page on another wire version reloads: none of its frames could be read. Every adapter that answers it says this. */
const WIRE_VERSION_REFUSAL = {
  kind: 'reload',
  reason: 'The server runs another wire version: reload the page.',
} as const satisfies Refusal

/** A request whose `x-telefunc-request` header is missing, invalid or contradicts its `_telefunc` URL parameter, as
 *  behind a proxy that drops the header: the page leaves SSE rather than retry. Every adapter that answers it says this,
 *  with the body a telefunction call reads. */
const REQUEST_KIND_REFUSAL = { kind: 'route', reason: STATUS_BODY_MALFORMED_REQUEST } as const satisfies Refusal

/** The `frames` refusal of an end for `cause`: of one channel's frame, naming `channel`; of the page's session, naming
 *  none. Its reason is the cause's, and so is the error an `ERROR` of its code gives where no refusal tells the end. */
function frameRefusal(cause: RefusalCause, channel: number | undefined): Refusal {
  const { code, reason } = END_CAUSE[cause]
  return { kind: 'frames', channel, cause: code, reason }
}

/** `text/vnd.telefunc.refusal; kind=frames; channel=3; cause=6; reason="…"`: the reason as a quoted string (RFC 9110
 *  section 5.6.4). */
function refusalContentType({ kind, channel, cause, reason }: Refusal): RefusalContentType {
  const named = channel === undefined ? '' : `; channel=${channel}`
  const caused = cause === undefined ? '' : `; cause=${cause}`
  return `${REFUSAL_MEDIA_TYPE}; kind=${kind}${named}${caused}; reason="${reason.replace(/["\\]/g, '\\$&')}"`
}
