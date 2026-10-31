export { encodeSseRequest, encodeSseBatch, encodeSseRequestMetadata, encodeLengthPrefixedBlob, parseSseRequestMetadata }
export type { SseRequestMetadata }

import { encodeLengthPrefixedString, encodeU32 } from '../wire/envelope.js'
import { assertProtocol } from '../wire/protocolViolation.js'

/** The metadata header that opens every SSE POST. Its `kind`:
 *  - `batch`: short outbox batch POST. Body ends quickly, so the server defers `reconciled`
 *    to body-end — dispatched frames lift each channel's `_lastClientSeq` first, giving
 *    accurate `lastSeq` numbers.
 *  - `streamResponse`: opens the server→client `text/event-stream` downstream wire.
 *  - `streamRequest`: the long-lived client→server upload POST. Its body never ends, so the
 *    server emits `reconciled` inline instead of deferring.
 *  - `close`: the page leaves for good, so the server closes the wire as a WebSocket close would. */
type SseRequestMetadata = {
  connId: string
  kind: 'batch' | 'streamResponse' | 'streamRequest' | 'close'
}

/** On the wire, a kind other than `batch` is its name set to `true`. */
const SSE_REQUEST_FLAGS = ['streamResponse', 'streamRequest', 'close'] as const satisfies SseRequestMetadata['kind'][]

/** `[u32 length][metadata UTF-8]` — the wire header, shared by every POST kind. The
 *  streaming stream-request POST pushes this onto its body directly (its body can't be a
 *  one-shot Blob); the others hand it to `encodeSseRequest` with their batch appended. */
function encodeSseRequestMetadata({ connId, kind }: SseRequestMetadata): Uint8Array<ArrayBuffer> {
  return encodeLengthPrefixedString(JSON.stringify(kind === 'batch' ? { connId } : { connId, [kind]: true }))
}

function encodeSseRequest(metadata: SseRequestMetadata, batch?: Uint8Array<ArrayBuffer>): Blob {
  const header = encodeSseRequestMetadata(metadata)
  return new Blob(batch ? [header, batch] : [header])
}

/** As `encodeSseRequest` with `frames` length-prefixed, but the Blob takes the frames as they are: a batch POST of a
 *  window's worth isn't copied into one buffer first. `head`: the first frames, length-prefixed already. */
function encodeSseBatch(metadata: SseRequestMetadata, frames: Uint8Array<ArrayBuffer>[], head?: Blob): Blob {
  return new Blob([encodeSseRequestMetadata(metadata), encodeLengthPrefixedBlob(frames, head)])
}

/** `frames` length-prefixed after `head`, in a Blob that takes them as they are. */
function encodeLengthPrefixedBlob(frames: Uint8Array<ArrayBuffer>[], head?: Blob): Blob {
  const parts: (Blob | Uint8Array<ArrayBuffer>)[] = head ? [head] : []
  for (const frame of frames) parts.push(encodeU32(frame.byteLength), frame)
  return new Blob(parts)
}

function parseSseRequestMetadata(value: unknown): SseRequestMetadata {
  assertProtocol(value !== null && typeof value === 'object', 'SSE request metadata is not an object')
  const metadata = value as Record<string, unknown>
  const { connId } = metadata
  assertProtocol(typeof connId === 'string' && connId.length > 0, 'SSE request connId')
  let kind: SseRequestMetadata['kind'] = 'batch'
  for (const flag of SSE_REQUEST_FLAGS) {
    if (metadata[flag] !== true) continue
    assertProtocol(kind === 'batch', 'SSE request of two kinds')
    kind = flag
  }
  return { connId, kind }
}
