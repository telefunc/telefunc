export { encodeSseRequest, encodeSseRequestMetadata, parseSseRequestMetadata }
export type { SseRequestMetadata }

import { encodeLengthPrefixedString } from './frame.js'
import { assert } from '../utils/assert.js'

/** The metadata header that opens every SSE POST. The two booleans select the POST kind:
 *  - neither: short outbox batch POST. Body ends quickly, so the server defers `reconciled`
 *    to body-end — dispatched frames lift each channel's `_lastClientSeq` first, giving
 *    accurate `lastSeq` numbers.
 *  - `streamResponse`: opens the server→client `text/event-stream` downstream wire.
 *  - `streamRequest`: the long-lived client→server upload POST. Its body never ends, so the
 *    server emits `reconciled` inline instead of deferring. */
type SseRequestMetadata = {
  connId: string
  streamResponse?: true
  streamRequest?: true
}

/** `[u32 length][metadata UTF-8]` — the wire header, shared by all three POST kinds. The
 *  streaming stream-request POST pushes this onto its body directly (its body can't be a
 *  one-shot Blob); the others hand it to `encodeSseRequest` with their batch appended. */
function encodeSseRequestMetadata(metadata: SseRequestMetadata): Uint8Array<ArrayBuffer> {
  return encodeLengthPrefixedString(JSON.stringify(metadata))
}

function encodeSseRequest(metadata: SseRequestMetadata, batch?: Uint8Array<ArrayBuffer>): Blob {
  const header = encodeSseRequestMetadata(metadata)
  return new Blob(batch ? [header, batch] : [header])
}

function parseSseRequestMetadata(metadataText: string): SseRequestMetadata {
  const raw = JSON.parse(metadataText) as Record<string, unknown>
  assert(typeof raw.connId === 'string' && raw.connId.length > 0, 'Malformed SSE request connId')
  const metadata: SseRequestMetadata = { connId: raw.connId }
  if (raw.streamResponse === true) metadata.streamResponse = true
  if (raw.streamRequest === true) metadata.streamRequest = true
  return metadata
}
