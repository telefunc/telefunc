export { encodeSseRequest, encodeSseRequestMetadata, parseSseRequestMetadata, SSE_FLUSH_TAKEN, SSE_FLUSH_READ }
export type { SseRequestMetadata }

import { encodeLengthPrefixedString } from './frame.js'
import { assert } from '../utils/assert.js'

/** The metadata header that opens every SSE POST. The booleans select the POST kind:
 *  - none: short batch POST, a heartbeat's PING or the barrier, read at once. Body ends quickly, so the server defers
 *    `reconciled` to body-end — dispatched frames lift each channel's `_lastClientSeq` first, giving
 *    accurate `lastSeq` numbers.
 *  - `flush`: a batch POST of the page's outbox, `reconciled` deferred as above. The server reads flushes one after
 *    another, answering each as it begins to read it, and the page sends a flush once the one before is answered: they
 *    come in order, and each crosses the link while the server still reads the one before.
 *  - `streamResponse`: opens the server→client `text/event-stream` downstream wire.
 *  - `streamRequest`: the long-lived client→server upload POST. Its body never ends, so the
 *    server emits `reconciled` inline instead of deferring. */
type SseRequestMetadata = {
  connId: string
  flush?: true
  streamResponse?: true
  streamRequest?: true
}

/** A flush's answer says this with its headers, as the server takes the flush: a browser hands the page an answer whose
 *  body is empty only once the request's body is sent (Firefox 144). */
const SSE_FLUSH_TAKEN = 'taken\n'
/** Then this, once the server has read all of it. Its answer ends without it if the server couldn't. */
const SSE_FLUSH_READ = 'read\n'

/** `[u32 length][metadata UTF-8]` — the wire header, shared by all POST kinds. The
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
  if (raw.flush === true) metadata.flush = true
  if (raw.streamResponse === true) metadata.streamResponse = true
  if (raw.streamRequest === true) metadata.streamRequest = true
  return metadata
}
