export { encodeSseRequest, parseSseRequestMetadata, parseSseChannels, METADATA_REFRESH_ALIAS }
export type { SseRequest, SseRequestMetadata, SseDataPostMetadata, SseRouteChannel }

import { encodeRequestEnvelope } from './frame.js'
import { assert } from '../utils/assert.js'

/** Per-channel routing entry the client supplies on every data POST. The receiver builds
 *  an alias → (id, home) map at POST start and routes each frame straight to its home —
 *  zero cluster lookups, zero frame decoding. */
type SseRouteChannel = { id: string; home: string }

/** Sentinel alias value: an entry with this alias carries a JSON-encoded `channels[]`
 *  refresh, used by the long-lived stream-request POST to swap routing tables in-band when
 *  a reconcile redefines the channel set without tearing down the half-duplex POST. */
const METADATA_REFRESH_ALIAS = 0xff

type SseRequest =
  | {
      connId: string
      /** Server returns a streaming `text/event-stream` response on this POST — the SSE
       *  downstream wire that pushes server→client frames. The request body is short
       *  (initial reconcile + initial outbox frames) and ends quickly. */
      streamResponse: true
      batch?: Uint8Array<ArrayBuffer>
    }
  | {
      connId: string
      ownerInstance: string
      channels: readonly SseRouteChannel[]
      /** Set on the long-lived client→server upload POST: the request body streams over
       *  the connection's lifetime so in-body reconciles must emit `reconciled` inline
       *  (the body never ends, can't defer to body-end). Outbox batch POSTs omit it and
       *  keep the deferred path: their body ends quickly, dispatched frames update each
       *  channel's `_lastClientSeq` first, and `reconciled` is sent at body end with
       *  accurate `lastSeq` numbers. */
      streamRequest?: true
      batch: Uint8Array<ArrayBuffer>
    }

type SseDataPostMetadata = {
  connId: string
  ownerInstance: string
  channels: readonly SseRouteChannel[]
  streamRequest: boolean
}

type SseRequestMetadata = { connId: string; streamResponse: true } | SseDataPostMetadata

function encodeSseRequest(request: SseRequest): Blob {
  const metadata: Record<string, unknown> = { connId: request.connId }
  if ('streamResponse' in request) {
    metadata.streamResponse = true
  } else {
    metadata.ownerInstance = request.ownerInstance
    metadata.channels = request.channels
    if (request.streamRequest) metadata.streamRequest = true
  }
  const batch = 'batch' in request && request.batch ? [request.batch] : []
  return encodeRequestEnvelope(JSON.stringify(metadata), batch)
}

function parseSseRequestMetadata(metadataText: string): SseRequestMetadata {
  const raw = JSON.parse(metadataText) as Record<string, unknown>
  assert(typeof raw.connId === 'string' && raw.connId.length > 0, 'Malformed SSE request connId')
  if (raw.streamResponse === true) return { connId: raw.connId, streamResponse: true }
  assert(typeof raw.ownerInstance === 'string' && raw.ownerInstance.length > 0, 'Malformed SSE data POST ownerInstance')
  return {
    connId: raw.connId,
    ownerInstance: raw.ownerInstance,
    channels: parseSseChannels(raw.channels),
    streamRequest: raw.streamRequest === true,
  }
}

function parseSseChannels(value: unknown): SseRouteChannel[] {
  assert(Array.isArray(value), 'Malformed SSE channels list')
  return value.map((entry, i) => {
    assert(typeof entry === 'object' && entry !== null, `Malformed SSE channels[${i}]`)
    const e = entry as Record<string, unknown>
    assert(typeof e.id === 'string' && typeof e.home === 'string', `Malformed SSE channels[${i}] fields`)
    return { id: e.id, home: e.home }
  })
}
