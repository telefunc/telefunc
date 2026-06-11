export { encodeSseRequest, parseSseRequestMetadata }
export type { SseRequest, SseRequestMetadata, SseDataPostMetadata }

import { encodeRequestEnvelope } from './frame.js'
import { assert } from '../utils/assert.js'

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
  streamRequest: boolean
}

type SseRequestMetadata = { connId: string; streamResponse: true } | SseDataPostMetadata

function encodeSseRequest(request: SseRequest): Blob {
  const metadata: Record<string, unknown> = { connId: request.connId }
  if ('streamResponse' in request) metadata.streamResponse = true
  else if (request.streamRequest) metadata.streamRequest = true
  const batch = 'batch' in request && request.batch ? [request.batch] : []
  return encodeRequestEnvelope(JSON.stringify(metadata), batch)
}

function parseSseRequestMetadata(metadataText: string): SseRequestMetadata {
  const raw = JSON.parse(metadataText) as Record<string, unknown>
  assert(typeof raw.connId === 'string' && raw.connId.length > 0, 'Malformed SSE request connId')
  if (raw.streamResponse === true) return { connId: raw.connId, streamResponse: true }
  return { connId: raw.connId, streamRequest: raw.streamRequest === true }
}
