export { encodeSseRequest, parseSseRequestMetadata }
export type { SseRequest, SseRequestMetadata }

import { encodeRequestEnvelope } from './frame.js'
import { assert } from '../utils/assert.js'

type SseRequest =
  | {
      connId: string
      stream: true
      batch?: Uint8Array<ArrayBuffer>
    }
  | {
      connId: string
      batch: Uint8Array<ArrayBuffer>
    }

type SseRequestMetadata = {
  connId: string
  stream?: true
}

function encodeSseRequest(request: SseRequest): Blob {
  const metadata =
    'stream' in request
      ? JSON.stringify({ connId: request.connId, stream: true })
      : JSON.stringify({ connId: request.connId })
  return encodeRequestEnvelope(metadata, 'batch' in request && request.batch ? [request.batch] : [])
}

function parseSseRequestMetadata(metadataText: string): SseRequestMetadata {
  const metadata = JSON.parse(metadataText) as { connId?: unknown; stream?: unknown }
  assert(typeof metadata.connId === 'string' && metadata.connId.length > 0, 'Malformed SSE request connId')
  assert(metadata.stream === undefined || metadata.stream === true, 'Malformed SSE request stream flag')
  return metadata.stream === true ? { connId: metadata.connId, stream: true } : { connId: metadata.connId }
}
