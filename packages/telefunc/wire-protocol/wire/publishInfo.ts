export { encodePublish, decodePublishText, decodePublishBinary }
export type { WirePublishInfo }

import { assertProtocol } from './protocolViolation.js'
import { decodeOrderingFrame, encodeOrderingFrame } from './orderingFrame.js'
import { assert } from '../../utils/assert.js'

/** Ordering metadata embedded in PUBLISH frames on the wire. */
type WirePublishInfo = { seq: number; timestamp: number }

// ===== Publish info helpers =====
// Format: seq,timestamp\n{serialized text}
// JSON.stringify never produces bare \n, so the first \n reliably splits info from payload.

/** A publish frame at `info`; a text one with no ordering (null) carries seq 0 and the current time. */
function encodePublish(text: string, info: WirePublishInfo | null): string
function encodePublish(data: Uint8Array, info: WirePublishInfo): Uint8Array
function encodePublish(payload: string | Uint8Array, info: WirePublishInfo | null): string | Uint8Array {
  if (typeof payload === 'string') {
    const { seq, timestamp } = info ?? { seq: 0, timestamp: Date.now() }
    return seq + ',' + timestamp + '\n' + payload
  }
  assert(info !== null)
  return encodeOrderingFrame(payload, info)
}

function decodePublishText(wire: string): { text: string; info: WirePublishInfo } {
  const nl = wire.indexOf('\n')
  assertProtocol(nl !== -1, 'PUBLISH frame missing info prefix')
  const comma = wire.indexOf(',')
  assertProtocol(comma !== -1 && comma < nl, 'PUBLISH frame malformed info prefix')
  const seq = Number(wire.slice(0, comma))
  const timestamp = Number(wire.slice(comma + 1, nl))
  assertProtocol(Number.isFinite(seq) && Number.isFinite(timestamp), 'PUBLISH frame info must be finite numbers')
  return { text: wire.slice(nl + 1), info: { seq, timestamp } }
}

function decodePublishBinary(wire: Uint8Array): { data: Uint8Array; info: WirePublishInfo } {
  const { payload, info } = decodeOrderingFrame(wire)
  return { data: payload, info }
}
