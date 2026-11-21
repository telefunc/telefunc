export { encodePublishText, decodePublishText, writePublishBinaryInfo, decodePublishBinary, PUBLISH_BINARY_HEADER }
export type { WirePublishInfo }

import { assertProtocol } from './protocolViolation.js'

/** Ordering metadata embedded in PUBLISH frames on the wire. */
type WirePublishInfo = { seq: number; timestamp: number }

// ===== Publish info helpers =====
// Format: seq,timestamp\n{serialized text}
// JSON.stringify never produces bare \n, so the first \n reliably splits info from payload.

function encodePublishText(text: string, info: WirePublishInfo): string {
  return info.seq + ',' + info.timestamp + '\n' + text
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

// Binary publish info: [seq as f64 LE, as a text publish's decimal][timestamp as f64 LE][binary data]

const PUBLISH_BINARY_HEADER = 16

function writePublishBinaryInfo(frame: Uint8Array<ArrayBuffer>, at: number, info: WirePublishInfo): void {
  const view = new DataView(frame.buffer, frame.byteOffset + at)
  view.setFloat64(0, info.seq, true)
  view.setFloat64(8, info.timestamp, true)
}

function decodePublishBinary(wire: Uint8Array): { data: Uint8Array; info: WirePublishInfo } {
  assertProtocol(wire.byteLength >= PUBLISH_BINARY_HEADER, 'PUBLISH_BINARY frame too short for info header')
  const view = new DataView(wire.buffer, wire.byteOffset, wire.byteLength)
  const seq = view.getFloat64(0, true)
  const timestamp = view.getFloat64(8, true)
  assertProtocol(Number.isFinite(seq) && Number.isFinite(timestamp), 'PUBLISH_BINARY frame info must be finite numbers')
  return { data: wire.subarray(PUBLISH_BINARY_HEADER), info: { seq, timestamp } }
}
