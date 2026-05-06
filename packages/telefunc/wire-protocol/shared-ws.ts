export {
  TAG,
  ACK_STATUS,
  encode,
  decode,
  isChannelCtrlTag,
  isConnCtrlTag,
  encodePublishText,
  decodePublishText,
  encodePublishBinary,
  decodePublishBinary,
}
export type {
  AckResultStatus,
  DecodedFrame,
  ChannelFrame,
  ChannelCtrlFrame,
  ChannelDataFrame,
  ReconcilePayload,
  ReconciledPayload,
  WirePublishInfo,
}

import { assert } from '../utils/assert.js'
import type { ChannelTransports } from './constants.js'

// ===== Wire protocol =====
//
// Every frame has a uniform 7-byte header:
//   [u8 tag][u16 LE index][u32 LE seq][payload...]
//
// `tag` discriminates the frame variant (data, connection ctrl, per-channel ctrl).
// `index` is the channel ix for per-channel frames; 0 for connection-level frames.
// `seq` is the replay sequence number for sequenced data frames; 0 for ctrl frames.
//
// Tag layout — sparse ranges so range checks classify:
//   0x01–0x09  connection-level control (no ix, no seq)
//   0x10–0x29  data plane (carries seq, payload varies)
//   0x30–      per-channel control (carries ix, no seq)
//
// Channel indices are client-owned and stable for the channel's lifetime.
// Sequence numbers are sender-assigned for replayable data frames in both directions.
// Each side tracks the highest seq received and replays after reconnect via reconcile.

const HEADER = 7
const DATA_TAG_MIN = 0x10
const CHANNEL_CTRL_TAG_MIN = 0x30

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

// ===== Tags =====

const TAG = {
  // ─── Connection-level control (no ix, no seq) ───
  PING: 0x01 as const,
  PONG: 0x02 as const,
  /** Server → client on old transport after upgrade drain: signals last frame on this transport. */
  FIN: 0x03 as const,
  /** Client → server on every (re)connect. JSON payload (`ReconcilePayload`). */
  RECONCILE: 0x04 as const,
  /** Server → client after reconcile. JSON payload (`ReconciledPayload`). */
  RECONCILED: 0x05 as const,
  /** Server → client wire-probe ack. Sent over the SSE downstream after the long-lived
   *  client→server stream-request POST's metadata has been parsed by the receiving server
   *  (forwarded via substrate if it lands on a non-owner). The client awaits this within
   *  `STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS` before declaring the transport open — confirms
   *  the half-duplex streaming wire round-trips end-to-end. */
  STREAM_REQUEST_OPEN_ACK: 0x06 as const,

  // ─── Data plane ───
  TEXT: 0x10 as const,
  BINARY: 0x11 as const,
  TEXT_ACK_REQ: 0x12 as const,
  BINARY_ACK_REQ: 0x13 as const,
  /** ACK response — carries `ackedSeq` + serialized result. */
  ACK_RES: 0x14 as const,
  /** Replayable publish frame delivered to keyed-channel subscribers. */
  PUBLISH: 0x15 as const,
  /** Replayable keyed publish frame that requests an acknowledgement receipt. */
  PUBLISH_ACK_REQ: 0x16 as const,
  /** Replayable binary publish frame delivered to keyed-channel subscribers. */
  PUBLISH_BINARY: 0x17 as const,
  /** Replayable keyed binary publish frame that requests an acknowledgement receipt. */
  PUBLISH_BINARY_ACK_REQ: 0x18 as const,

  // ─── Per-channel control (carries ix, no seq) ───
  CLOSE: 0x30 as const,
  CLOSE_ACK: 0x31 as const,
  /** Server → client: channel closed with an abort value (analogous to `throw Abort()`). */
  ABORT: 0x32 as const,
  /** Server → client: channel closed due to an unhandled server error (no payload). */
  ERROR: 0x33 as const,
  /** Flow-control window update: peer has consumed N bytes, may send N more. */
  WINDOW: 0x34 as const,
  BROADCAST_SUB: 0x35 as const,
  BROADCAST_UNSUB: 0x36 as const,
}

function isConnCtrlTag(tag: number): boolean {
  return tag < DATA_TAG_MIN
}

function isChannelCtrlTag(tag: number): boolean {
  return tag >= CHANNEL_CTRL_TAG_MIN
}

// ===== Reconcile payloads (JSON-encoded after the header) =====

type ReconcilePayload = {
  sessionId?: string
  /** Set when the client is upgrading from SSE to WS and has kept SSE alive.
   *  Server should drain the SSE send chain before replaying and attaching. */
  upgrade?: true
  /** Cluster-stable identifier of the previous transport's wire (the SSE owner) when this
   *  reconcile may land on a different instance than the one currently holding that wire.
   *  The receiving instance uses it as the cluster directory key (`locateConnection`) to
   *  verify `sessionId` and to RPC the previous wire's drain+fin to its owner. Always set
   *  by the client when a previous wire exists — server skips the lookup on local hits. */
  prevConnId?: string
  /** `initial: true` means this is the first reconcile for that channel — the server may
   *  not have created it yet (late-creation race during request body parse), so the server
   *  should wait up to `connectTtl` for it. Established channels (already reconciled at
   *  least once) omit `initial`; the server fails them fast if they're missing rather than
   *  stalling the entire reconcile. */
  open: { id: string; ix: number; lastSeq: number; initial?: true }[]
}

/** All channels the server actually attached, with the cluster instance hosting each
 *  channel's runtime state (`home`) and the instance holding the SSE response wire
 *  (`ownerInstance`). The client mirrors these into every subsequent data-POST metadata
 *  so the receiver can route per-frame from a 1-byte alias alone — no cluster lookup,
 *  no frame decoding. Alias 0 routes to `ownerInstance` (connection-level frames);
 *  alias N (≥ 1) routes to `open[N − 1]` (`home` + `id`). */
type ReconciledPayload = {
  sessionId: string
  open: { id: string; ix: number; lastSeq: number; home: string }[]
  ownerInstance: string
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  clientReplayBuffer: number
  clientReplayBufferBinary: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  transports: ChannelTransports
}

/** Ack result outcome on the wire — same byte value in memory and on the wire.
 *  - `OK`: `text` is the serialized ack value.
 *  - `ERROR`: a generic listener/channel error; `text` is the user-facing message.
 *  - `ABORT`: `text` is the serialized abort value.
 *  - `SHIELD_ERROR`: a shield validator rejected the data/ack; `text` is the validator message.
 *    Kept distinct from `ERROR` so the receiving side can throw `ShieldValidationError`. */
const ACK_STATUS = {
  OK: 0x00 as const,
  ERROR: 0x01 as const,
  ABORT: 0x02 as const,
  SHIELD_ERROR: 0x03 as const,
}

type AckResultStatus = (typeof ACK_STATUS)[keyof typeof ACK_STATUS]

/** Ordering metadata embedded in PUBLISH frames on the wire. */
type WirePublishInfo = { seq: number; ts: number }

// ===== Decoded frame =====

type ChannelDataFrame =
  | { tag: typeof TAG.TEXT; index: number; seq: number; text: string }
  | { tag: typeof TAG.BINARY; index: number; seq: number; data: Uint8Array }
  | { tag: typeof TAG.TEXT_ACK_REQ; index: number; seq: number; text: string }
  | { tag: typeof TAG.BINARY_ACK_REQ; index: number; seq: number; data: Uint8Array }
  | { tag: typeof TAG.ACK_RES; index: number; seq: number; ackedSeq: number; status: AckResultStatus; text: string }
  | { tag: typeof TAG.PUBLISH; index: number; seq: number; text: string; info: WirePublishInfo }
  | { tag: typeof TAG.PUBLISH_ACK_REQ; index: number; seq: number; text: string }
  | { tag: typeof TAG.PUBLISH_BINARY; index: number; seq: number; data: Uint8Array; info: WirePublishInfo }
  | { tag: typeof TAG.PUBLISH_BINARY_ACK_REQ; index: number; seq: number; data: Uint8Array }

type ChannelCtrlFrame =
  | { tag: typeof TAG.CLOSE; index: number; timeoutMs: number }
  | { tag: typeof TAG.CLOSE_ACK; index: number }
  | { tag: typeof TAG.ABORT; index: number; abortValue: string }
  | { tag: typeof TAG.ERROR; index: number }
  | { tag: typeof TAG.WINDOW; index: number; bytes: number }
  | { tag: typeof TAG.BROADCAST_SUB; index: number; binary: boolean }
  | { tag: typeof TAG.BROADCAST_UNSUB; index: number; binary: boolean }

/** Frames that carry an `index` (channel ix) — both data and per-channel ctrl. */
type ChannelFrame = ChannelDataFrame | ChannelCtrlFrame

type ConnCtrlFrame =
  | { tag: typeof TAG.PING }
  | { tag: typeof TAG.PONG }
  | { tag: typeof TAG.FIN }
  | { tag: typeof TAG.RECONCILE; payload: ReconcilePayload }
  | { tag: typeof TAG.RECONCILED; payload: ReconciledPayload }
  | { tag: typeof TAG.STREAM_REQUEST_OPEN_ACK }

type DecodedFrame = ChannelFrame | ConnCtrlFrame

// ===== Encode =====

function writeHeader(frame: Uint8Array, tag: number, index: number, seq: number): void {
  frame[0] = tag
  frame[1] = index & 0xff
  frame[2] = (index >> 8) & 0xff
  frame[3] = seq & 0xff
  frame[4] = (seq >> 8) & 0xff
  frame[5] = (seq >> 16) & 0xff
  frame[6] = (seq >> 24) & 0xff
}

function writeU32(frame: Uint8Array, offset: number, n: number): void {
  frame[offset] = n & 0xff
  frame[offset + 1] = (n >> 8) & 0xff
  frame[offset + 2] = (n >> 16) & 0xff
  frame[offset + 3] = (n >> 24) & 0xff
}

function readU32(buf: Uint8Array, offset: number): number {
  return (
    (buf[offset] as number) |
    ((buf[offset + 1] as number) << 8) |
    ((buf[offset + 2] as number) << 16) |
    ((buf[offset + 3] as number) << 24)
  )
}

function encodeTextFrame(tag: number, index: number, text: string, seq: number): Uint8Array<ArrayBuffer> {
  const payload = textEncoder.encode(text)
  const frame = new Uint8Array(HEADER + payload.byteLength)
  writeHeader(frame, tag, index, seq)
  frame.set(payload, HEADER)
  return frame
}

function encodeBinaryFrame(tag: number, index: number, data: Uint8Array, seq: number): Uint8Array<ArrayBuffer> {
  const frame = new Uint8Array(HEADER + data.byteLength)
  writeHeader(frame, tag, index, seq)
  frame.set(data, HEADER)
  return frame
}

function encodeBareFrame(tag: number, index = 0): Uint8Array<ArrayBuffer> {
  const frame = new Uint8Array(HEADER)
  writeHeader(frame, tag, index, 0)
  return frame
}

function encodeJsonFrame(tag: number, payload: unknown): Uint8Array<ArrayBuffer> {
  const json = textEncoder.encode(JSON.stringify(payload))
  const frame = new Uint8Array(HEADER + json.byteLength)
  writeHeader(frame, tag, 0, 0)
  frame.set(json, HEADER)
  return frame
}

const encode = {
  text: (index: number, text: string, seq = 0) => encodeTextFrame(TAG.TEXT, index, text, seq),
  publish: (index: number, text: string, seq = 0) => encodeTextFrame(TAG.PUBLISH, index, text, seq),
  publishAckReq: (index: number, text: string, seq = 0) => encodeTextFrame(TAG.PUBLISH_ACK_REQ, index, text, seq),
  textAckReq: (index: number, text: string, seq = 0) => encodeTextFrame(TAG.TEXT_ACK_REQ, index, text, seq),

  binary: (index: number, data: Uint8Array, seq = 0) => encodeBinaryFrame(TAG.BINARY, index, data, seq),
  publishBinary: (index: number, data: Uint8Array, seq = 0) => encodeBinaryFrame(TAG.PUBLISH_BINARY, index, data, seq),
  publishBinaryAckReq: (index: number, data: Uint8Array, seq = 0) =>
    encodeBinaryFrame(TAG.PUBLISH_BINARY_ACK_REQ, index, data, seq),
  binaryAckReq: (index: number, data: Uint8Array, seq = 0) => encodeBinaryFrame(TAG.BINARY_ACK_REQ, index, data, seq),

  /** Wire: [header][u32 ackedSeq][u8 status][result bytes...]
   *  `ownSeq` — this frame's own replay sequence number.
   *  `ackedSeq` — the seq of the ACK_REQ frame being acknowledged. */
  ackRes(
    index: number,
    ownSeq: number,
    ackedSeq: number,
    result: string,
    status: AckResultStatus = ACK_STATUS.OK,
  ): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(result)
    const frame = new Uint8Array(HEADER + 5 + payload.byteLength)
    writeHeader(frame, TAG.ACK_RES, index, ownSeq)
    writeU32(frame, HEADER, ackedSeq)
    frame[HEADER + 4] = status
    frame.set(payload, HEADER + 5)
    return frame
  },

  // ── Connection-level ctrls ──
  ping: () => encodeBareFrame(TAG.PING),
  pong: () => encodeBareFrame(TAG.PONG),
  fin: () => encodeBareFrame(TAG.FIN),
  reconcile: (payload: ReconcilePayload) => encodeJsonFrame(TAG.RECONCILE, payload),
  reconciled: (payload: ReconciledPayload) => encodeJsonFrame(TAG.RECONCILED, payload),
  streamRequestOpenAck: () => encodeBareFrame(TAG.STREAM_REQUEST_OPEN_ACK),

  // ── Per-channel ctrls ──
  close(index: number, timeoutMs: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4)
    writeHeader(frame, TAG.CLOSE, index, 0)
    writeU32(frame, HEADER, timeoutMs)
    return frame
  },
  closeAck: (index: number) => encodeBareFrame(TAG.CLOSE_ACK, index),
  abort(index: number, abortValue: string): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(abortValue)
    const frame = new Uint8Array(HEADER + payload.byteLength)
    writeHeader(frame, TAG.ABORT, index, 0)
    frame.set(payload, HEADER)
    return frame
  },
  error: (index: number) => encodeBareFrame(TAG.ERROR, index),
  window(index: number, bytes: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4)
    writeHeader(frame, TAG.WINDOW, index, 0)
    writeU32(frame, HEADER, bytes)
    return frame
  },
  broadcastSub(index: number, binary: boolean): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 1)
    writeHeader(frame, TAG.BROADCAST_SUB, index, 0)
    frame[HEADER] = binary ? 1 : 0
    return frame
  },
  broadcastUnsub(index: number, binary: boolean): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 1)
    writeHeader(frame, TAG.BROADCAST_UNSUB, index, 0)
    frame[HEADER] = binary ? 1 : 0
    return frame
  },
}

// ===== Decode =====

function decode(frame: Uint8Array): DecodedFrame {
  assert(frame.length >= HEADER, 'frame too short')
  const tag = frame[0] as number
  const index = (frame[1] as number) | ((frame[2] as number) << 8)
  const seq = readU32(frame, 3)
  const payload = frame.subarray(HEADER)

  switch (tag) {
    case TAG.TEXT:
      return { tag: TAG.TEXT, index, seq, text: textDecoder.decode(payload) }
    case TAG.BINARY:
      return { tag: TAG.BINARY, index, seq, data: payload }
    case TAG.TEXT_ACK_REQ:
      return { tag: TAG.TEXT_ACK_REQ, index, seq, text: textDecoder.decode(payload) }
    case TAG.BINARY_ACK_REQ:
      return { tag: TAG.BINARY_ACK_REQ, index, seq, data: payload }
    case TAG.PUBLISH: {
      const { text, info } = decodePublishText(textDecoder.decode(payload))
      return { tag: TAG.PUBLISH, index, seq, text, info }
    }
    case TAG.PUBLISH_ACK_REQ:
      return { tag: TAG.PUBLISH_ACK_REQ, index, seq, text: textDecoder.decode(payload) }
    case TAG.PUBLISH_BINARY: {
      const { data, info } = decodePublishBinary(payload)
      return { tag: TAG.PUBLISH_BINARY, index, seq, data, info }
    }
    case TAG.PUBLISH_BINARY_ACK_REQ:
      return { tag: TAG.PUBLISH_BINARY_ACK_REQ, index, seq, data: payload }
    case TAG.ACK_RES: {
      assert(payload.length >= 5, 'ACK_RES payload too short')
      const ackedSeq = readU32(payload, 0)
      const status = payload[4] as number
      assert(
        status === ACK_STATUS.OK ||
          status === ACK_STATUS.ERROR ||
          status === ACK_STATUS.ABORT ||
          status === ACK_STATUS.SHIELD_ERROR,
        `ACK_RES unknown status ${status}`,
      )
      return { tag: TAG.ACK_RES, index, seq, ackedSeq, status, text: textDecoder.decode(payload.subarray(5)) }
    }

    case TAG.PING:
      return { tag: TAG.PING }
    case TAG.PONG:
      return { tag: TAG.PONG }
    case TAG.FIN:
      return { tag: TAG.FIN }
    case TAG.RECONCILE:
      return { tag: TAG.RECONCILE, payload: JSON.parse(textDecoder.decode(payload)) as ReconcilePayload }
    case TAG.RECONCILED:
      return { tag: TAG.RECONCILED, payload: JSON.parse(textDecoder.decode(payload)) as ReconciledPayload }
    case TAG.STREAM_REQUEST_OPEN_ACK:
      return { tag: TAG.STREAM_REQUEST_OPEN_ACK }

    case TAG.CLOSE:
      assert(payload.length >= 4, 'CLOSE payload too short')
      return { tag: TAG.CLOSE, index, timeoutMs: readU32(payload, 0) }
    case TAG.CLOSE_ACK:
      return { tag: TAG.CLOSE_ACK, index }
    case TAG.ABORT:
      return { tag: TAG.ABORT, index, abortValue: textDecoder.decode(payload) }
    case TAG.ERROR:
      return { tag: TAG.ERROR, index }
    case TAG.WINDOW:
      assert(payload.length >= 4, 'WINDOW payload too short')
      return { tag: TAG.WINDOW, index, bytes: readU32(payload, 0) }
    case TAG.BROADCAST_SUB:
      assert(payload.length >= 1, 'BROADCAST_SUB payload too short')
      return { tag: TAG.BROADCAST_SUB, index, binary: payload[0] === 1 }
    case TAG.BROADCAST_UNSUB:
      assert(payload.length >= 1, 'BROADCAST_UNSUB payload too short')
      return { tag: TAG.BROADCAST_UNSUB, index, binary: payload[0] === 1 }

    default:
      assert(false, `Unknown wire frame tag ${tag}`)
  }
}

// ===== Publish info helpers =====
// Format: seq,ts\n{serialized text}
// JSON.stringify never produces bare \n, so the first \n reliably splits info from payload.

function encodePublishText(text: string, info: WirePublishInfo): string {
  return info.seq + ',' + info.ts + '\n' + text
}

function decodePublishText(wire: string): { text: string; info: WirePublishInfo } {
  const nl = wire.indexOf('\n')
  assert(nl !== -1, 'PUBLISH frame missing info prefix')
  const comma = wire.indexOf(',')
  assert(comma !== -1 && comma < nl, 'PUBLISH frame malformed info prefix')
  const seq = Number(wire.slice(0, comma))
  const ts = Number(wire.slice(comma + 1, nl))
  assert(Number.isFinite(seq) && Number.isFinite(ts), 'PUBLISH frame info must be finite numbers')
  return { text: wire.slice(nl + 1), info: { seq, ts } }
}

// ===== Binary publish info helpers =====
// Format: [4 bytes: seq as u32 LE][8 bytes: ts as f64 LE][binary data]

const PUBLISH_BINARY_HEADER = 12

function encodePublishBinary(data: Uint8Array, info: WirePublishInfo): Uint8Array {
  const result = new Uint8Array(PUBLISH_BINARY_HEADER + data.byteLength)
  const view = new DataView(result.buffer)
  view.setUint32(0, info.seq, true)
  view.setFloat64(4, info.ts, true)
  result.set(data, PUBLISH_BINARY_HEADER)
  return result
}

function decodePublishBinary(wire: Uint8Array): { data: Uint8Array; info: WirePublishInfo } {
  assert(wire.byteLength >= PUBLISH_BINARY_HEADER, 'PUBLISH_BINARY frame too short for info header')
  const view = new DataView(wire.buffer, wire.byteOffset, wire.byteLength)
  const seq = view.getUint32(0, true)
  const ts = view.getFloat64(4, true)
  assert(Number.isFinite(seq) && Number.isFinite(ts), 'PUBLISH_BINARY frame info must be finite numbers')
  return { data: wire.subarray(PUBLISH_BINARY_HEADER), info: { seq, ts } }
}
