export {
  TAG,
  encode,
  decode,
  encodeCtrl,
  encodePublishText,
  decodePublishText,
  encodePublishBinary,
  decodePublishBinary,
}
export type { AckResultStatus, CtrlMessage, CtrlReconcile, CtrlReconciled, DecodedFrame, WirePublishInfo }

import { assert } from '../utils/assert.js'
import type { ChannelTransports } from './constants.js'

// ===== WebSocket Wire Protocol =====
//
// Every message is binary.
//
// CTRL frames (lifecycle & flow control) — 3-byte header, no seq:
//   [u8 TAG_CTRL][u16 LE 0][JSON payload...]
//
// Data frames (TEXT / PUBLISH / BINARY) — 7-byte header with sequence number:
//   [u8 tag][u16 LE index][u32 LE seq][payload...]
//
//   TAG_CTRL   (0x01) — lifecycle & flow control, index unused (0)
//   TAG_TEXT   (0x02) — user channel text data, index = channel
//   TAG_BINARY (0x03) — user channel binary data, index = channel
//   TAG_PUBLISH (0x06) — keyed-channel pub/sub text data, index = channel
//   TAG_PUBLISH_ACK_REQ (0x07) — keyed-channel pub/sub text data that requests an ACK_RES receipt
//
// Sequence numbers are assigned by the sender for replayable data frames in
// both directions. Each side tracks the highest seq received and sends it in
// reconcile so the peer can replay missed data after reconnect.
// CTRL frames remain unsequenced.
//
// Channel indices are client-owned and stable for the channel's lifetime.

const HEADER_CTRL = 3
const HEADER_DATA = 7

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

// ===== Tags =====

const TAG = {
  CTRL: 0x01 as const,
  TEXT: 0x02 as const,
  BINARY: 0x03 as const,
  /** ACK response — carries receiver's ack_seq + serialized result. */
  ACK_RES: 0x04 as const,
  /** Text frame that requests an acknowledgement from the receiver. */
  TEXT_ACK_REQ: 0x05 as const,
  /** Replayable publish frame delivered to keyed-channel subscribers. */
  PUBLISH: 0x06 as const,
  /** Replayable keyed publish frame that requests an acknowledgement receipt. */
  PUBLISH_ACK_REQ: 0x07 as const,
  /** Replayable binary publish frame delivered to keyed-channel subscribers. */
  PUBLISH_BINARY: 0x08 as const,
  /** Replayable keyed binary publish frame that requests an acknowledgement receipt. */
  PUBLISH_BINARY_ACK_REQ: 0x09 as const,
  /** Binary frame that requests an acknowledgement from the receiver. */
  BINARY_ACK_REQ: 0x0a as const,
}

// ===== Control message types =====

type CtrlClose = { t: 'close'; ix: number; timeoutMs: number }
type CtrlCloseAck = { t: 'close-ack'; ix: number }
type CtrlWindow = { t: 'window'; ix: number; bytes: number }
type CtrlPing = { t: 'ping' }
type CtrlPong = { t: 'pong' }
/** Server → client: channel closed with an abort value (analogous to throw Abort() in streaming). */
type CtrlAbort = { t: 'abort'; ix: number; abortValue: string }
/** Server → client: channel closed due to an unhandled server error (no details sent, analogous to BUG in streaming). */
type CtrlError = { t: 'error'; ix: number }
/** Client → server on every (re)connect: all open channels with client-owned indices. */
type CtrlReconcile = {
  t: 'reconcile'
  sessionId?: string
  /** Set when the client is upgrading from SSE to WS and has kept SSE alive.
   *  Server should drain the SSE send chain before replaying and attaching. */
  upgrade?: true
  open: { id: string; ix: number; lastSeq: number; defer?: boolean }[]
}
/** Server → client after reconcile: all channels the server actually attached, with lastSeq the server received per channel. */
type CtrlReconciled = {
  t: 'reconciled'
  sessionId: string
  open: { id: string; ix: number; lastSeq: number }[]
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  clientReplayBuffer: number
  clientReplayBufferBinary: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  transports: ChannelTransports
}
/** Server → client on old transport after upgrade drain: signals the client that this is the last frame on this transport. */
type CtrlFin = { t: 'fin' }
/** Client → server: subscribe this channel to pubsub. binary flag selects text or binary pubsub. */
type CtrlPubSubSub = { t: 'pubsub-sub'; ix: number; binary?: true }
/** Client → server: unsubscribe this channel from pubsub. */
type CtrlPubSubUnsub = { t: 'pubsub-unsub'; ix: number; binary?: true }
type CtrlMessage =
  | CtrlClose
  | CtrlCloseAck
  | CtrlAbort
  | CtrlError
  | CtrlWindow
  | CtrlPing
  | CtrlPong
  | CtrlReconcile
  | CtrlReconciled
  | CtrlFin
  | CtrlPubSubSub
  | CtrlPubSubUnsub

type AckResultStatus = 'ok' | 'error' | 'abort'

/** Ordering metadata embedded in PUBLISH frames on the wire. */
type WirePublishInfo = { seq: number; ts: number }

// ===== Decoded frame =====

type DecodedFrame =
  | { tag: typeof TAG.CTRL; ctrl: CtrlMessage }
  | { tag: typeof TAG.TEXT; index: number; seq: number; text: string }
  | { tag: typeof TAG.PUBLISH; index: number; seq: number; text: string; info: WirePublishInfo }
  | { tag: typeof TAG.PUBLISH_ACK_REQ; index: number; seq: number; text: string }
  | { tag: typeof TAG.BINARY; index: number; seq: number; data: Uint8Array }
  | { tag: typeof TAG.ACK_RES; index: number; seq: number; ackedSeq: number; status: AckResultStatus; text: string }
  | { tag: typeof TAG.TEXT_ACK_REQ; index: number; seq: number; text: string }
  | { tag: typeof TAG.PUBLISH_BINARY; index: number; seq: number; data: Uint8Array; info: WirePublishInfo }
  | { tag: typeof TAG.PUBLISH_BINARY_ACK_REQ; index: number; seq: number; data: Uint8Array }
  | { tag: typeof TAG.BINARY_ACK_REQ; index: number; seq: number; data: Uint8Array }

const ACK_RESULT_STATUS = {
  ok: 0x00,
  error: 0x01,
  abort: 0x02,
} as const

function encodeAckResultStatus(status: AckResultStatus): number {
  switch (status) {
    case 'ok':
      return ACK_RESULT_STATUS.ok
    case 'error':
      return ACK_RESULT_STATUS.error
    case 'abort':
      return ACK_RESULT_STATUS.abort
  }
}

function decodeAckResultStatus(value: number): AckResultStatus {
  switch (value) {
    case ACK_RESULT_STATUS.ok:
      return 'ok'
    case ACK_RESULT_STATUS.error:
      return 'error'
    case ACK_RESULT_STATUS.abort:
      return 'abort'
    default:
      assert(false, 'ACK_RES status is invalid')
  }
}

// ===== Encode =====

function writeDataHeader(frame: Uint8Array, tag: number, index: number, seq: number): void {
  frame[0] = tag
  frame[1] = index & 0xff
  frame[2] = (index >> 8) & 0xff
  frame[3] = seq & 0xff
  frame[4] = (seq >> 8) & 0xff
  frame[5] = (seq >> 16) & 0xff
  frame[6] = (seq >> 24) & 0xff
}

const encode = {
  ctrl(msg: CtrlMessage): Uint8Array<ArrayBuffer> {
    const json = textEncoder.encode(JSON.stringify(msg))
    const frame = new Uint8Array(HEADER_CTRL + json.byteLength)
    frame[0] = TAG.CTRL
    frame.set(json, HEADER_CTRL)
    return frame
  },

  text(index: number, text: string, seq = 0): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(text)
    const frame = new Uint8Array(HEADER_DATA + payload.byteLength)
    writeDataHeader(frame, TAG.TEXT, index, seq)
    frame.set(payload, HEADER_DATA)
    return frame
  },

  publish(index: number, text: string, seq = 0): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(text)
    const frame = new Uint8Array(HEADER_DATA + payload.byteLength)
    writeDataHeader(frame, TAG.PUBLISH, index, seq)
    frame.set(payload, HEADER_DATA)
    return frame
  },

  publishAckReq(index: number, text: string, seq = 0): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(text)
    const frame = new Uint8Array(HEADER_DATA + payload.byteLength)
    writeDataHeader(frame, TAG.PUBLISH_ACK_REQ, index, seq)
    frame.set(payload, HEADER_DATA)
    return frame
  },

  binary(index: number, data: Uint8Array, seq = 0): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER_DATA + data.byteLength)
    writeDataHeader(frame, TAG.BINARY, index, seq)
    frame.set(data, HEADER_DATA)
    return frame
  },

  publishBinary(index: number, data: Uint8Array, seq = 0): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER_DATA + data.byteLength)
    writeDataHeader(frame, TAG.PUBLISH_BINARY, index, seq)
    frame.set(data, HEADER_DATA)
    return frame
  },

  publishBinaryAckReq(index: number, data: Uint8Array, seq = 0): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER_DATA + data.byteLength)
    writeDataHeader(frame, TAG.PUBLISH_BINARY_ACK_REQ, index, seq)
    frame.set(data, HEADER_DATA)
    return frame
  },

  /** Identical wire layout to BINARY, but signals to the receiver: send ACK_RES when done. */
  binaryAckReq(index: number, data: Uint8Array, seq = 0): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER_DATA + data.byteLength)
    writeDataHeader(frame, TAG.BINARY_ACK_REQ, index, seq)
    frame.set(data, HEADER_DATA)
    return frame
  },

  /** Identical wire layout to TEXT, but signals to the receiver: send ACK_RES when done. */
  textAckReq(index: number, text: string, seq = 0): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(text)
    const frame = new Uint8Array(HEADER_DATA + payload.byteLength)
    writeDataHeader(frame, TAG.TEXT_ACK_REQ, index, seq)
    frame.set(payload, HEADER_DATA)
    return frame
  },

  /** Acknowledgement response frame.
   *  Wire: [u8 TAG.ACK_RES][u16 LE index][u32 LE own_seq][u32 LE ack_seq][u8 status][result bytes...]
   *  `ownSeq`  — this frame's own replay sequence number.
   *  `ackedSeq` — the seq of the TEXT_ACK_REQ frame being acknowledged. */
  ackRes(
    index: number,
    ownSeq: number,
    ackedSeq: number,
    result: string,
    status: AckResultStatus = 'ok',
  ): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(result)
    const frame = new Uint8Array(HEADER_DATA + 5 + payload.byteLength)
    writeDataHeader(frame, TAG.ACK_RES, index, ownSeq)
    frame[7] = ackedSeq & 0xff
    frame[8] = (ackedSeq >> 8) & 0xff
    frame[9] = (ackedSeq >> 16) & 0xff
    frame[10] = (ackedSeq >> 24) & 0xff
    frame[11] = encodeAckResultStatus(status)
    frame.set(payload, HEADER_DATA + 5)
    return frame
  },
}

/** Shorthand for encode.ctrl — used frequently in transport code. */
const encodeCtrl = encode.ctrl

// ===== Decode =====

function decode(frame: Uint8Array): DecodedFrame {
  const tag = frame[0] as number

  if (tag === TAG.CTRL) {
    assert(frame.length >= HEADER_CTRL, 'CTRL frame too short')
    const payload = frame.subarray(HEADER_CTRL)
    return { tag: TAG.CTRL, ctrl: JSON.parse(textDecoder.decode(payload)) }
  }

  assert(frame.length >= HEADER_DATA, 'data frame too short')
  const index = (frame[1] as number) | ((frame[2] as number) << 8)
  const seq =
    (frame[3] as number) | ((frame[4] as number) << 8) | ((frame[5] as number) << 16) | ((frame[6] as number) << 24)
  const payload = frame.subarray(HEADER_DATA)

  if (tag === TAG.TEXT) {
    return { tag: TAG.TEXT, index, seq, text: textDecoder.decode(payload) }
  }
  if (tag === TAG.PUBLISH) {
    const { text, info } = decodePublishText(textDecoder.decode(payload))
    return { tag: TAG.PUBLISH, index, seq, text, info }
  }
  if (tag === TAG.PUBLISH_ACK_REQ) {
    return { tag: TAG.PUBLISH_ACK_REQ, index, seq, text: textDecoder.decode(payload) }
  }
  if (tag === TAG.PUBLISH_BINARY) {
    const { data, info } = decodePublishBinary(payload)
    return { tag: TAG.PUBLISH_BINARY, index, seq, data, info }
  }
  if (tag === TAG.PUBLISH_BINARY_ACK_REQ) {
    return { tag: TAG.PUBLISH_BINARY_ACK_REQ, index, seq, data: payload }
  }
  if (tag === TAG.TEXT_ACK_REQ) {
    return { tag: TAG.TEXT_ACK_REQ, index, seq, text: textDecoder.decode(payload) }
  }
  if (tag === TAG.ACK_RES) {
    assert(payload.length >= 5, 'ACK_RES frame payload too short')
    const ackedSeq =
      (payload[0] as number) |
      ((payload[1] as number) << 8) |
      ((payload[2] as number) << 16) |
      ((payload[3] as number) << 24)
    const status = decodeAckResultStatus(payload[4] as number)
    return { tag: TAG.ACK_RES, index, seq, ackedSeq, status, text: textDecoder.decode(payload.subarray(5)) }
  }
  if (tag === TAG.BINARY_ACK_REQ) {
    return { tag: TAG.BINARY_ACK_REQ, index, seq, data: payload }
  }
  return { tag: TAG.BINARY, index, seq, data: payload }
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
