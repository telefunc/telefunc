export { TAG, encode, decode, encodeCtrl }
export type { CtrlMessage }

import { assert } from '../utils/assert.js'

// ===== WebSocket Wire Protocol =====
//
// Every message is binary.
//
// CTRL frames (lifecycle & flow control) — 3-byte header, no seq:
//   [u8 TAG_CTRL][u16 LE 0][JSON payload...]
//
// Data frames (TEXT / BINARY) — 7-byte header with sequence number:
//   [u8 tag][u16 LE index][u32 LE seq][payload...]
//
//   TAG_CTRL   (0x01) — lifecycle & flow control, index unused (0)
//   TAG_TEXT   (0x02) — user channel text data, index = channel
//   TAG_BINARY (0x03) — user channel binary data, index = channel
//
// Sequence numbers are assigned by the server for server→client data frames.
// Client→server data frames use seq=0 (unused). The client tracks the highest
// seq received and sends it in reconcile for replay on reconnect.
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
}

// ===== Control message types =====

type CtrlClose = { t: 'close'; ix: number }
type CtrlPause = { t: 'pause'; ix: number }
type CtrlResume = { t: 'resume'; ix: number }
type CtrlPing = { t: 'ping' }
type CtrlPong = { t: 'pong' }
/** Server → client: channel closed with an abort value (analogous to throw Abort() in streaming). */
type CtrlAbort = { t: 'abort'; ix: number; abortValue: string }
/** Server → client: channel closed due to an unhandled server error (no details sent, analogous to BUG in streaming). */
type CtrlError = { t: 'error'; ix: number }
/** Client → server on every (re)connect: all open channels with client-owned indices. */
type CtrlReconcile = { t: 'reconcile'; open: { id: string; ix: number; lastSeq: number }[] }
/** Server → client after reconcile: all channels the server actually attached, with lastSeq the server received per channel. */
type CtrlReconciled = { t: 'reconciled'; open: { id: string; ix: number; lastSeq: number }[] }
type CtrlMessage =
  | CtrlClose
  | CtrlAbort
  | CtrlError
  | CtrlPause
  | CtrlResume
  | CtrlPing
  | CtrlPong
  | CtrlReconcile
  | CtrlReconciled

// ===== Decoded frame =====

type DecodedFrame =
  | { tag: typeof TAG.CTRL; ctrl: CtrlMessage }
  | { tag: typeof TAG.TEXT; index: number; seq: number; text: string }
  | { tag: typeof TAG.BINARY; index: number; seq: number; data: Uint8Array }
  | { tag: typeof TAG.ACK_RES; index: number; seq: number; ackedSeq: number; text: string }
  | { tag: typeof TAG.TEXT_ACK_REQ; index: number; seq: number; text: string }

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

  binary(index: number, data: Uint8Array, seq = 0): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER_DATA + data.byteLength)
    writeDataHeader(frame, TAG.BINARY, index, seq)
    frame.set(data, HEADER_DATA)
    return frame
  },

  /** Identical wire layout to TEXT, but signals to the receiver: send ACK_RES when done. */
  textAckReq(index: number, text: string, seq = 0): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(text)
    const frame = new Uint8Array(HEADER_DATA + payload.byteLength)
    writeDataHeader(frame, TAG.TEXT_ACK_REQ, index, seq)
    frame.set(payload, HEADER_DATA)
    return frame as Uint8Array<ArrayBuffer>
  },

  /** Acknowledgement response frame.
   *  Wire: [u8 TAG.ACK_RES][u16 LE index][u32 LE own_seq][u32 LE ack_seq][result bytes...]
   *  `ownSeq`  — this frame's own replay sequence number.
   *  `ackedSeq` — the seq of the TEXT_ACK_REQ frame being acknowledged. */
  ackRes(index: number, ownSeq: number, ackedSeq: number, result: string): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(result)
    const frame = new Uint8Array(HEADER_DATA + 4 + payload.byteLength)
    writeDataHeader(frame, TAG.ACK_RES, index, ownSeq)
    frame[7] = ackedSeq & 0xff
    frame[8] = (ackedSeq >> 8) & 0xff
    frame[9] = (ackedSeq >> 16) & 0xff
    frame[10] = (ackedSeq >> 24) & 0xff
    frame.set(payload, HEADER_DATA + 4)
    return frame as Uint8Array<ArrayBuffer>
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
  if (tag === TAG.TEXT_ACK_REQ) {
    return { tag: TAG.TEXT_ACK_REQ, index, seq, text: textDecoder.decode(payload) }
  }
  if (tag === TAG.ACK_RES) {
    assert(payload.length >= 4, 'ACK_RES frame payload too short')
    const ackedSeq =
      (payload[0] as number) |
      ((payload[1] as number) << 8) |
      ((payload[2] as number) << 16) |
      ((payload[3] as number) << 24)
    return { tag: TAG.ACK_RES, index, seq, ackedSeq, text: textDecoder.decode(payload.subarray(4)) }
  }
  return { tag: TAG.BINARY, index, seq, data: payload }
}
