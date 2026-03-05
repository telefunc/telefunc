export { TAG, encode, decode, encodeCtrl }
export type { CtrlMessage }

import { assert } from '../utils/assert.js'

// ===== WebSocket Wire Protocol =====
//
// Every message is binary. Uniform 3-byte header:
//
//   [u8 tag][u16 LE index][payload...]
//
//   TAG_CTRL   (0x01) — lifecycle & flow control, index unused (0)
//   TAG_TEXT   (0x02) — user channel text data, index = channel
//   TAG_BINARY (0x03) — user channel binary data, index = channel
//
// Channel indices are client-owned and stable for the channel's lifetime.

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

// ===== Tags =====

const TAG = {
  CTRL: 0x01 as const,
  TEXT: 0x02 as const,
  BINARY: 0x03 as const,
}

// ===== Control message types =====

type CtrlClose = { t: 'close'; ix: number }
type CtrlPause = { t: 'pause'; ix: number }
type CtrlResume = { t: 'resume'; ix: number }
type CtrlPing = { t: 'ping' }
type CtrlPong = { t: 'pong' }
/** Client → server on every (re)connect: all open channels with client-owned indices. */
type CtrlReconcile = { t: 'reconcile'; open: { id: string; ix: number }[] }
/** Server → client after reconcile: all channels the server actually attached. */
type CtrlReconciled = { t: 'reconciled'; open: { id: string; ix: number }[] }
type CtrlMessage = CtrlClose | CtrlPause | CtrlResume | CtrlPing | CtrlPong | CtrlReconcile | CtrlReconciled

// ===== Decoded frame =====

type DecodedFrame =
  | { tag: typeof TAG.CTRL; ctrl: CtrlMessage }
  | { tag: typeof TAG.TEXT; index: number; text: string }
  | { tag: typeof TAG.BINARY; index: number; data: Uint8Array }

// ===== Encode =====

const encode = {
  ctrl(msg: CtrlMessage): Uint8Array<ArrayBuffer> {
    const json = textEncoder.encode(JSON.stringify(msg))
    const frame = new Uint8Array(3 + json.byteLength)
    frame[0] = TAG.CTRL
    frame.set(json, 3)
    return frame
  },

  text(index: number, text: string): Uint8Array<ArrayBuffer> {
    const payload = textEncoder.encode(text)
    const frame = new Uint8Array(3 + payload.byteLength)
    frame[0] = TAG.TEXT
    frame[1] = index & 0xff
    frame[2] = (index >> 8) & 0xff
    frame.set(payload, 3)
    return frame
  },

  binary(index: number, data: Uint8Array): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(3 + data.byteLength)
    frame[0] = TAG.BINARY
    frame[1] = index & 0xff
    frame[2] = (index >> 8) & 0xff
    frame.set(data, 3)
    return frame
  },
}

/** Shorthand for encode.ctrl — used frequently in transport code. */
const encodeCtrl = encode.ctrl

// ===== Decode =====

function decode(frame: Uint8Array): DecodedFrame {
  assert(frame.length >= 3, 'frame too short')
  const tag = frame[0] as number
  const index = (frame[1] as number) | ((frame[2] as number) << 8)
  const payload = frame.subarray(3)

  if (tag === TAG.CTRL) {
    return { tag: TAG.CTRL, ctrl: JSON.parse(textDecoder.decode(payload)) }
  }
  if (tag === TAG.TEXT) {
    return { tag: TAG.TEXT, index, text: textDecoder.decode(payload) }
  }
  return { tag: TAG.BINARY, index, data: payload }
}
