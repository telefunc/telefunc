export {
  ACK_STATUS,
  ERROR_REASON,
  encode,
  withTook,
  MESSAGE_FRAME_OVERHEAD_MAX_BYTES,
  isOneFrame,
  messageFrames,
  lengthBytes,
  decode,
  decodeClientFrame,
  isSequencedFrame,
  peekTag,
}
export type {
  AckResultStatus,
  ErrorReason,
  DecodedFrame,
  ChannelFrame,
  ChannelCtrlFrame,
  ChannelDataFrame,
  PingEntry,
  PongEntry,
  PiecesAck,
  BdpPingAck,
  SequencedEncoder,
}

import { HEADER, SEQ_OFFSET, writeHeader, writeU16, writeU32, writeU64, readU16, readU32, readU64 } from './header.js'
import { TAG, isSequencedTag } from './tags.js'
import {
  parseBarrierPayload,
  parseJsonPayload,
  parsePreparePayload,
  parseReconcilePayload,
  type BarrierPayload,
  type PreparePayload,
  type ReadyPayload,
  type ReconcilePayload,
  type ReconciledPayload,
} from './payloads.js'
import {
  PUBLISH_BINARY_HEADER,
  decodePublishBinary,
  decodePublishText,
  encodePublishText,
  writePublishBinaryInfo,
  type WirePublishInfo,
} from './publishInfo.js'
import { ProtocolViolationError, assertProtocol } from './protocolViolation.js'
import { textDecoder, textEncoder } from './envelope.js'

const PING_ENTRY_BYTES = 6
const PONG_ENTRY_BYTES = 7
/** An ACK_RES's `ackedSeq` and status. */
const ACK_RES_PREFIX = 9
/** The most a frame adds to a channel message or its ack answer: an ACK_RES's header and prefix. */
const MESSAGE_FRAME_OVERHEAD_MAX_BYTES = HEADER + ACK_RES_PREFIX

/** A closed channel of the page's: the last seq the page has of what the server sent on it. */
type PingEntry = { ix: number; lastSeq: number }

/** The last seq the server has of what the page sent on that channel, or null: the server no longer holds it. */
type PongEntry = { ix: number; lastSeq: number | null }

/** Ack result outcome on the wire — same byte value in memory and on the wire.
 *  - `OK`: `text` is the serialized ack value.
 *  - `ERROR`: a generic listener/channel error; `text` is the user-facing message.
 *  - `ABORT`: `text` is the serialized abort value.
 *  - `SHIELD_ERROR`: a shield validator rejected the data/ack; `text` is the validator message.
 *    Kept distinct from `ERROR` so the receiving side can throw `ShieldValidationError`.
 *  - `OVERFLOW`: the server couldn't hold the page's ack request or its answer; no `text`. Server → page only. */
const ACK_STATUS = {
  OK: 0x00 as const,
  ERROR: 0x01 as const,
  ABORT: 0x02 as const,
  SHIELD_ERROR: 0x03 as const,
  OVERFLOW: 0x04 as const,
}

type AckResultStatus = (typeof ACK_STATUS)[keyof typeof ACK_STATUS]

/** Why the server closed a channel with `ERROR`. */
const ERROR_REASON = {
  /** An unhandled server error. */
  BUG: 0x00 as const,
  /** More was published to its broadcast while its page was offline than the server holds for it. */
  OVERFLOW: 0x01 as const,
  /** A reconnect needed frames its sender's replay buffer had dropped to stay within its size. */
  LOST: 0x02 as const,
}

type ErrorReason = (typeof ERROR_REASON)[keyof typeof ERROR_REASON]

// ===== Decoded frame =====

/** `bytes`: the whole frame, what flow control counts, so an empty message counts too. */
type ChannelDataFrame = { index: number; seq: number; bytes: number } & (
  | { tag: typeof TAG.TEXT; text: string }
  | { tag: typeof TAG.BINARY; data: Uint8Array }
  | { tag: typeof TAG.TEXT_ACK_REQ; text: string }
  | { tag: typeof TAG.BINARY_ACK_REQ; data: Uint8Array }
  | { tag: typeof TAG.ACK_RES; ackedSeq: number; status: AckResultStatus; text: string }
  | { tag: typeof TAG.PUBLISH; text: string; info: WirePublishInfo }
  | { tag: typeof TAG.PUBLISH_ACK_REQ; text: string }
  | { tag: typeof TAG.PUBLISH_BINARY; data: Uint8Array; info: WirePublishInfo }
  | { tag: typeof TAG.PUBLISH_BINARY_ACK_REQ; data: Uint8Array }
  | { tag: typeof TAG.PUBLISH_TO }
)

type ChannelCtrlFrame =
  | { tag: typeof TAG.CLOSE; index: number; seq: number; timeoutMs: number }
  | { tag: typeof TAG.CLOSE_ACK; index: number; seq: number }
  | { tag: typeof TAG.ABORT; index: number; seq: number; abortValue: string }
  | { tag: typeof TAG.ERROR; index: number; seq: number; reason: number }
  | { tag: typeof TAG.WINDOW; index: number; bytes: number; lastSeq: number }
  | { tag: typeof TAG.MSG_WINDOW; index: number; count: number }
  | { tag: typeof TAG.BROADCAST_SUB; index: number; binary: boolean }
  | { tag: typeof TAG.BROADCAST_UNSUB; index: number; binary: boolean }
  | { tag: typeof TAG.BDP_PING; index: number; probe: number }
  | ({ tag: typeof TAG.BDP_PING_ACK; index: number } & BdpPingAck)
  /** `lastSeq` is null when the channel wasn't attached. */
  | { tag: typeof TAG.ATTACH_RESULT; index: number; lastSeq: number | null }

/** Frames that carry an `index` (channel ix) — both data and per-channel ctrl. */
type ChannelFrame = ChannelDataFrame | ChannelCtrlFrame

type SequencedFrame =
  | ChannelDataFrame
  | Extract<ChannelCtrlFrame, { tag: typeof TAG.CLOSE | typeof TAG.CLOSE_ACK | typeof TAG.ABORT | typeof TAG.ERROR }>

type PiecesAck = { bytes: number; heldMs: number; spanUs: number }

/** `backlog` in bytes, `Infinity` where credit didn't run out; `pathRtt` in ms, `Infinity` where none was measured. */
type BdpPingAck = { probe: number; backlog: number; pathRtt: number }

type WireCtrlFrame =
  | { tag: typeof TAG.PING; ended: PingEntry[]; took: number }
  | { tag: typeof TAG.PONG; ended: PongEntry[]; took: number }
  | { tag: typeof TAG.FIN }
  | { tag: typeof TAG.RECONCILE; payload: ReconcilePayload }
  | { tag: typeof TAG.BARRIER; payload: BarrierPayload }
  | { tag: typeof TAG.RECONCILED; payload: ReconciledPayload }
  | { tag: typeof TAG.STREAM_REQUEST_OPEN_ACK }
  | { tag: typeof TAG.PREPARE; payload: PreparePayload }
  | { tag: typeof TAG.READY; payload: ReadyPayload }
  | { tag: typeof TAG.PIECE; total: number; piece: Uint8Array }
  | ({ tag: typeof TAG.PIECES_ACK } & PiecesAck)
  | { tag: typeof TAG.ANNOUNCE; bytes: number; floor: number }

type DecodedFrame = ChannelFrame | WireCtrlFrame

/** What a replay holds: data, and the closing frames. */
function isSequencedFrame(frame: DecodedFrame): frame is SequencedFrame {
  return isSequencedTag(frame.tag)
}

// ===== Encode =====

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

function encodeBareFrame(tag: number, index = 0, seq = 0): Uint8Array<ArrayBuffer> {
  const frame = new Uint8Array(HEADER)
  writeHeader(frame, tag, index, seq)
  return frame
}

function encodeJsonFrame(tag: number, payload: unknown): Uint8Array<ArrayBuffer> {
  const json = textEncoder.encode(JSON.stringify(payload))
  const frame = new Uint8Array(HEADER + json.byteLength)
  writeHeader(frame, tag, 0, 0)
  frame.set(json, HEADER)
  return frame
}

/** An encoder of a sequenced frame of `data`, as `encode.text`: the sender passes it by reference, so a send builds no
 *  closure. */
type SequencedEncoder<T> = (index: number, data: T, seq: number) => Uint8Array<ArrayBuffer>

const encode = {
  text: (index: number, text: string, seq: number) => encodeTextFrame(TAG.TEXT, index, text, seq),
  /** A publish's body, which names no channel: `publishTo` goes ahead of it on each subscriber's wire. */
  publish: (text: string, info: WirePublishInfo) => encodeTextFrame(TAG.PUBLISH, 0, encodePublishText(text, info), 0),
  publishAckReq: (index: number, text: string, seq: number) => encodeTextFrame(TAG.PUBLISH_ACK_REQ, index, text, seq),
  textAckReq: (index: number, text: string, seq: number) => encodeTextFrame(TAG.TEXT_ACK_REQ, index, text, seq),

  binary: (index: number, data: Uint8Array, seq: number) => encodeBinaryFrame(TAG.BINARY, index, data, seq),
  /** A binary publish's body, as `publish`. */
  publishBinary(data: Uint8Array, info: WirePublishInfo): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + PUBLISH_BINARY_HEADER + data.byteLength)
    writeHeader(frame, TAG.PUBLISH_BINARY, 0, 0)
    writePublishBinaryInfo(frame, HEADER, info)
    frame.set(data, HEADER + PUBLISH_BINARY_HEADER)
    return frame
  },
  publishTo: (index: number, seq: number) => encodeBareFrame(TAG.PUBLISH_TO, index, seq),
  publishBinaryAckReq: (index: number, data: Uint8Array, seq: number) =>
    encodeBinaryFrame(TAG.PUBLISH_BINARY_ACK_REQ, index, data, seq),
  binaryAckReq: (index: number, data: Uint8Array, seq: number) =>
    encodeBinaryFrame(TAG.BINARY_ACK_REQ, index, data, seq),

  /** Wire: [header][u64 ackedSeq][u8 status][result bytes...]
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
    const frame = new Uint8Array(HEADER + ACK_RES_PREFIX + payload.byteLength)
    writeHeader(frame, TAG.ACK_RES, index, ownSeq)
    writeU64(frame, HEADER, ackedSeq)
    frame[HEADER + 8] = status
    frame.set(payload, HEADER + ACK_RES_PREFIX)
    return frame
  },

  // ── Wire-level ctrls ──
  /** Wire: [header]([u16 ix][u32 lastSeq])* */
  ping(ended: PingEntry[] = []): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + PING_ENTRY_BYTES * ended.length)
    writeHeader(frame, TAG.PING, 0, 0)
    let offset = HEADER
    for (const { ix, lastSeq } of ended) {
      writeU16(frame, offset, ix)
      writeU32(frame, offset + 2, lastSeq)
      offset += PING_ENTRY_BYTES
    }
    return frame
  },
  /** Wire: [header]([u16 ix][u8 held][u32 lastSeq])* */
  pong(ended: PongEntry[] = []): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + PONG_ENTRY_BYTES * ended.length)
    writeHeader(frame, TAG.PONG, 0, 0)
    let offset = HEADER
    for (const { ix, lastSeq } of ended) {
      writeU16(frame, offset, ix)
      frame[offset + 2] = lastSeq === null ? 0 : 1
      writeU32(frame, offset + 3, lastSeq ?? 0)
      offset += PONG_ENTRY_BYTES
    }
    return frame
  },
  fin: () => encodeBareFrame(TAG.FIN),
  reconcile: (payload: ReconcilePayload) => encodeJsonFrame(TAG.RECONCILE, payload),
  barrier: (payload: BarrierPayload) => encodeJsonFrame(TAG.BARRIER, payload),
  reconciled: (payload: ReconciledPayload) => encodeJsonFrame(TAG.RECONCILED, payload),
  streamRequestOpenAck: () => encodeBareFrame(TAG.STREAM_REQUEST_OPEN_ACK),
  prepare: (payload: PreparePayload) => encodeJsonFrame(TAG.PREPARE, payload),
  ready: (payload: ReadyPayload) => encodeJsonFrame(TAG.READY, payload),
  piece(total: number, piece: Uint8Array): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4 + piece.byteLength)
    writeHeader(frame, TAG.PIECE, 0, 0)
    writeU32(frame, HEADER, total)
    frame.set(piece, HEADER + 4)
    return frame
  },
  piecesAck(bytes: number, heldMs: number, spanUs: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 12)
    writeHeader(frame, TAG.PIECES_ACK, 0, 0)
    writeU32(frame, HEADER, bytes)
    writeU32(frame, HEADER + 4, heldMs)
    writeU32(frame, HEADER + 8, spanUs)
    return frame
  },
  announce(bytes: number, floorPerS: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4)
    writeHeader(frame, TAG.ANNOUNCE, 0, floorPerS)
    writeU32(frame, HEADER, bytes)
    return frame
  },
  /** `frames`, of `bytes` in all with the LENGTH each before the last needs (`lengthBytes`), in one message. */
  message(frames: readonly Uint8Array[], bytes: number): Uint8Array<ArrayBuffer> {
    const message = new Uint8Array(bytes)
    const last = frames.length - 1
    let at = 0
    for (let i = 0; i <= last; i++) {
      const frame = frames[i]!
      if (i < last && lengthBytes(frame[0]!) > 0) {
        writeHeader(message.subarray(at), TAG.LENGTH, 0, frame.byteLength)
        at += HEADER
      }
      message.set(frame, at)
      at += frame.byteLength
    }
    return message
  },

  // ── Per-channel ctrls ──
  close(index: number, timeoutMs: number, seq: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4)
    writeHeader(frame, TAG.CLOSE, index, seq)
    writeU32(frame, HEADER, timeoutMs)
    return frame
  },
  closeAck: (index: number, seq: number) => encodeBareFrame(TAG.CLOSE_ACK, index, seq),
  abort: (index: number, abortValue: string, seq: number) => encodeTextFrame(TAG.ABORT, index, abortValue, seq),
  error(index: number, reason: ErrorReason, seq: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 1)
    writeHeader(frame, TAG.ERROR, index, seq)
    frame[HEADER] = reason
    return frame
  },
  /** Wire: [header][u32 limit][u32 lastSeq] */
  window(index: number, bytes: number, lastSeq: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 8)
    writeHeader(frame, TAG.WINDOW, index, 0)
    writeU32(frame, HEADER, bytes)
    writeU32(frame, HEADER + 4, lastSeq)
    return frame
  },
  msgWindow(index: number, count: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4)
    writeHeader(frame, TAG.MSG_WINDOW, index, 0)
    writeU32(frame, HEADER, count)
    return frame
  },
  bdpPing(index: number, probe: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4)
    writeHeader(frame, TAG.BDP_PING, index, 0)
    writeU32(frame, HEADER, probe)
    return frame
  },
  bdpPingAck(index: number, probe: number, backlog: number, pathRtt: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 12)
    writeHeader(frame, TAG.BDP_PING_ACK, index, 0)
    writeU32(frame, HEADER, probe)
    writeU32(frame, HEADER + 4, Math.min(0xffffffff, backlog))
    writeU32(
      frame,
      HEADER + 8,
      pathRtt === Infinity ? 0 : Math.min(0xffffffff, Math.max(1, Math.round(pathRtt * 1000))),
    )
    return frame
  },
  /** Wire: [header][u8 attached][u32 lastSeq] */
  attachResult(index: number, lastSeq: number | null): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 5)
    writeHeader(frame, TAG.ATTACH_RESULT, index, 0)
    frame[HEADER] = lastSeq === null ? 0 : 1
    writeU32(frame, HEADER + 1, lastSeq ?? 0)
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

/** Writes into `heartbeat`, which its caller just encoded, that its sender took `bytes` of what its WebSocket carried
 *  (mod 2^32): it bounds the link's rate. */
function withTook(heartbeat: Uint8Array<ArrayBuffer>, bytes: number): Uint8Array<ArrayBuffer> {
  writeHeader(heartbeat, heartbeat[0]!, 0, bytes)
  return heartbeat
}

// ===== Decode =====

/** The bytes of every frame of `tag`, 0 where they vary. */
function fixedBytes(tag: number): number {
  return FIXED_BYTES[tag] ?? 0
}

const FIXED_BYTES: Partial<Record<number, number>> = {
  [TAG.FIN]: encode.fin().byteLength,
  [TAG.PUBLISH_TO]: encode.publishTo(0, 0).byteLength,
  [TAG.STREAM_REQUEST_OPEN_ACK]: encode.streamRequestOpenAck().byteLength,
  [TAG.PIECES_ACK]: encode.piecesAck(0, 0, 0).byteLength,
  [TAG.ANNOUNCE]: encode.announce(0, 0).byteLength,
  [TAG.CLOSE]: encode.close(0, 0, 0).byteLength,
  [TAG.CLOSE_ACK]: encode.closeAck(0, 0).byteLength,
  [TAG.ERROR]: encode.error(0, ERROR_REASON.LOST, 0).byteLength,
  [TAG.WINDOW]: encode.window(0, 0, 0).byteLength,
  [TAG.BROADCAST_SUB]: encode.broadcastSub(0, false).byteLength,
  [TAG.BROADCAST_UNSUB]: encode.broadcastUnsub(0, false).byteLength,
  [TAG.BDP_PING]: encode.bdpPing(0, 0).byteLength,
  [TAG.BDP_PING_ACK]: encode.bdpPingAck(0, 0, 0, 0).byteLength,
  [TAG.MSG_WINDOW]: encode.msgWindow(0, 0).byteLength,
  [TAG.ATTACH_RESULT]: encode.attachResult(0, null).byteLength,
}

/** The bytes of the LENGTH a frame of `tag` needs ahead of it when another follows it in its message: 0 where its
 *  tag fixes its bytes. */
function lengthBytes(tag: number): number {
  return fixedBytes(tag) === 0 ? HEADER : 0
}

/** Whether a WebSocket `message` holds one frame. */
function isOneFrame(message: Uint8Array): boolean {
  const bytes = fixedBytes(message[0]!)
  return message[0] !== TAG.LENGTH && (bytes === 0 || message.byteLength <= bytes)
}

/** The frames of a message of several, each a copy (a `Buffer`'s `slice` is a view), read only as it is taken. */
function* messageFrames(message: Uint8Array<ArrayBuffer>): Generator<Uint8Array<ArrayBuffer>> {
  const end = message.byteLength
  let at = 0
  for (;;) {
    let bytes = fixedBytes(message[at]!)
    if (message[at] === TAG.LENGTH) {
      assertProtocol(end - at > HEADER, 'LENGTH last in its message')
      bytes = readU32(message, at + SEQ_OFFSET)
      at += HEADER
      assertProtocol(fixedBytes(message[at]!) === 0 && bytes >= HEADER && end - at > bytes, 'LENGTH of no frame')
    } else if (bytes === 0 || end - at <= bytes) {
      yield new Uint8Array(message.subarray(at))
      return
    }
    yield new Uint8Array(message.subarray(at, at + bytes))
    at += bytes
  }
}

/** The tag is the first header byte — readable before the frame is known to be well-formed. */
function peekTag(raw: Uint8Array): number | undefined {
  return raw[0]
}

function decode(frame: Uint8Array): DecodedFrame {
  assertProtocol(frame.length >= HEADER, 'frame too short')
  const tag = frame[0] as number
  const index = readU16(frame, 1)
  const seq = isSequencedTag(tag) ? readU32(frame, SEQ_OFFSET) : 0
  const payload = frame.subarray(HEADER)

  const bytes = frame.byteLength

  switch (tag) {
    case TAG.TEXT:
      return { tag: TAG.TEXT, index, seq, bytes, text: textDecoder.decode(payload) }
    case TAG.BINARY:
      return { tag: TAG.BINARY, index, seq, bytes, data: payload }
    case TAG.TEXT_ACK_REQ:
      return { tag: TAG.TEXT_ACK_REQ, index, seq, bytes, text: textDecoder.decode(payload) }
    case TAG.BINARY_ACK_REQ:
      return { tag: TAG.BINARY_ACK_REQ, index, seq, bytes, data: payload }
    case TAG.PUBLISH: {
      const { text, info } = decodePublishText(textDecoder.decode(payload))
      return { tag: TAG.PUBLISH, index, seq, bytes, text, info }
    }
    case TAG.PUBLISH_ACK_REQ:
      return { tag: TAG.PUBLISH_ACK_REQ, index, seq, bytes, text: textDecoder.decode(payload) }
    case TAG.PUBLISH_BINARY: {
      const { data, info } = decodePublishBinary(payload)
      return { tag: TAG.PUBLISH_BINARY, index, seq, bytes, data, info }
    }
    case TAG.PUBLISH_BINARY_ACK_REQ:
      return { tag: TAG.PUBLISH_BINARY_ACK_REQ, index, seq, bytes, data: payload }
    case TAG.PUBLISH_TO:
      return { tag: TAG.PUBLISH_TO, index, seq, bytes }
    case TAG.ACK_RES: {
      assertProtocol(payload.length >= ACK_RES_PREFIX, 'ACK_RES payload too short')
      const ackedSeq = readU64(payload, 0)
      const status = payload[8] as number
      assertProtocol(
        status === ACK_STATUS.OK ||
          status === ACK_STATUS.ERROR ||
          status === ACK_STATUS.ABORT ||
          status === ACK_STATUS.SHIELD_ERROR ||
          status === ACK_STATUS.OVERFLOW,
        `ACK_RES unknown status ${status}`,
      )
      const text = textDecoder.decode(payload.subarray(ACK_RES_PREFIX))
      return { tag: TAG.ACK_RES, index, seq, bytes, ackedSeq, status, text }
    }

    case TAG.PING: {
      assertProtocol(payload.length % PING_ENTRY_BYTES === 0, 'PING payload')
      const ended: PingEntry[] = []
      for (let offset = 0; offset < payload.length; offset += PING_ENTRY_BYTES) {
        const ix = readU16(payload, offset)
        ended.push({ ix, lastSeq: readU32(payload, offset + 2) })
      }
      return { tag: TAG.PING, ended, took: readU32(frame, SEQ_OFFSET) }
    }
    case TAG.PONG: {
      assertProtocol(payload.length % PONG_ENTRY_BYTES === 0, 'PONG payload')
      const ended: PongEntry[] = []
      for (let offset = 0; offset < payload.length; offset += PONG_ENTRY_BYTES) {
        const ix = readU16(payload, offset)
        const held = payload[offset + 2] === 1
        ended.push({ ix, lastSeq: held ? readU32(payload, offset + 3) : null })
      }
      return { tag: TAG.PONG, ended, took: readU32(frame, SEQ_OFFSET) }
    }
    case TAG.FIN:
      return { tag: TAG.FIN }
    // Client→server payloads are validated here, at the trust boundary; server→client ones are
    // trusted like every other payload our own server sends.
    case TAG.RECONCILE:
      return { tag: TAG.RECONCILE, payload: parseReconcilePayload(parseJsonPayload(payload)) }
    case TAG.BARRIER:
      return { tag: TAG.BARRIER, payload: parseBarrierPayload(parseJsonPayload(payload)) }
    case TAG.RECONCILED:
      return { tag: TAG.RECONCILED, payload: parseJsonPayload(payload) as ReconciledPayload }
    case TAG.STREAM_REQUEST_OPEN_ACK:
      return { tag: TAG.STREAM_REQUEST_OPEN_ACK }
    case TAG.PREPARE:
      return { tag: TAG.PREPARE, payload: parsePreparePayload(parseJsonPayload(payload)) }
    case TAG.READY:
      return { tag: TAG.READY, payload: parseJsonPayload(payload) as ReadyPayload }
    case TAG.PIECE:
      assertProtocol(payload.length >= 4, 'PIECE payload too short')
      return { tag: TAG.PIECE, total: readU32(payload, 0), piece: payload.subarray(4) }
    case TAG.PIECES_ACK:
      assertProtocol(payload.length >= 12, 'PIECES_ACK payload too short')
      return {
        tag: TAG.PIECES_ACK,
        bytes: readU32(payload, 0),
        heldMs: readU32(payload, 4),
        spanUs: readU32(payload, 8),
      }
    case TAG.ANNOUNCE:
      assertProtocol(payload.length >= 4, 'ANNOUNCE payload too short')
      return { tag: TAG.ANNOUNCE, bytes: readU32(payload, 0), floor: readU32(frame, SEQ_OFFSET) / 1000 }

    case TAG.CLOSE:
      assertProtocol(payload.length >= 4, 'CLOSE payload too short')
      return { tag: TAG.CLOSE, index, seq, timeoutMs: readU32(payload, 0) }
    case TAG.CLOSE_ACK:
      return { tag: TAG.CLOSE_ACK, index, seq }
    case TAG.ABORT:
      return { tag: TAG.ABORT, index, seq, abortValue: textDecoder.decode(payload) }
    case TAG.ERROR:
      assertProtocol(payload.length >= 1, 'ERROR payload too short')
      return { tag: TAG.ERROR, index, seq, reason: payload[0] as number }
    case TAG.WINDOW:
      assertProtocol(payload.length >= 8, 'WINDOW payload too short')
      return {
        tag: TAG.WINDOW,
        index,
        bytes: readU32(payload, 0),
        lastSeq: readU32(payload, 4),
      }
    case TAG.MSG_WINDOW:
      assertProtocol(payload.length >= 4, 'MSG_WINDOW payload too short')
      return { tag: TAG.MSG_WINDOW, index, count: readU32(payload, 0) }
    case TAG.BDP_PING:
      assertProtocol(payload.length >= 4, 'BDP_PING payload too short')
      return { tag: TAG.BDP_PING, index, probe: readU32(payload, 0) }
    case TAG.BDP_PING_ACK: {
      assertProtocol(payload.length >= 12, 'BDP_PING_ACK payload too short')
      const backlog = readU32(payload, 4)
      const pathRtt = readU32(payload, 8)
      return {
        tag: TAG.BDP_PING_ACK,
        index,
        probe: readU32(payload, 0),
        backlog: backlog === 0xffffffff ? Infinity : backlog,
        pathRtt: pathRtt === 0 ? Infinity : pathRtt / 1000,
      }
    }
    case TAG.BROADCAST_SUB:
      assertProtocol(payload.length >= 1, 'BROADCAST_SUB payload too short')
      return { tag: TAG.BROADCAST_SUB, index, binary: payload[0] === 1 }
    case TAG.BROADCAST_UNSUB:
      assertProtocol(payload.length >= 1, 'BROADCAST_UNSUB payload too short')
      return { tag: TAG.BROADCAST_UNSUB, index, binary: payload[0] === 1 }
    case TAG.ATTACH_RESULT: {
      assertProtocol(payload.length >= 5, 'ATTACH_RESULT payload too short')
      const lastSeq = payload[0] === 1 ? readU32(payload, 1) : null
      return { tag: TAG.ATTACH_RESULT, index, lastSeq }
    }

    default:
      throw new ProtocolViolationError(`unknown wire frame tag ${tag}`)
  }
}

const CLIENT_TAGS: ReadonlySet<number> = new Set([
  TAG.PING,
  TAG.PIECE,
  TAG.PIECES_ACK,
  TAG.ANNOUNCE,
  TAG.RECONCILE,
  TAG.BARRIER,
  TAG.PREPARE,
  TAG.TEXT,
  TAG.BINARY,
  TAG.TEXT_ACK_REQ,
  TAG.BINARY_ACK_REQ,
  TAG.ACK_RES,
  TAG.PUBLISH_ACK_REQ,
  TAG.PUBLISH_BINARY_ACK_REQ,
  TAG.CLOSE,
  TAG.CLOSE_ACK,
  TAG.ABORT,
  TAG.ERROR,
  TAG.WINDOW,
  TAG.MSG_WINDOW,
  TAG.BDP_PING,
  TAG.BDP_PING_ACK,
  TAG.BROADCAST_SUB,
  TAG.BROADCAST_UNSUB,
])

/** Server ingress: `decode` owns the frame's shape, this owns its direction and the upgrade frames'
 *  size cap. The cap is checked on the raw bytes because its job is to bound what an unauthenticated
 *  peer can make us parse — after `decode` it would be bounding nothing. */
function decodeClientFrame(raw: Uint8Array<ArrayBuffer>, maxUpgradeFrameBytes: number): DecodedFrame {
  const tag = peekTag(raw)
  const isUpgradeFrame = tag === TAG.PREPARE || tag === TAG.BARRIER
  assertProtocol(!isUpgradeFrame || raw.byteLength <= maxUpgradeFrameBytes, 'upgrade frame over byte cap')
  const frame = decode(raw)
  assertProtocol(CLIENT_TAGS.has(frame.tag), `client sent a server-only frame ${frame.tag}`)
  return frame
}
