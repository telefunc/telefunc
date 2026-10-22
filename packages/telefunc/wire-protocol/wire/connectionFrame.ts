export {
  TAG,
  ACK_STATUS,
  ERROR_REASON,
  encode,
  withTook,
  MESSAGE_FRAME_OVERHEAD_MAX_BYTES,
  fixedBytes,
  isOneFrame,
  messageFrames,
  decode,
  decodeClientFrame,
  assertProtocol,
  peekTag,
  ProtocolViolationError,
  isChannelCtrlTag,
  isSequencedFrame,
  isSequencedTag,
  passesData,
  replayLaneOf,
  countsCredit,
  isConnCtrlTag,
  encodePublishText,
  encodePublishBinary,
  seqNear,
  seqThrough,
}
export type {
  AckResultStatus,
  ErrorReason,
  ReplayLaneKind,
  DecodedFrame,
  ChannelFrame,
  ChannelCtrlFrame,
  ChannelDataFrame,
  ReconcilePayload,
  ReconcileOpenEntry,
  ReattachState,
  BarrierPayload,
  ReconciledPayload,
  PreparePayload,
  ReadyPayload,
  PingEntry,
  PongEntry,
  PiecesAck,
  BdpPingAck,
  WirePublishInfo,
}

import { MAX_CHANNELS_PER_CONNECTION, type ChannelTransports } from '../constants.js'
import { assert } from '../../utils/assert.js'

// ===== Wire protocol =====
//
// Every frame has a uniform 7-byte header:
//   [u8 tag][u16 LE index][u32 LE seq][payload...]
//
// `tag` discriminates the frame variant (data, connection ctrl, per-channel ctrl).
// `index` is the channel ix for per-channel frames; 0 for connection-level frames.
// `seq` is the replay sequence number of a sequenced frame: a data frame, or a CLOSE, CLOSE_ACK, ABORT or ERROR. Other
// ctrl frames carry 0, but a WebSocket's PING, PONG and ANNOUNCE, which say a count there. Seqs count on
// past 32 bits, and a u32 seq on the wire is their low 32 bits, which `decode` returns as they are: the frame's owner
// reads the whole seq from where it stands on the channel (`seqNear`, `seqThrough`).
//
// Tag layout — sparse ranges so range checks classify:
//   0x01–0x0f  connection-level control (no ix; seq 0 except on a WebSocket's PING, PONG and ANNOUNCE)
//   0x10–0x29  data plane (carries seq, payload varies)
//   0x30–      per-channel control (carries ix; seq 0 except on the four closing frames)
//
// Channel indices are client-owned and stable for the channel's lifetime.
// Sequence numbers are sender-assigned for replayable data frames in both directions.
// Each side tracks the highest seq received and replays after reconnect via reconcile.

const HEADER = 7
/** Where a header's u32 starts: a frame's seq, or the count a PING, PONG or ANNOUNCE carries in its place. */
const SEQ_OFFSET = 3
/** How many seqs a u32 tells apart. */
const SEQ_SPAN = 2 ** 32
const PING_ENTRY_BYTES = 6
const PONG_ENTRY_BYTES = 7
/** An ACK_RES's `ackedSeq` and status. */
const ACK_RES_PREFIX = 9
/** The most a frame adds to a channel message or its ack answer: an ACK_RES's header and prefix. */
const MESSAGE_FRAME_OVERHEAD_MAX_BYTES = HEADER + ACK_RES_PREFIX
const DATA_TAG_MIN = 0x10

const CHANNEL_CTRL_TAG_MIN = 0x30

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

// ===== Tags =====

const TAG = {
  // ─── Connection-level control (no ix) ───
  /** Client → server heartbeat: a `PingEntry` per closed attached channel; on a WebSocket, its seq is what it took. */
  PING: 0x01 as const,
  /** Server → client heartbeat: a `PongEntry` per channel the PING names; on a WebSocket, its seq is what it took. */
  PONG: 0x02 as const,
  /** Server → client on old transport after upgrade drain: signals last frame on this transport. */
  FIN: 0x03 as const,
  /** Client → server on every (re)connect, and as channels open. JSON payload (`ReconcilePayload`). */
  RECONCILE: 0x04 as const,
  /** Server → client after reconcile. JSON payload (`ReconciledPayload`). */
  RECONCILED: 0x05 as const,
  /** Server → client wire-probe ack. Sent over the SSE downstream after the long-lived
   *  client→server stream-request POST's metadata has been parsed. The client awaits this
   *  within `STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS` before declaring the transport open —
   *  confirms the half-duplex streaming wire round-trips end-to-end. */
  STREAM_REQUEST_OPEN_ACK: 0x06 as const,
  PREPARE: 0x07 as const,
  READY: 0x08 as const,
  /** Client → server on the OLD wire as its final frame: the same cursors as a RECONCILE, but
   *  addressed to the staged probe. Its own tag so the size cap lands on the raw bytes. */
  BARRIER: 0x09 as const,
  /** A frame's piece. Payload: u32 the frame's bytes, then the piece; an empty one has the next two acked at once. */
  PIECE: 0x0a as const,
  /** Payload: u32 bytes taken (mod 2^32), u32 ms it held the newest, u32 µs since the last ack's newest (0: none). */
  PIECES_ACK: 0x0b as const,
  /** The bytes of the frames that follow at once (see `pieces.ts`). Payload: u32; its seq: the floor in bytes/s. */
  ANNOUNCE: 0x0c as const,

  // ─── Data plane ───
  TEXT: 0x10 as const,
  BINARY: 0x11 as const,
  TEXT_ACK_REQ: 0x12 as const,
  BINARY_ACK_REQ: 0x13 as const,
  /** ACK response: `ackedSeq` whole, as it may come any number of seqs after its request, then the result. */
  ACK_RES: 0x14 as const,
  /** Replayable publish frame delivered to keyed-channel subscribers. */
  PUBLISH: 0x15 as const,
  /** Replayable keyed publish frame that requests an acknowledgement receipt. */
  PUBLISH_ACK_REQ: 0x16 as const,
  /** Replayable binary publish frame delivered to keyed-channel subscribers. */
  PUBLISH_BINARY: 0x17 as const,
  /** Replayable keyed binary publish frame that requests an acknowledgement receipt. */
  PUBLISH_BINARY_ACK_REQ: 0x18 as const,

  // ─── Per-channel control (carries ix) ───
  /** A close request. Sequenced, so it replays like data. */
  CLOSE: 0x30 as const,
  /** Sequenced, as CLOSE is. */
  CLOSE_ACK: 0x31 as const,
  /** Channel closed with an abort value (analogous to `throw Abort()`). Sequenced, as CLOSE is. */
  ABORT: 0x32 as const,
  /** Channel closed with an error. Payload: u8 `ERROR_REASON`. Client → server for `LOST` only; sequenced as CLOSE. */
  ERROR: 0x33 as const,
  /** Receiver → sender: bytes consumed plus window (mod 2^32), and the last seq it has, which the replay lets go. */
  WINDOW: 0x34 as const,
  BROADCAST_SUB: 0x35 as const,
  BROADCAST_UNSUB: 0x36 as const,
  /** BDP probe — receiver→sender. Sender echoes `BDP_PING_ACK` immediately so the
   *  receiver can measure bytes-in-flight during one RTT and grow `WINDOW` to BDP. Payload: u32, the probe's number. */
  BDP_PING: 0x37 as const,
  /** Payload: u32 the probe, u32 the least backlog at credit run-out (all ones: none), u32 path rtt in µs (0: none). */
  BDP_PING_ACK: 0x38 as const,
  /** Flow-control message-count limit. Parallel to `WINDOW` but counted in messages, not bytes,
   *  to bound receiver dispatch CPU regardless of message size. */
  MSG_WINDOW: 0x39 as const,
  /** Server → client: an initial channel a RECONCILED left out, attached at `lastSeq` or not within `connectTtl`. */
  ATTACH_RESULT: 0x3b as const,
}

function isConnCtrlTag(tag: number): boolean {
  return tag < DATA_TAG_MIN
}

function isChannelCtrlTag(tag: number): boolean {
  return tag >= CHANNEL_CTRL_TAG_MIN
}

/** What a replay holds: data, and the closing frames. */
function isSequencedFrame(frame: DecodedFrame): frame is SequencedFrame {
  return isSequencedTag(frame.tag)
}

function isSequencedTag(tag: number): boolean {
  return (tag >= DATA_TAG_MIN && tag < CHANNEL_CTRL_TAG_MIN) || isClosingTag(tag)
}

/** Whether a frame of `tag` passes the data waiting ahead of it: not sequenced, nor FIN, which ends its wire's data. */
function passesData(tag: number): boolean {
  return !isSequencedTag(tag) && tag !== TAG.FIN
}

function isClosingTag(tag: number): boolean {
  return tag === TAG.CLOSE || tag === TAG.CLOSE_ACK || tag === TAG.ABORT || tag === TAG.ERROR
}

/** The replay lane of a sequenced frame; `closing` holds the frame that ends a channel, which no byte budget drops. */
type ReplayLaneKind = 'text' | 'binary' | 'closing'

function replayLaneOf(tag: number): ReplayLaneKind {
  if (isClosingTag(tag)) return 'closing'
  assert(tag >= DATA_TAG_MIN && tag < CHANNEL_CTRL_TAG_MIN)
  return tag === TAG.BINARY ||
    tag === TAG.BINARY_ACK_REQ ||
    tag === TAG.PUBLISH_BINARY ||
    tag === TAG.PUBLISH_BINARY_ACK_REQ
    ? 'binary'
    : 'text'
}

/** Whether flow control counts a data frame (see `constants.ts`): a message, not an ack request or its answer. */
function countsCredit(tag: number): boolean {
  return tag === TAG.TEXT || tag === TAG.BINARY || tag === TAG.PUBLISH || tag === TAG.PUBLISH_BINARY
}

// ===== Reconcile payloads (JSON-encoded after the header) =====

type ReconcileOpenEntry = {
  id: string
  ix: number
  lastSeq: number
  /** First reconcile: the server awaits it up to `connectTtl`, then an ATTACH_RESULT settles it; others fail fast. */
  initial?: true
  /** A broadcast's subscriptions as of this (re)attach, applied before its `onOpen` fires. */
  broadcast?: { text: boolean; binary: boolean }
  /** A BDP probe the RECONCILED answers: on a wire other than the last, it queues behind none of the channel's data. */
  probe?: number
}

/** What a channel adds to its own RECONCILE entry. */
type ReattachState = Pick<ReconcileOpenEntry, 'broadcast' | 'probe'>

/** On a new wire it names every channel the page holds; on the wire of the session it names, with `released`, it names
 *  only the channels no RECONCILE named there yet, and `released` the ones the page let go since its last. */
type ReconcilePayload = {
  sessionId?: string
  open: ReconcileOpenEntry[]
  released?: number[]
}

/** A barrier names the session it retires and the upgrade it commits — both mandatory, where a
 *  reconcile has neither. A malformed one can no longer fall through to ordinary reconciliation. */
type BarrierPayload = {
  sessionId: string
  upgradeId: string
  open: ReconcileOpenEntry[]
}

type PreparePayload = {
  upgradeId: string
  sessionId: string
}

type ReadyPayload = {
  upgradeId: string
}

/** A closed channel of the page's: the last seq the page has of what the server sent on it. */
type PingEntry = { ix: number; lastSeq: number }

/** The last seq the server has of what the page sent on that channel, or null: the server no longer holds it. */
type PongEntry = { ix: number; lastSeq: number | null }

/** Per-channel acknowledgment: the server confirms each `ix` it attached and tells the
 *  client which `lastSeq` it has on file, so the client can replay frames after that. */
type ReconciledPayload = {
  sessionId: string
  open: { ix: number; lastSeq: number }[]
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
  /** The largest frame the server takes on a WebSocket. */
  maxFrameBytes: number
  serverReplayBuffer: number
  serverReplayBufferBinary: number
  clientReplayBuffer: number
  clientReplayBufferBinary: number
  sseFlushThrottle: number
  ssePostIdleFlushDelay: number
  transports: ChannelTransports
  upgradeId?: string
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

/** Why the server closed a channel with `ERROR`. */
const ERROR_REASON = {
  /** An unhandled server error. */
  BUG: 0x00 as const,
  /** Its page fell further behind a broadcast than the server holds for it. */
  OVERFLOW: 0x01 as const,
  /** A reconnect needed frames its sender's replay buffer had dropped to stay within its size. */
  LOST: 0x02 as const,
}

type ErrorReason = (typeof ERROR_REASON)[keyof typeof ERROR_REASON]

/** Ordering metadata embedded in PUBLISH frames on the wire. */
type WirePublishInfo = { seq: number; timestamp: number }

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

type ConnCtrlFrame =
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

type DecodedFrame = ChannelFrame | ConnCtrlFrame

// ===== Encode =====

function writeHeader(frame: Uint8Array, tag: number, index: number, seq: number): void {
  frame[0] = tag
  writeU16(frame, 1, index)
  writeU32(frame, SEQ_OFFSET, seq)
}

function writeU16(frame: Uint8Array, offset: number, n: number): void {
  frame[offset] = n & 0xff
  frame[offset + 1] = (n >> 8) & 0xff
}

function writeU32(frame: Uint8Array, offset: number, n: number): void {
  frame[offset] = n & 0xff
  frame[offset + 1] = (n >> 8) & 0xff
  frame[offset + 2] = (n >> 16) & 0xff
  frame[offset + 3] = (n >> 24) & 0xff
}

function readU16(buf: Uint8Array, offset: number): number {
  return (buf[offset] as number) | ((buf[offset + 1] as number) << 8)
}

/** u32 low, then u32 high: exact to 2^53, past which a JS number skips integers. */
function writeU64(frame: Uint8Array, offset: number, n: number): void {
  writeU32(frame, offset, n)
  writeU32(frame, offset + 4, n / SEQ_SPAN)
}

function readU32(buf: Uint8Array, offset: number): number {
  return (
    ((buf[offset] as number) |
      ((buf[offset + 1] as number) << 8) |
      ((buf[offset + 2] as number) << 16) |
      ((buf[offset + 3] as number) << 24)) >>>
    0
  )
}

function readU64(buf: Uint8Array, offset: number): number {
  return readU32(buf, offset) + readU32(buf, offset + 4) * SEQ_SPAN
}

// ===== Seqs off the wire =====

/** The seq with low bits `bits` nearest `expected` (RFC 9000, A.3): no wire or replay holds 2^31 frames. */
function seqNear(bits: number, expected: number): number {
  const seq = expected - (expected >>> 0) + bits
  if (seq <= expected - SEQ_SPAN / 2) return seq + SEQ_SPAN
  if (seq > expected + SEQ_SPAN / 2 && seq >= SEQ_SPAN) return seq - SEQ_SPAN
  return seq
}

/** The latest seq with low bits `bits` through `sent`: a peer never lacks 2^32 frames of what was sent. */
function seqThrough(bits: number, sent: number): number {
  const seq = sent - (sent >>> 0) + bits
  return seq > sent && seq >= SEQ_SPAN ? seq - SEQ_SPAN : seq
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

  // ── Connection-level ctrls ──
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

  // ── Per-channel ctrls ──
  close(index: number, timeoutMs: number, seq = 0): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4)
    writeHeader(frame, TAG.CLOSE, index, seq)
    writeU32(frame, HEADER, timeoutMs)
    return frame
  },
  closeAck: (index: number, seq = 0) => encodeBareFrame(TAG.CLOSE_ACK, index, seq),
  abort: (index: number, abortValue: string, seq = 0) => encodeTextFrame(TAG.ABORT, index, abortValue, seq),
  error(index: number, reason: ErrorReason, seq = 0): Uint8Array<ArrayBuffer> {
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

/** `heartbeat` saying its sender took `bytes` of what its WebSocket carried (mod 2^32): it bounds the link's rate. */
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
  [TAG.STREAM_REQUEST_OPEN_ACK]: encode.streamRequestOpenAck().byteLength,
  [TAG.PIECES_ACK]: encode.piecesAck(0, 0, 0).byteLength,
  [TAG.ANNOUNCE]: encode.announce(0, 0).byteLength,
  [TAG.CLOSE]: encode.close(0, 0).byteLength,
  [TAG.CLOSE_ACK]: encode.closeAck(0).byteLength,
  [TAG.ERROR]: encode.error(0, ERROR_REASON.LOST).byteLength,
  [TAG.WINDOW]: encode.window(0, 0, 0).byteLength,
  [TAG.BROADCAST_SUB]: encode.broadcastSub(0, false).byteLength,
  [TAG.BROADCAST_UNSUB]: encode.broadcastUnsub(0, false).byteLength,
  [TAG.BDP_PING]: encode.bdpPing(0, 0).byteLength,
  [TAG.BDP_PING_ACK]: encode.bdpPingAck(0, 0, 0, 0).byteLength,
  [TAG.MSG_WINDOW]: encode.msgWindow(0, 0).byteLength,
  [TAG.ATTACH_RESULT]: encode.attachResult(0, null).byteLength,
}

/** Whether a WebSocket `message` holds one frame. */
function isOneFrame(message: Uint8Array): boolean {
  const bytes = fixedBytes(message[0]!)
  return bytes === 0 || message.byteLength <= bytes
}

/** The frames of a message of several, each a copy (a `Buffer`'s `slice` is a view), read only as it is taken. */
function* messageFrames(message: Uint8Array<ArrayBuffer>): Generator<Uint8Array<ArrayBuffer>> {
  const end = message.byteLength
  let at = 0
  for (;;) {
    const bytes = fixedBytes(message[at]!)
    if (bytes === 0 || end - at <= bytes) {
      yield new Uint8Array(message.subarray(at))
      return
    }
    const tag = message[at]
    assertProtocol(tag !== TAG.PIECE && tag !== TAG.ANNOUNCE, 'frame ahead of another it never is')
    yield new Uint8Array(message.subarray(at, at + bytes))
    at += bytes
  }
}

/** Raised for every malformed-bytes path. Never `assert`: that one tells the user they found a
 *  telefunc bug, and these bytes are the peer's. */
class ProtocolViolationError extends Error {
  constructor(
    debugInfo?: string,
    readonly target?: unknown,
  ) {
    super(debugInfo)
  }
}

/** `assert` for peer input, `debugInfo` and all. `target` names the wire to terminate when it
 *  isn't the sender. */
function assertProtocol(condition: unknown, debugInfo: string, target?: unknown): asserts condition {
  if (!condition) throw new ProtocolViolationError(debugInfo, target)
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
    case TAG.ACK_RES: {
      assertProtocol(payload.length >= ACK_RES_PREFIX, 'ACK_RES payload too short')
      const ackedSeq = readU64(payload, 0)
      const status = payload[8] as number
      assertProtocol(
        status === ACK_STATUS.OK ||
          status === ACK_STATUS.ERROR ||
          status === ACK_STATUS.ABORT ||
          status === ACK_STATUS.SHIELD_ERROR,
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

function parseJsonPayload(payload: Uint8Array): unknown {
  try {
    return JSON.parse(textDecoder.decode(payload))
  } catch {
    throw new ProtocolViolationError('payload is not JSON')
  }
}

function asObject(value: unknown): Record<string, unknown> {
  assertProtocol(value !== null && typeof value === 'object' && !Array.isArray(value), 'payload is not an object')
  return value as Record<string, unknown>
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0
}

function isUint(value: unknown, max: number): value is number {
  return Number.isInteger(value) && (value as number) >= 0 && (value as number) <= max
}

function parsePreparePayload(value: unknown): PreparePayload {
  const payload = asObject(value)
  assertProtocol(isNonEmptyString(payload.upgradeId), 'PREPARE upgradeId')
  assertProtocol(isNonEmptyString(payload.sessionId), 'PREPARE sessionId')
  return { upgradeId: payload.upgradeId, sessionId: payload.sessionId }
}

function parseOpenList(payload: Record<string, unknown>): void {
  assertProtocol(Array.isArray(payload.open), 'RECONCILE open')
  assertProtocol(payload.open.length <= MAX_CHANNELS_PER_CONNECTION, 'RECONCILE over entry cap')
  const indexes = new Set<number>()
  for (const rawEntry of payload.open) {
    const entry = asObject(rawEntry)
    assertProtocol(typeof entry.id === 'string', 'RECONCILE entry id')
    // `ix` is truncated to u16 by the header writer, so a wider one would alias onto another channel.
    assertProtocol(isUint(entry.ix, 0xffff) && !indexes.has(entry.ix), 'RECONCILE entry ix')
    indexes.add(entry.ix)
    assertProtocol(isUint(entry.lastSeq, Number.MAX_SAFE_INTEGER), 'RECONCILE entry lastSeq')
    assertProtocol(entry.initial === undefined || entry.initial === true, 'RECONCILE entry initial')
    assertProtocol(entry.probe === undefined || isUint(entry.probe, 0xffffffff), 'RECONCILE entry probe')
    if (entry.broadcast !== undefined) {
      const broadcast = asObject(entry.broadcast)
      assertProtocol(
        typeof broadcast.text === 'boolean' && typeof broadcast.binary === 'boolean',
        'RECONCILE entry broadcast',
      )
    }
  }
}

function parseReconcilePayload(value: unknown): ReconcilePayload {
  const payload = asObject(value)
  parseOpenList(payload)
  assertProtocol(payload.sessionId === undefined || isNonEmptyString(payload.sessionId), 'RECONCILE sessionId')
  if (payload.released !== undefined) {
    assertProtocol(isNonEmptyString(payload.sessionId) && Array.isArray(payload.released), 'RECONCILE released')
    assertProtocol(payload.released.length <= MAX_CHANNELS_PER_CONNECTION, 'RECONCILE released over entry cap')
    for (const ix of payload.released) assertProtocol(isUint(ix, 0xffff), 'RECONCILE released ix')
  }
  return payload as ReconcilePayload
}

function parseBarrierPayload(value: unknown): BarrierPayload {
  const payload = asObject(value)
  parseOpenList(payload)
  // Both ids are mandatory: a barrier that cannot name its session and its upgrade has no claim
  // on either, and there is no ordinary-reconcile leg left for it to fall through to.
  assertProtocol(isNonEmptyString(payload.sessionId), 'barrier sessionId')
  assertProtocol(isNonEmptyString(payload.upgradeId), 'barrier upgradeId')
  return payload as BarrierPayload
}

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

function encodePublishBinary(data: Uint8Array, info: WirePublishInfo): Uint8Array {
  const result = new Uint8Array(PUBLISH_BINARY_HEADER + data.byteLength)
  const view = new DataView(result.buffer)
  view.setFloat64(0, info.seq, true)
  view.setFloat64(8, info.timestamp, true)
  result.set(data, PUBLISH_BINARY_HEADER)
  return result
}

function decodePublishBinary(wire: Uint8Array): { data: Uint8Array; info: WirePublishInfo } {
  assertProtocol(wire.byteLength >= PUBLISH_BINARY_HEADER, 'PUBLISH_BINARY frame too short for info header')
  const view = new DataView(wire.buffer, wire.byteOffset, wire.byteLength)
  const seq = view.getFloat64(0, true)
  const timestamp = view.getFloat64(8, true)
  assertProtocol(Number.isFinite(seq) && Number.isFinite(timestamp), 'PUBLISH_BINARY frame info must be finite numbers')
  return { data: wire.subarray(PUBLISH_BINARY_HEADER), info: { seq, timestamp } }
}
