export {
  TAG,
  ACK_STATUS,
  encode,
  decode,
  decodeClientFrame,
  assertProtocol,
  peekTag,
  ProtocolViolationError,
  isChannelCtrlTag,
  isChannelDataFrame,
  isConnCtrlTag,
  encodePublishText,
  encodePublishBinary,
  decodePublishBinary,
  payloadBytes,
}
export type {
  AckResultStatus,
  DecodedFrame,
  ChannelFrame,
  ChannelCtrlFrame,
  ChannelDataFrame,
  ReconcilePayload,
  ReconcileOpenEntry,
  BarrierPayload,
  ReconciledPayload,
  PreparePayload,
  ReadyPayload,
  WirePublishInfo,
}

import { decodeOrderingFrame, encodeOrderingFrame } from './ordering-frame.js'
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
const payloadBytes = (frame: Uint8Array): number => frame.byteLength - HEADER
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
   *  client→server stream-request POST's metadata has been parsed. The client awaits this
   *  within `STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS` before declaring the transport open —
   *  confirms the half-duplex streaming wire round-trips end-to-end. */
  STREAM_REQUEST_OPEN_ACK: 0x06 as const,
  PREPARE: 0x07 as const,
  READY: 0x08 as const,
  /** Client → server on the OLD wire as its final frame: the same cursors as a RECONCILE, but
   *  addressed to the staged probe. Its own tag so the size cap lands on the raw bytes. */
  BARRIER: 0x09 as const,

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
  /** Flow-control window update — sets the peer's send credit to the advertised value. */
  WINDOW: 0x34 as const,
  BROADCAST_SUB: 0x35 as const,
  BROADCAST_UNSUB: 0x36 as const,
  /** BDP probe — receiver→sender. Sender echoes `BDP_PING_ACK` immediately so the
   *  receiver can measure bytes-in-flight during one RTT and grow `WINDOW` to BDP. */
  BDP_PING: 0x37 as const,
  BDP_PING_ACK: 0x38 as const,
  /** Flow-control message-count update — sets the peer's send msg-credit to the
   *  advertised value. Parallel to `WINDOW` but counted in messages, not bytes,
   *  to bound receiver dispatch CPU regardless of message size. */
  MSG_WINDOW: 0x39 as const,
}

function isConnCtrlTag(tag: number): boolean {
  return tag < DATA_TAG_MIN
}

function isChannelCtrlTag(tag: number): boolean {
  return tag >= CHANNEL_CTRL_TAG_MIN
}

function isChannelDataFrame(frame: DecodedFrame): frame is ChannelDataFrame {
  return frame.tag >= DATA_TAG_MIN && frame.tag < CHANNEL_CTRL_TAG_MIN
}

// ===== Reconcile payloads (JSON-encoded after the header) =====

type ReconcileOpenEntry = {
  id: string
  ix: number
  lastSeq: number
  /** `initial: true` means this is the first reconcile for that channel — the server may
   *  not have created it yet (late-creation race during request body parse), so the server
   *  should wait up to `connectTtl` for it. Established channels (already reconciled at
   *  least once) omit `initial`; the server fails them fast if they're missing rather than
   *  stalling the entire reconcile. */
  initial?: true
}

type ReconcilePayload = {
  sessionId?: string
  open: ReconcileOpenEntry[]
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

/** Per-channel acknowledgment: the server confirms each `ix` it attached and tells the
 *  client which `lastSeq` it has on file, so the client can replay frames after that. */
type ReconciledPayload = {
  sessionId: string
  open: { ix: number; lastSeq: number }[]
  reconnectTimeout: number
  idleTimeout: number
  pingInterval: number
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

/** Ordering metadata embedded in PUBLISH frames on the wire. */
type WirePublishInfo = { seq: number; timestamp: number }

// ===== Decoded frame =====

type ChannelDataFrame =
  | { tag: typeof TAG.TEXT; index: number; seq: number; text: string; bytes: number }
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
  | { tag: typeof TAG.MSG_WINDOW; index: number; count: number }
  | { tag: typeof TAG.BROADCAST_SUB; index: number; binary: boolean }
  | { tag: typeof TAG.BROADCAST_UNSUB; index: number; binary: boolean }
  | { tag: typeof TAG.BDP_PING; index: number }
  | { tag: typeof TAG.BDP_PING_ACK; index: number }

/** Frames that carry an `index` (channel ix) — both data and per-channel ctrl. */
type ChannelFrame = ChannelDataFrame | ChannelCtrlFrame

type ConnCtrlFrame =
  | { tag: typeof TAG.PING }
  | { tag: typeof TAG.PONG }
  | { tag: typeof TAG.FIN }
  | { tag: typeof TAG.RECONCILE; payload: ReconcilePayload }
  | { tag: typeof TAG.BARRIER; payload: BarrierPayload }
  | { tag: typeof TAG.RECONCILED; payload: ReconciledPayload }
  | { tag: typeof TAG.STREAM_REQUEST_OPEN_ACK }
  | { tag: typeof TAG.PREPARE; payload: PreparePayload }
  | { tag: typeof TAG.READY; payload: ReadyPayload }

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
  barrier: (payload: BarrierPayload) => encodeJsonFrame(TAG.BARRIER, payload),
  reconciled: (payload: ReconciledPayload) => encodeJsonFrame(TAG.RECONCILED, payload),
  streamRequestOpenAck: () => encodeBareFrame(TAG.STREAM_REQUEST_OPEN_ACK),
  prepare: (payload: PreparePayload) => encodeJsonFrame(TAG.PREPARE, payload),
  ready: (payload: ReadyPayload) => encodeJsonFrame(TAG.READY, payload),

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
  msgWindow(index: number, count: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(HEADER + 4)
    writeHeader(frame, TAG.MSG_WINDOW, index, 0)
    writeU32(frame, HEADER, count)
    return frame
  },
  bdpPing: (index: number) => encodeBareFrame(TAG.BDP_PING, index),
  bdpPingAck: (index: number) => encodeBareFrame(TAG.BDP_PING_ACK, index),
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
  const index = (frame[1] as number) | ((frame[2] as number) << 8)
  const seq = readU32(frame, 3)
  const payload = frame.subarray(HEADER)

  switch (tag) {
    case TAG.TEXT:
      return { tag: TAG.TEXT, index, seq, text: textDecoder.decode(payload), bytes: payload.byteLength }
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
      assertProtocol(payload.length >= 5, 'ACK_RES payload too short')
      const ackedSeq = readU32(payload, 0)
      const status = payload[4] as number
      assertProtocol(
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

    case TAG.CLOSE:
      assertProtocol(payload.length >= 4, 'CLOSE payload too short')
      return { tag: TAG.CLOSE, index, timeoutMs: readU32(payload, 0) }
    case TAG.CLOSE_ACK:
      return { tag: TAG.CLOSE_ACK, index }
    case TAG.ABORT:
      return { tag: TAG.ABORT, index, abortValue: textDecoder.decode(payload) }
    case TAG.ERROR:
      return { tag: TAG.ERROR, index }
    case TAG.WINDOW:
      assertProtocol(payload.length >= 4, 'WINDOW payload too short')
      return { tag: TAG.WINDOW, index, bytes: readU32(payload, 0) }
    case TAG.MSG_WINDOW:
      assertProtocol(payload.length >= 4, 'MSG_WINDOW payload too short')
      return { tag: TAG.MSG_WINDOW, index, count: readU32(payload, 0) }
    case TAG.BDP_PING:
      return { tag: TAG.BDP_PING, index }
    case TAG.BDP_PING_ACK:
      return { tag: TAG.BDP_PING_ACK, index }
    case TAG.BROADCAST_SUB:
      assertProtocol(payload.length >= 1, 'BROADCAST_SUB payload too short')
      return { tag: TAG.BROADCAST_SUB, index, binary: payload[0] === 1 }
    case TAG.BROADCAST_UNSUB:
      assertProtocol(payload.length >= 1, 'BROADCAST_UNSUB payload too short')
      return { tag: TAG.BROADCAST_UNSUB, index, binary: payload[0] === 1 }

    default:
      throw new ProtocolViolationError(`unknown wire frame tag ${tag}`)
  }
}

const CLIENT_TAGS: ReadonlySet<number> = new Set([
  TAG.PING,
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
  TAG.WINDOW,
  TAG.MSG_WINDOW,
  TAG.BDP_PING,
  TAG.BDP_PING_ACK,
  TAG.BROADCAST_SUB,
  TAG.BROADCAST_UNSUB,
])

/** Server ingress: `decode` owns the frame's shape, this owns its direction and the upgrade frames'
 *  size cap. Both are checked on the raw bytes because their job is to bound what an unauthenticated
 *  peer can make us parse — after `decode` they would be bounding nothing. */
function decodeClientFrame(raw: Uint8Array<ArrayBuffer>, maxUpgradeFrameBytes: number): DecodedFrame {
  const tag = peekTag(raw)
  assertProtocol(tag !== undefined && CLIENT_TAGS.has(tag), `client sent a server-only frame ${tag}`)
  const isUpgradeFrame = tag === TAG.PREPARE || tag === TAG.BARRIER
  assertProtocol(!isUpgradeFrame || raw.byteLength <= maxUpgradeFrameBytes, 'upgrade frame over byte cap')
  return decode(raw)
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
  const indexes = new Set<number>()
  for (const rawEntry of payload.open) {
    const entry = asObject(rawEntry)
    assertProtocol(typeof entry.id === 'string', 'RECONCILE entry id')
    // `ix` is truncated to u16 by the header writer, so a wider one would alias onto another channel.
    assertProtocol(isUint(entry.ix, 0xffff) && !indexes.has(entry.ix), 'RECONCILE entry ix')
    indexes.add(entry.ix)
    assertProtocol(isUint(entry.lastSeq, 0xffffffff), 'RECONCILE entry lastSeq')
    assertProtocol(entry.initial === undefined || entry.initial === true, 'RECONCILE entry initial')
  }
}

function parseReconcilePayload(value: unknown): ReconcilePayload {
  const payload = asObject(value)
  parseOpenList(payload)
  assertProtocol(payload.sessionId === undefined || isNonEmptyString(payload.sessionId), 'RECONCILE sessionId')
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

// Current binary publish frames are explicitly versioned. The NaN timestamp sentinel makes an
// older 12-byte reader fail loudly instead of silently shifting payload bytes.
const PUBLISH_BINARY_PREFIX = new Uint8Array([0x54, 0x46, 0x42, 1, 0, 0, 0, 0, 0, 0, 0xf8, 0x7f])

function encodePublishBinary(data: Uint8Array, info: WirePublishInfo): Uint8Array {
  const ordered = encodeOrderingFrame(data, info)
  const wire = new Uint8Array(PUBLISH_BINARY_PREFIX.byteLength + ordered.byteLength)
  wire.set(PUBLISH_BINARY_PREFIX)
  wire.set(ordered, PUBLISH_BINARY_PREFIX.byteLength)
  return wire
}

function decodePublishBinary(wire: Uint8Array): { data: Uint8Array; info: WirePublishInfo } {
  assertProtocol(
    wire.byteLength >= PUBLISH_BINARY_PREFIX.byteLength,
    'PUBLISH_BINARY frame too short for version header',
  )
  const view = new DataView(wire.buffer, wire.byteOffset, wire.byteLength)
  const versioned = view.getUint16(0, true) === 0x4654 && wire[2] === 0x42 && Number.isNaN(view.getFloat64(4, true))
  assertProtocol(versioned, 'PUBLISH_BINARY frame uses an unsupported legacy wire format')
  assertProtocol(wire[3] === 1, `Unsupported PUBLISH_BINARY wire version ${wire[3]}`)
  const { payload, info } = decodeOrderingFrame(wire.subarray(PUBLISH_BINARY_PREFIX.byteLength))
  return { data: payload, info }
}
