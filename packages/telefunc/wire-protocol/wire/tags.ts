export { TAG, isWireCtrlTag, isChannelCtrlTag, isClosingTag, isSequencedTag, passesData, replayKindOf, countsCredit }
export type { ReplayKind }

import { assert } from '../../utils/assert.js'

// Tag layout — sparse ranges so range checks classify:
//   0x01–0x0f  wire-level control (no ix; seq 0 except on a LENGTH and a WebSocket's PING, PONG and ANNOUNCE)
//   0x10–0x2f  data plane (carries seq, payload varies)
//   0x30–      per-channel control (carries ix; seq 0 except on the four closing frames)

const DATA_TAG_MIN = 0x10

const CHANNEL_CTRL_TAG_MIN = 0x30

const TAG = {
  // ─── Wire-level control (no ix) ───
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
  /** Goes ahead of a frame of no fixed size that another follows in its message. Its seq: that frame's bytes (see
   *  `messageFrames`). */
  LENGTH: 0x0d as const,

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
  /** Goes right before a PUBLISH or PUBLISH_BINARY on a subscriber's wire and names that subscriber's channel and seq
   *  for it: the body is the same bytes for every subscriber, so its own index and seq are 0. */
  PUBLISH_TO: 0x19 as const,

  // ─── Per-channel control (carries ix) ───
  /** A close request. Sequenced, so it replays like data. */
  CLOSE: 0x30 as const,
  /** Sequenced, as CLOSE is. */
  CLOSE_ACK: 0x31 as const,
  /** Channel closed with an abort value (analogous to `throw Abort()`). Sequenced, as CLOSE is. */
  ABORT: 0x32 as const,
  /** Channel closed with an error, either way; a page's is `LOST`. Payload: u8 `ERROR_REASON`. Sequenced, as CLOSE. */
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
  /** Server → client: an initial channel a RECONCILED left out, attached at `lastSeq` or not within
   *  `CHANNEL_CONNECT_TTL_MS`. */
  ATTACH_RESULT: 0x3b as const,
}

function isWireCtrlTag(tag: number): boolean {
  return tag < DATA_TAG_MIN
}

function isChannelCtrlTag(tag: number): boolean {
  return tag >= CHANNEL_CTRL_TAG_MIN
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

/** The replay queue of a sequenced frame; `closing` holds the frame that ends a channel, which no byte budget drops. */
type ReplayKind = 'text' | 'binary' | 'closing'

function replayKindOf(tag: number): ReplayKind {
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
