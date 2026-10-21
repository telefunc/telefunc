import { assert } from '../utils/assert.js'

export const SERIALIZER_PREFIX_FILE = '!TelefuncFile:'
export const SERIALIZER_PREFIX_BLOB = '!TelefuncBlob:'
export const SERIALIZER_PREFIX_FILE_DOWNLOAD = '!TelefuncFileDownload:'
export const SERIALIZER_PREFIX_BLOB_DOWNLOAD = '!TelefuncBlobDownload:'
export const SERIALIZER_PREFIX_STREAM = '!TelefuncStream:'
export const SERIALIZER_PREFIX_GENERATOR = '!TelefuncGenerator:'
export const SERIALIZER_PREFIX_PROMISE = '!TelefuncPromise:'
export const SERIALIZER_PREFIX_CHANNEL = '!TelefuncChannel:'
export const SERIALIZER_PREFIX_FUNCTION = '!TelefuncFunction:'
export const SERIALIZER_PREFIX_BROADCAST = '!TelefuncBroadcast:'
export const SERIALIZER_PREFIX_ROOM_STEM = '!TelefuncRoom'
export const SERIALIZER_PREFIX_ROOM = `${SERIALIZER_PREFIX_ROOM_STEM}:`
export const SERIALIZER_PREFIX_ROOM_PARTICIPANT = `${SERIALIZER_PREFIX_ROOM_STEM}Participant:`
export const SERIALIZER_PREFIX_ROOM_REMOTE = `${SERIALIZER_PREFIX_ROOM_STEM}RemoteParticipant:`

/** Marker key used on the ack payload of a server-returned function when its arg shield
 *  rejects the incoming args. The client-side reviver detects this and throws on the
 *  caller's side — keeps the channel oblivious to shield semantics. */
export const FN_SHIELD_ERROR_KEY = '__telefunc_fn_shield_error'

// ===== Channel pump frame tags =====

/** 1-byte tag prefixed to every binary send in per-channel streaming pumps. */
export const CHANNEL_PUMP_TAG_DATA = 0x00
export const CHANNEL_PUMP_TAG_ERROR = 0x01
/** The producer's end, before its close: a stream whose channel closes without it was cut short. */
export const CHANNEL_PUMP_TAG_END = 0x02

// ===== Streaming error frames =====

/** Marker u32 value used as frame length in streaming responses to signal an error frame. */
export const STREAMING_ERROR_FRAME_MARKER = 0xffffffff

/** Error types used in streaming error frame payloads. */
export const STREAMING_ERROR_TYPE = {
  ABORT: 'abort',
  BUG: 'bug',
} as const
export type StreamingErrorType = (typeof STREAMING_ERROR_TYPE)[keyof typeof STREAMING_ERROR_TYPE]

/** Streaming error frame payload: abort with value. */
export type StreamingErrorFrameAbort = {
  type: typeof STREAMING_ERROR_TYPE.ABORT
  abortValue: unknown
}

/** Streaming error frame payload: unhandled bug (no details sent to client). */
export type StreamingErrorFrameBug = {
  type: typeof STREAMING_ERROR_TYPE.BUG
}

/** Union of all streaming error frame payloads. */
export type StreamingErrorFramePayload = StreamingErrorFrameAbort | StreamingErrorFrameBug

// ===== Transport =====

export const STREAM_TRANSPORT = {
  BINARY_INLINE: 'binary-inline',
  CHANNEL: 'channel',
  SSE_INLINE: 'sse-inline',
} as const

export const CHANNEL_TRANSPORT = {
  SSE: 'sse',
  WS: 'ws',
} as const

/** Transport for streamed values returned by telefunctions. */
export type StreamTransport = (typeof STREAM_TRANSPORT)[keyof typeof STREAM_TRANSPORT]
export const DEFAULT_STREAM_TRANSPORT: StreamTransport = STREAM_TRANSPORT.BINARY_INLINE

/** Transport for persistent channels created with `new Channel()`. */
export type ChannelTransport = (typeof CHANNEL_TRANSPORT)[keyof typeof CHANNEL_TRANSPORT]
/** Ordered list of transports to use for channels. `['sse', 'ws']` starts on SSE and upgrades to WebSocket. */
export type ChannelTransports = ChannelTransport[]
/** Default transports the client tries, in order. Starts on SSE and upgrades to WS if available. */
export const DEFAULT_CLIENT_CHANNEL_TRANSPORTS: ChannelTransports = [CHANNEL_TRANSPORT.SSE, CHANNEL_TRANSPORT.WS]
/** Default transports enabled on the server. SSE only until a WS adapter calls enableChannelTransports. */
export const DEFAULT_SERVER_CHANNEL_TRANSPORTS: ChannelTransports = [CHANNEL_TRANSPORT.SSE]

/** How long to wait for a WebSocket probe ping/pong before giving up on the upgrade. */
export const WS_PROBE_TIMEOUT_MS = 3_000

/** How long to wait for the stream-request POST handshake ack on the SSE downstream
 *  before declaring the upstream wire dead and falling back to outbox+batch POSTs. */
export const STREAM_REQUEST_HANDSHAKE_TIMEOUT_MS = 3_000

// ===== SSE -> WS upgrade =====

/** Batch mode: the barrier's wait for an outbox drain, before it flushes it or, with a POST out, defers the upgrade. */
export const UPGRADE_DRAIN_TIMEOUT_MS = 2_000

/** Post-flip wait: FIN on the old wire from the flip or its last delivery if later, RECONCILED on the new one. */
export const UPGRADE_HANDOFF_JOIN_TIMEOUT_MS = 2_000

/** Past either bound the upgrade is abandoned rather than letting a stalled join buffer forever. */
export const UPGRADE_HANDOFF_BUFFER_BYTES = 8 * 1024 * 1024
export const UPGRADE_HANDOFF_BUFFER_FRAMES = 4_096

/** Wall-clock bound on one attempt, PREPARE to COMMITTED (see the assertion below). */
export const UPGRADE_ATTEMPT_TIMEOUT_MS = 10_000

/** How long a staged, uncommitted upgrade may hold its session before the probe is dropped. */
export const UPGRADE_STAGE_TTL_MS = 10_000

/** Longest channel id the wire will carry. */
export const UPGRADE_MAX_ID_BYTES = 256

/** Worst case for one open entry beyond its id: its other keys at their largest values, and the separator. */
const RECONCILE_ENTRY_ENVELOPE_BYTES = 124

/** Bounds what unauthenticated PREPARE frames can pin in memory before any of them commits. */
export const UPGRADE_MAX_STAGED_RECORDS = 1_024
export const UPGRADE_MAX_STAGED_BYTES = 64 * 1024 * 1024

// ===== Server ingress bounds =====

/** Largest data-plane frame. Generous because it carries user payloads. */
export const WIRE_MAX_RAW_FRAME_BYTES = 64 * 1024 * 1024

/** The single channels-per-connection limit: the client refuses to open past it, a barrier may
 *  list no more, and the control-frame byte cap below is derived from it. Every reconcile lists
 *  every channel, so this is also what a peer can make the server parse. (The wire could address
 *  65 536 — a u16 index — but that is exhaustion, not a working limit.) */
export const MAX_CHANNELS_PER_CONNECTION = 4_096

/** Largest control frame the server will decode, checked on the raw bytes. Derived, never picked:
 *  the biggest legitimate one lists every channel at the id cap, and a cap that refuses a legal
 *  frame is worse than no cap. */
export const WIRE_MAX_CONN_CTRL_FRAME_BYTES =
  MAX_CHANNELS_PER_CONNECTION * (UPGRADE_MAX_ID_BYTES + RECONCILE_ENTRY_ENVELOPE_BYTES) + 1_024

/** A peer past its connection's allowance of unprocessed frames is terminated: this base, plus a window per channel. */
export const WIRE_RECV_BACKLOG_BASE_BYTES = 64 * 1024 * 1024
export const WIRE_RECV_BACKLOG_BASE_FRAMES = 50_000

/** A server wire holding more for its peer than this base, plus each channel's allowance, is terminated as a loss. */
export const WIRE_SEND_BACKLOG_BASE_BYTES = WIRE_MAX_RAW_FRAME_BYTES

/** Largest SSE request metadata header the server will read off a POST body. */
export const SSE_METADATA_MAX_BYTES = 64 * 1024

/** How long a RECONCILE waits for RECONCILED, and the wire for any frame where tracked, before the page reconnects. */
export const RECONCILE_TIMEOUT_MS = 10_000

// An attempt outliving the reconcile watchdog would let the watchdog drop the wire mid-commit.
assert(UPGRADE_ATTEMPT_TIMEOUT_MS <= RECONCILE_TIMEOUT_MS)

// ===== Multiplexed SSE transport =====

/** Shorter flush delay for the first SSE batch POST after an idle period. */
export const SSE_POST_IDLE_FLUSH_DELAY_MS = 50

/** Idle window used to decide whether the next upstream SSE batch is post-idle. */
export const SSE_FLUSH_THROTTLE_MS = 300

/** A POST out past this plus its bytes at `SSE_POST_MIN_BYTES_PER_S` is lost; generous, as a proxy may hold bodies. */
export const SSE_POST_FLOOR_MS = 30_000
export const SSE_POST_MIN_BYTES_PER_S = 16 * 1024

/** A batch POST is sized to take about this long, so what waits behind it, a window refresh or a RECONCILE, waits as long. */
export const SSE_POST_TARGET_MS = 1500
/** The smallest a batch POST's allowance gets, however slow the uplink. */
export const SSE_POST_MIN_BYTES = 64 * 1024

/** Latest-send deadline for SSE reconcile batches so immediate channel activity can coalesce into one POST. */
export const SSE_RECONCILE_DEADLINE_MS = 10

// ===== Channel transport defaults =====

export const CHANNEL_RECONNECT_TIMEOUT_MS = 60_000
export const CHANNEL_IDLE_TIMEOUT_MS = 60_000
export const CHANNEL_PING_INTERVAL_MS = 5_000
export const CHANNEL_PING_INTERVAL_MIN_MS = 1_000
export const CHANNEL_CLOSE_TIMEOUT_MS = 5_000
/** The longest delay a timer takes: past it, setTimeout fires at once. Bounds every duration a channel waits out. */
export const TIMER_DELAY_MAX_MS = 2 ** 31 - 1

// ===== WebSocket frames in pieces (see `pieces.ts`) =====

/** A receiver acknowledges a frame over this size, or, where its peer holds data back, this many bytes of any frames. */
export const WIRE_PIECES_ACK_BYTES = 16 * 1024
/** The second of a frame's first two pieces with no rate measured: their arrivals' gap measures the rate. */
export const WIRE_PROBE_PAIR_BYTES = 2 * 1024
/** The most a WebSocket's first announced message carries: what a ping deadline carries on any link a PING crosses. */
export const WIRE_PROBE_FIRST_BYTES = 64
/** The least time a probe's measure takes: the coarsest clock a browser gives a page. */
export const WIRE_PROBE_CLOCK_MS = 1
/** A frame goes whole only if it would cross within a ping interval at the fastest measured rate divided by this. */
export const WIRE_PIECE_RATE_MARGIN = 8
/** The most bytes a message of the whole frames sent in one turn carries (see `PieceSender`), each copied into it once. */
export const WIRE_GATHER_BYTES = 64 * 1024
/** An announced message may carry twice the one before it, or this many bytes whatever that carried. */
export const WIRE_ANNOUNCED_MIN_BYTES = 8 * 1024
/** A receiver waits this many times as long for the next announced message as it takes at the last one's rate. */
export const WIRE_ANNOUNCED_WAIT_MARGIN = 2
/** How long a measured rate stands after it last rose: past it, the next measure replaces it, lower or not. */
export const WIRE_PIECE_RATE_WINDOW_MS = 60_000
/** A receiver sends at most one PIECES_ACK this often, covering every frame it took since the last. */
export const WIRE_PIECES_ACK_GAP_MS = 50
/** The same where its peer holds data back by what it acknowledges (see `flowControl/client/SendBudget.ts`). */
export const WIRE_SEND_ACK_GAP_MS = 10
/** Where its peer holds data back, a receiver acks data this soon once `WIRE_SEND_ACK_MIN_BYTES` wait, not 16 KiB. */
export const WIRE_SEND_ACK_MS = 25
/** What such an acknowledgement covers at least: its own bytes stay under 1% of it. */
export const WIRE_SEND_ACK_MIN_BYTES = 2 * 1024
/** The most of a page uplink's rate its acknowledgements of what it receives take: past it, each waits its turn. */
export const WIRE_ACK_SHARE = 0.0025
/** The most its window refreshes take, which move the download's credit. */
export const WIRE_REFRESH_SHARE = 0.0075
/** The least a page's server may leave unacknowledged while the page hands its WebSocket data. */
export const WIRE_SEND_AHEAD_MIN_BYTES = 64 * 1024
/** A server hands its socket data while its path holds less than the page takes in this many least round trips. */
export const WIRE_SEND_AHEAD_ROUND_TRIPS = 4
/** How long a page's data frame may wait in its wire's queues, as acknowledgements measure it, before it holds back. */
export const WIRE_QUEUE_DELAY_MS = 300
/**
 * Maximum bytes buffered per channel for text messages sent before a peer connects.
 * When the budget is exceeded the oldest entries are evicted (FIFO) so the
 * channel stays alive and memory stays bounded.
 */
export const CHANNEL_BUFFER_LIMIT_BYTES = 512 * 1024
/** Maximum bytes buffered per channel for binary messages sent before a peer connects. */
export const CHANNEL_BUFFER_LIMIT_BINARY_BYTES = 2 * 1024 * 1024
/** How long a Broadcast publish or a Room commit waits for this instance's new subscriptions on its key or lane. */
export const ESTABLISH_HOLD_MS = 60_000

/** How long a channel waits for its peer to connect: it outlasts a page's noticing its wire died, and a reconnect. */
export const CHANNEL_CONNECT_TTL_MS = 15_000

// Client-side channel reconnect defaults
export const CHANNEL_RECONNECT_INITIAL_DELAY_MS = 500
export const CHANNEL_RECONNECT_MAX_DELAY_MS = 5_000

// ===== Credit-based flow control =====
//
// Peak throughput across a credit-based wire is `W / RTT` — once a sender exhausts `W`
// it must wait one round-trip for the receiver's credit refresh. Sizing `W` to the
// link's BDP is the only way to avoid this stall (no algorithm beats W/RTT). Rather
// than picking a single fixed `W`, the receiver maintains a gRPC-style BDP estimator
// (see `flowControl/BdpEstimator.ts`) that probes the in-flight byte count once per RTT and
// doubles `W` whenever the sample saturates ≥ 2/3 of the current window, unless a queue
// the probe's ack waited behind accounts for it. Idle channels stay at the small initial
// cost; fat-pipe channels climb to the cap within a handful of RTTs.
//
// Scope — which frames credit governs:
//
//   TEXT, BINARY (no-ack)     credit-counted on send, BDP-tracked on receive
//   *_ACK_REQ variants        NOT credit-counted — kept out of scope to avoid
//                             overloading `await send({ack:true})`. If credit gated
//                             the ack Promise too, the caller's await would block on
//                             ack arrival AND credit refresh, which is confusing for
//                             users. `await` is already the natural backpressure
//                             signal here. Revisit if real workloads need it.
//                             Also NOT tracked via `FlowControl.onConsumed` on
//                             receive — those bytes were never in the sender's
//                             credit budget, so counting them would advertise
//                             free buffer the receiver doesn't actually have.
//   ACK_RES                   bypasses credit on send to prevent request/response
//                             deadlocks (server with depleted credit could otherwise
//                             never reply to a client that's waiting for that reply
//                             to free credit).
//   PUBLISH, PUBLISH_BINARY   a BroadcastChannel's or a Room's fan-out: counted in
//                             bytes only, taking no message credit and starting no
//                             BDP probe, against the largest byte window the page
//                             grants, from the start. Nothing waits on them: the
//                             count tells the server how far behind the page is,
//                             past which it closes the channel they ride.
//
// Window semantics: `WINDOW` and `MSG_WINDOW` advertise cumulative limits, as QUIC's MAX_DATA does.
//
// Replay: each end keeps what it sent on a channel until a `WINDOW` acknowledges it, to replay after a reconnect.

/** Initial credit window — sized so a typical ~MB-scale burst doesn't stall on
 *  the BDP ramp-up. Grows further via the estimator up to `CREDIT_WINDOW_MAX_BYTES`. */
export const CREDIT_WINDOW_INITIAL_BYTES = 2 * 1024 * 1024

/** Initial window when SSE upstream has fallen back to per-frame POSTs — each
 *  POST has its own RTT, so a small window would spend it on flow-control alone. */
export const CREDIT_WINDOW_INITIAL_BYTES_BATCH = 8 * 1024 * 1024

/** Hard cap on the adaptive credit window — bounds per-channel buffering worst case.
 *  Covers ~5 Gbit/s × 100 ms RTT or ~500 Mbit/s × 1 s RTT. */
export const CREDIT_WINDOW_MAX_BYTES = 64 * 1024 * 1024

/** The most a batch POST is sized to carry (see `SSE_POST_TARGET_MS`). */
export const SSE_POST_MAX_BYTES = CREDIT_WINDOW_MAX_BYTES / 2

/** Each lane of a channel's replay buffer, on both ends: twice `CREDIT_WINDOW_MAX_BYTES` (see `replayWindow`). */
export const CHANNEL_SERVER_REPLAY_BUFFER_BYTES = 2 * CREDIT_WINDOW_MAX_BYTES
export const CHANNEL_SERVER_REPLAY_BUFFER_BINARY_BYTES = 2 * CREDIT_WINDOW_MAX_BYTES
export const CHANNEL_CLIENT_REPLAY_BUFFER_BYTES = 2 * CREDIT_WINDOW_MAX_BYTES
export const CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES = 2 * CREDIT_WINDOW_MAX_BYTES

/** Initial per-channel message-count credit. Independent of the byte budget:
 *  many tiny frames within a normal byte window can still bury the receive loop
 *  in dispatch work. Grows alongside the byte window from the same BDP probe —
 *  the receiver derives bytes-in-flight and msgs-in-flight samples per RTT,
 *  each saturating its own window independently. */
export const CREDIT_MSG_WINDOW_INITIAL = 100

/** Hard cap on the adaptive message-count window. The self-utilisation gate in
 *  `FlowControl.onPingAck` stops growth long before this on hosts that can't
 *  sustain the dispatch rate. */
export const CREDIT_MSG_WINDOW_MAX = 50_000

/** Floor on the gap between consecutive BDP probes. On a fat-pipe-long-latency link
 *  RTT alone paces the probes; on loopback (RTT ≈ 1 ms) this prevents thousands of
 *  ping round-trips per second of event-loop churn for no estimator benefit. */
export const BDP_PING_MIN_INTERVAL_MS = 50

/** Ceiling for the adaptive probe interval — keeps a converged link's BDP probes
 *  rare without ever freezing them, so a rate change is rediscovered within this. */
export const BDP_PING_MAX_INTERVAL_MS = 10_000

/** Rolling window over which `FlowControl` measures its own contribution to the
 *  event loop (telefunc-side self-time vs wall-clock). 1 s captures recent
 *  pressure without lingering on stale history. */
export const FC_SELF_TIME_WINDOW_MS = 1000

/** How much a page's channel sends between macrotask yields, so its browser keeps the wire fed (see `FlowControl`). */
export const FC_PAGE_YIELD_BYTES = 64 * 1024

/** Self-utilisation threshold above which the flow-control sender yields a
 *  macrotask before its next send, and above which the receiver-side BDP gate
 *  refuses to grow either window. Half the loop reserved for everything else
 *  (UI, other channels, GC). Higher = throughput-first; lower = realtime-first.
 *
 *  Note: this measures *our* CPU share specifically. An unrelated UI library
 *  hogging the loop doesn't affect this value, so we don't falsely throttle
 *  telefunc when the choke is elsewhere. */
export const FC_SELF_UTIL_THRESHOLD = 0.5
