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

/** Marker key used on the ack payload of a server-returned function when its arg shield
 *  rejects the incoming args. The client-side reviver detects this and throws on the
 *  caller's side — keeps the channel oblivious to shield semantics. */
export const FN_SHIELD_ERROR_KEY = '__telefunc_fn_shield_error'

// ===== Channel pump frame tags =====

/** 1-byte tag prefixed to every binary send in per-channel streaming pumps. */
export const CHANNEL_PUMP_TAG_DATA = 0x00
export const CHANNEL_PUMP_TAG_ERROR = 0x01

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

/** How long phase 1 of the upgrade drain waits for natural SSE outbox drain before
 *  phase 2 gates the producer and forces the cutover. */
export const UPGRADE_DRAIN_TIMEOUT_MS = 2_000

/** Deadline for the FIN(old wire) + RECONCILED(new wire) join that commits the handoff. Armed when
 *  the handoff is ENTERED, not when the first limb arrives — arming it on FIN leaves a MISSING FIN
 *  unbounded, which is the one case it exists to catch. */
export const UPGRADE_HANDOFF_JOIN_TIMEOUT_MS = 2_000

/** ONE budget shared across BOTH handoff buffers (old + new); either limit trips the same fallback
 *  as a missing FIN. Frames are never evicted to stay under it — eviction would silently drop
 *  precisely the non-replayable old-wire frames the source partitioning exists to protect. */
export const UPGRADE_HANDOFF_BUFFER_BYTES = 8 * 1024 * 1024
export const UPGRADE_HANDOFF_BUFFER_FRAMES = 4_096

/** CLIENT-side deadline for one attempt, armed at the `PREPARE` and disarmed at handoff entry
 *  (excluding the probe, which `WS_PROBE_TIMEOUT_MS` bounds). Not redundant with the server's stage
 *  TTL: a barrier the server finds STALE is refused SILENTLY, so nothing on the wire would otherwise
 *  ever end such an attempt. From handoff entry on, the join timeout takes over. */
export const UPGRADE_ATTEMPT_TIMEOUT_MS = 10_000

/** SERVER-side stage lifetime, from accepted PREPARE to commit. NON-refreshing on purpose: PING
 *  resets only the liveness timer, so a client that pings but never barriers would otherwise hold
 *  its stage forever. On expiry the old SSE session is left completely untouched. */
export const UPGRADE_STAGE_TTL_MS = 10_000

/** Admission caps for PREPARE and barrier RECONCILE frames ONLY — an ordinary RECONCILE keeps its
 *  uncapped contract. The entry/id caps bound what the JSON decoder allocated, which the byte cap
 *  alone does not: 1024 ids of 256 B is a very different object graph from one 256 KiB id. */
export const UPGRADE_MAX_FRAME_BYTES = 256 * 1024
export const UPGRADE_MAX_OPEN_ENTRIES = 1_024
export const UPGRADE_MAX_ID_BYTES = 256

/** Global staged budget per server instance. Both units are needed — thousands of empty stages still
 *  cost sockets, timers and closures. At capacity only the NEW probe is rejected. */
export const UPGRADE_MAX_STAGED_RECORDS = 1_024
export const UPGRADE_MAX_STAGED_BYTES = 64 * 1024 * 1024

// ===== Inbound wire resource caps =====
//
// Not upgrade-specific: these sit at the two inbound entry points every frame crosses. Nothing here
// bounds hostile APPLICATION memory — flow-control credit is cooperative at the sender — which is a
// documented residual of the design, not an oversight.

/** Ceiling on ONE raw wire frame, enforced before decode. Both inbound paths need it for different
 *  reasons: an SSE frame is rejected by its DECLARED length before a body byte is pulled, while a WS
 *  binary message has already been read and is capped against the cost of DECODING it. 64 MiB is the
 *  protocol's maximum adaptive byte credit, so no compliant frame comes near it. */
export const WIRE_MAX_RAW_FRAME_BYTES = 64 * 1024 * 1024

/** Ceiling on the raw frames queued on ONE connection's recv chain — bytes AND count, because a
 *  flood of tiny frames costs closures and promise links rather than bytes. A frame parked behind an
 *  awaited turn is retained by the chain's closure until that turn completes, so without this a
 *  client that stalls the chain and keeps sending has an unbounded holding area. Overflow terminates
 *  that wire and any upgrade staged on it, never any other. */
export const WIRE_MAX_RECV_BACKLOG_BYTES = 64 * 1024 * 1024
export const WIRE_MAX_RECV_BACKLOG_FRAMES = 50_000

/** Ceiling on an SSE POST's metadata header — a tiny JSON object (`connId` plus two booleans), so
 *  the cap is generous by three orders of magnitude and no compliant request approaches it. It gets
 *  its own, much smaller ceiling than a frame because it is read FIRST, on every POST shape, ahead
 *  of every validation: it is the earliest allocation an unauthenticated request can drive. */
export const SSE_METADATA_MAX_BYTES = 64 * 1024

/** How long the client waits for RECONCILED after sending a RECONCILE before declaring the
 *  wire dead and reconnecting. A downstream that stalls without erroring (bytes stop, no FIN)
 *  otherwise wedges the connection: the upstream keeps sending pings but `handlePongTimeout`
 *  is suppressed while reconciling, so nothing notices the dead wire and every call buffered
 *  behind the un-acked RECONCILE hangs. */
export const RECONCILE_TIMEOUT_MS = 10_000

// ===== Multiplexed SSE transport =====

/** Shorter flush delay for the first SSE batch POST after an idle period. */
export const SSE_POST_IDLE_FLUSH_DELAY_MS = 50

/** Idle window used to decide whether the next upstream SSE batch is post-idle. */
export const SSE_FLUSH_THROTTLE_MS = 300

/** Latest-send deadline for SSE reconcile batches so immediate channel activity can coalesce into one POST. */
export const SSE_RECONCILE_DEADLINE_MS = 10

// ===== Channel transport defaults =====

export const CHANNEL_RECONNECT_TIMEOUT_MS = 60_000
export const CHANNEL_IDLE_TIMEOUT_MS = 60_000
export const CHANNEL_PING_INTERVAL_MS = 5_000
export const CHANNEL_PING_INTERVAL_MIN_MS = 1_000
export const CHANNEL_CLOSE_TIMEOUT_MS = 5_000
/** Per-channel replay buffer for text frames kept on the server for reconnect recovery. */
export const CHANNEL_SERVER_REPLAY_BUFFER_BYTES = 256 * 1024
/** Per-channel replay buffer for binary frames kept on the server for reconnect recovery. */
export const CHANNEL_SERVER_REPLAY_BUFFER_BINARY_BYTES = 2 * 1024 * 1024
/** Per-channel replay buffer for text frames advertised to the client for reconnect replay. */
export const CHANNEL_CLIENT_REPLAY_BUFFER_BYTES = 1024 * 1024
/** Per-channel replay buffer for binary frames advertised to the client for reconnect replay. */
export const CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES = 2 * 1024 * 1024
/**
 * Maximum bytes buffered per channel for text messages sent before a peer connects.
 * When the budget is exceeded the oldest entries are evicted (FIFO) so the
 * channel stays alive and memory stays bounded.
 */
export const CHANNEL_BUFFER_LIMIT_BYTES = 512 * 1024
/** Maximum bytes buffered per channel for binary messages sent before a peer connects. */
export const CHANNEL_BUFFER_LIMIT_BINARY_BYTES = 2 * 1024 * 1024

/** How long a channel waits for a peer to connect after the server→client
 *  HTTP response carrying `channel.client` has been serialized. */
export const CHANNEL_CONNECT_TTL_MS = 5_000

// Client-side channel reconnect defaults
export const CHANNEL_RECONNECT_INITIAL_DELAY_MS = 500
export const CHANNEL_RECONNECT_MAX_DELAY_MS = 5_000

// ===== Credit-based flow control =====
//
// Peak throughput across a credit-based wire is `W / RTT` — once a sender exhausts `W`
// it must wait one round-trip for the receiver's credit refresh. Sizing `W` to the
// link's BDP is the only way to avoid this stall (no algorithm beats W/RTT). Rather
// than picking a single fixed `W`, the receiver maintains a gRPC-style BDP estimator
// (see `bdp-estimator.ts`) that probes the in-flight byte count once per RTT and
// doubles `W` whenever the sample saturates ≥ 2/3 of the current window. Idle channels
// stay at the small initial cost; fat-pipe channels climb to the cap within a handful
// of RTTs.
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
//   PUBLISH, PUBLISH_BINARY   broadcast fan-out, separate flow control entirely.
//
// Window semantics — `WINDOW` frame advertises an absolute value (not additive like
// HTTP/2). Sender resets `_peerWindow` to `CREDIT_WINDOW_INITIAL_BYTES` on transport
// reattach; receiver preserves its grown `W` across reconnect (BDP is a property of
// the path, not of any single wire instance — slight divergence from gRPC's
// per-connection reset, acceptable for typical transport hiccups).

/** Initial credit window — sized so a typical ~MB-scale burst doesn't stall on
 *  the BDP ramp-up. Grows further via the estimator up to `CREDIT_WINDOW_MAX_BYTES`. */
export const CREDIT_WINDOW_INITIAL_BYTES = 2 * 1024 * 1024

/** Initial window when SSE upstream has fallen back to per-frame POSTs — each
 *  POST has its own RTT, so a small window would spend it on flow-control alone. */
export const CREDIT_WINDOW_INITIAL_BYTES_BATCH = 8 * 1024 * 1024

/** Hard cap on the adaptive credit window — bounds per-channel buffering worst case.
 *  Covers ~5 Gbit/s × 100 ms RTT or ~500 Mbit/s × 1 s RTT. */
export const CREDIT_WINDOW_MAX_BYTES = 64 * 1024 * 1024

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

/** Self-utilisation threshold above which the flow-control sender yields a
 *  macrotask before its next send, and above which the receiver-side BDP gate
 *  refuses to grow either window. Half the loop reserved for everything else
 *  (UI, other channels, GC). Higher = throughput-first; lower = realtime-first.
 *
 *  Note: this measures *our* CPU share specifically. An unrelated UI library
 *  hogging the loop doesn't affect this value, so we don't falsely throttle
 *  telefunc when the choke is elsewhere. */
export const FC_SELF_UTIL_THRESHOLD = 0.5

// ===== Session routing =====

/** User-facing header for sticky session routing (opaque token). */
export const TELEFUNC_SESSION_HEADER = 'x-telefunc-session'
