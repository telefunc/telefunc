export const SERIALIZER_PREFIX_FILE = '!TelefuncFile:'
export const SERIALIZER_PREFIX_BLOB = '!TelefuncBlob:'
export const SERIALIZER_PREFIX_STREAM = '!TelefuncStream:'
export const SERIALIZER_PREFIX_GENERATOR = '!TelefuncGenerator:'
export const SERIALIZER_PREFIX_PROMISE = '!TelefuncPromise:'
export const SERIALIZER_PREFIX_CHANNEL = '!TelefuncChannel:'
export const SERIALIZER_PREFIX_FUNCTION = '!TelefuncFunction:'

// ===== WS transport =====

/** JSON key for the frame channel injected into WS transport responses. */
export const FRAME_CHANNEL_KEY = '__frameChannel'

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

/** Transport for persistent channels created with `createChannel()`. */
export type ChannelTransport = (typeof CHANNEL_TRANSPORT)[keyof typeof CHANNEL_TRANSPORT]
export const DEFAULT_CHANNEL_TRANSPORT: ChannelTransport = CHANNEL_TRANSPORT.SSE

// ===== Multiplexed SSE transport =====

/** Default shorter flush delay for the first SSE batch POST after an idle period. */
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
export const CHANNEL_SERVER_REPLAY_BUFFER_BYTES = 256 * 1024
export const CHANNEL_CLIENT_REPLAY_BUFFER_BYTES = 1024 * 1024
/**
 * Maximum bytes buffered per channel for messages sent before a peer connects.
 * When the budget is exceeded the oldest entries are evicted (FIFO) so the
 * channel stays alive and memory stays bounded.
 */
export const CHANNEL_BUFFER_LIMIT_BYTES = 512 * 1024

/**
 * How long (ms) a channel waits for a peer to connect after the server→client
 * HTTP response carrying `channel.client` has been serialized.
 *
 */
export const CHANNEL_CONNECT_TTL_MS = 5_000

// Client-side channel reconnect defaults
export const CHANNEL_RECONNECT_INITIAL_DELAY_MS = 500
export const CHANNEL_RECONNECT_MAX_DELAY_MS = 5_000
