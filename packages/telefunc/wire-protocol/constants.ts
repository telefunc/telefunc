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

/** Transport modes for streaming values. */
export const TRANSPORT = {
  STREAM: 'stream',
  SSE: 'sse',
  WS: 'ws',
} as const
export type Transport = (typeof TRANSPORT)[keyof typeof TRANSPORT]
export const DEFAULT_TRANSPORT: Transport = TRANSPORT.STREAM

// ===== WS connection defaults =====

export const WS_RECONNECT_TIMEOUT = 60_000
export const WS_IDLE_TIMEOUT = 60_000
export const WS_PING_INTERVAL = 5_000
export const WS_PING_INTERVAL_MIN = 1_000
export const WS_SERVER_REPLAY_BUFFER = 256 * 1024
export const WS_CLIENT_REPLAY_BUFFER = 1024 * 1024
/**
 * Maximum bytes buffered per channel for messages sent before a peer connects.
 * When the budget is exceeded the oldest entries are evicted (FIFO) so the
 * channel stays alive and memory stays bounded.
 */
export const WS_CHANNEL_SEND_BUFFER = 512 * 1024

/**
 * How long (ms) a channel waits for a peer to connect after the server→client
 * HTTP response carrying `channel.client` has been serialized.
 *
 * This is intentionally the same as the reconcile defer timeout (`channelConnectTtl`
 * in `getTelefuncChannelHooks` options): both cap the same real-world window —
 * the time between the telefunction returning its result and the client completing
 * its first WS reconcile for that channel.
 */
export const WS_CHANNEL_CONNECT_TTL_MS = 5_000

// Client-side WS connection defaults
export const WS_RECONNECT_INITIAL_DELAY = 500
export const WS_RECONNECT_MAX_DELAY = 5_000
