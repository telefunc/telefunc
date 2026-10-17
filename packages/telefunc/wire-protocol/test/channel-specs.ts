export { flowOf, produce, reconciled, run }

import { vi } from 'vitest'

import {
  CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
  CHANNEL_IDLE_TIMEOUT_MS,
  CHANNEL_PING_INTERVAL_MS,
  CHANNEL_RECONNECT_TIMEOUT_MS,
  CHANNEL_SERVER_REPLAY_BUFFER_BINARY_BYTES,
  CHANNEL_SERVER_REPLAY_BUFFER_BYTES,
  CHANNEL_TRANSPORT,
  SSE_FLUSH_THROTTLE_MS,
  SSE_POST_IDLE_FLUSH_DELAY_MS,
  WIRE_MAX_RAW_FRAME_BYTES,
} from '../constants.js'
import type { ReconciledPayload } from '../shared-ws.js'

const run = (ms: number) => vi.advanceTimersByTimeAsync(ms)

/** `for (;;) await channel.send(next)`, the channel page's backpressure loop, counting what it has sent. */
function produce<T = number>(
  channel: { send(data: T): Promise<void>; isClosed: boolean },
  { message = (n) => n as T, onSent }: { message?: (n: number) => T; onSent?: (count: number) => void } = {},
) {
  const sent = { count: 0 }
  void (async () => {
    while (!channel.isClosed) {
      const sending = channel.send(message(sent.count++))
      onSent?.(sent.count)
      await sending
    }
  })().catch(() => {})
  return sent
}

const flowOf = (channel: unknown) => (channel as { _flow: { msgWindow: number; byteWindow: number } })._flow

/** A RECONCILED from a server with the default channel config. */
function reconciled(payload: Pick<ReconciledPayload, 'sessionId' | 'open'> & Partial<ReconciledPayload>) {
  return {
    reconnectTimeout: CHANNEL_RECONNECT_TIMEOUT_MS,
    idleTimeout: CHANNEL_IDLE_TIMEOUT_MS,
    pingInterval: CHANNEL_PING_INTERVAL_MS,
    maxFrameBytes: WIRE_MAX_RAW_FRAME_BYTES,
    serverReplayBuffer: CHANNEL_SERVER_REPLAY_BUFFER_BYTES,
    serverReplayBufferBinary: CHANNEL_SERVER_REPLAY_BUFFER_BINARY_BYTES,
    clientReplayBuffer: CHANNEL_CLIENT_REPLAY_BUFFER_BYTES,
    clientReplayBufferBinary: CHANNEL_CLIENT_REPLAY_BUFFER_BINARY_BYTES,
    sseFlushThrottle: SSE_FLUSH_THROTTLE_MS,
    ssePostIdleFlushDelay: SSE_POST_IDLE_FLUSH_DELAY_MS,
    transports: [CHANNEL_TRANSPORT.SSE],
    ...payload,
  } satisfies ReconciledPayload
}
