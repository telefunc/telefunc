export { withContext, getPendingContext }
export type { ClientCallContext, StreamTransport }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import { assertUsage } from '../utils/assert.js'
import { TIMER_DELAY_MAX_MS } from '../wire-protocol/constants.js'
import type { StreamTransport, ChannelTransports } from '../wire-protocol/constants.js'

const globalObject = getGlobalObject<{ pendingContext: ClientCallContext | null }>('withContext.ts', {
  pendingContext: null,
})

type StreamCallContext = {
  /** Streamed-value transport — overrides `config.stream.transport` for this call. */
  transport?: StreamTransport
}

type ChannelCallContext = {
  /** Channel transports — overrides `config.channel.transports` for this call. */
  transports?: ChannelTransports
  /** Cache key — calls sharing it share a `ClientConnection`; distinct keys are isolated. */
  connectionKey?: string
  /** How long (ms) to keep the underlying transport alive after all channels close. Default: 60 000. Pass 0 to dispose immediately. */
  idleTimeout?: number
}

/** Per-call context options for the HTTP transport layer. */
type ClientCallContext = {
  /** AbortSignal to cancel the telefunc call. */
  signal?: AbortSignal
  /** Additional HTTP headers for this call. */
  headers?: Record<string, string>
  /** Override `config.telefuncUrl` for this call and any of its channels. */
  telefuncUrl?: string
  /** Streamed-value transport overrides for this call. */
  stream?: StreamCallContext
  /** Channel transport overrides for this call. */
  channel?: ChannelCallContext
  /** Per-call extension data, keyed by extension name. */
  extensions?: Record<string, Record<string, unknown>>
}

/** Wrap a telefunc function with per-call context (signal, headers).
 *
 *  ```ts
 *  import { withContext } from 'telefunc/client'
 *  const call = withContext(onLoadTodoItem, { signal, headers: { Priority: 'u=0' } })
 *  const res = await call(id)
 *  ```
 */
function withContext<F extends (...args: any[]) => any>(telefunc: F, context: ClientCallContext): F {
  const idleTimeout = context.channel?.idleTimeout
  assertUsage(
    idleTimeout === undefined ||
      (Number.isSafeInteger(idleTimeout) && idleTimeout >= 0 && idleTimeout <= TIMER_DELAY_MAX_MS),
    `withContext()'s \`channel.idleTimeout\` should be a non-negative safe integer of milliseconds, at most ${TIMER_DELAY_MAX_MS}, the longest a timer waits`,
  )
  return ((...args: any[]) => {
    globalObject.pendingContext = context
    try {
      return telefunc(...args)
    } finally {
      globalObject.pendingContext = null
    }
  }) as F
}

// Global because the caller may wrap the telefunc in a closure — e.g. `() => onGetPosts()` —
// and there's no way to thread context through an arbitrary wrapper to the generated stub.
function getPendingContext(): ClientCallContext | null {
  const ctx = globalObject.pendingContext
  globalObject.pendingContext = null
  return ctx
}
