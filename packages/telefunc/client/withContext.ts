export { withContext, getPendingContext }
export type { ClientCallContext, StreamTransport }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import type { StreamTransport, ChannelTransports } from '../wire-protocol/constants.js'
import type { TelefuncExtensionRegistry } from '../node/server/extensions.js'

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
}

/** Per-call context options for the HTTP transport layer. */
type ClientCallContext = {
  /** AbortSignal to cancel the telefunc call. */
  signal?: AbortSignal
  /** Additional HTTP headers for this call. */
  headers?: Record<string, string>
  /** Streamed-value transport overrides for this call. */
  stream?: StreamCallContext
  /** Channel transport overrides for this call. */
  channel?: ChannelCallContext
  /** Per-call extension data, keyed by extension name. */
  extensions?: { [K in keyof TelefuncExtensionRegistry]?: TelefuncExtensionRegistry[K] }
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
