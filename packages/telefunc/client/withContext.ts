export { withContext }
export type { ClientCallContext, StreamTransport }

import type { StreamTransport, ChannelTransports } from '../wire-protocol/constants.js'

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
    ;(telefunc as any)._context = context
    try {
      return telefunc(...args)
    } finally {
      ;(telefunc as any)._context = undefined
    }
  }) as F
}
