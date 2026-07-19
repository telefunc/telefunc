export { withContext, withContextChecked, getPendingContext }
export type { ClientCallContext, StreamTransport }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import { contextConsumerOf } from './callAttribution.js'
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
  return ((...args: any[]) => withContextChecked(telefunc, context, ...(args as Parameters<F>)).result) as F
}

/** `withContext`, plus the answer to "is the value I got back the call that took my context?".
 *
 *  `@internal` — reachable from `telefunc/client` because the TanStack adapter needs it at runtime, but not
 *  part of the supported API and free to change shape.
 *
 *  Calls `telefunc(...args)` inside the pending-context window and reports whether the RETURNED value is a
 *  telefunction call that consumed THIS context. A caller that must not silently lose the context — e.g. a
 *  query adapter attaching a cancellation signal — can then fail loudly instead of issuing a request that
 *  quietly ignores it.
 *
 *  ATTRIBUTION, not a side effect. `getPendingContext()` nulls the slot as it reads, so "the slot no longer
 *  holds our context" proves something consumed it — but not what, and that gap is a real hole rather than a
 *  pedantic one: a closure that calls one telefunction, drops it, and returns a different value would pass a
 *  consumption test while handing back a call the signal never reached. Nested checked calls fail the same
 *  way, the inner one satisfying the outer one's test. So the question asked here is about the returned
 *  value itself (`callAttribution.ts`), and anything unattributable — a plain value, a promise from an async
 *  wrapper, a call stamped with somebody else's context — reads as false. It FAILS CLOSED by construction.
 *
 *  A counter over the pending slot would not have fixed this and cannot be used anyway: `getGlobalObject` is
 *  keyed by filename, so a second copy of this module reuses whichever object registered the key first and
 *  any field added later is silently missing from it.
 *
 *  `consumed` is true for a direct telefunction call and for a SYNCHRONOUS wrapper that RETURNS one
 *  (`() => onGetPosts(id)`); false when the function returns anything else, including when it reaches a
 *  telefunction only after an `await` — by which time the window has closed. */
function withContextChecked<F extends (...args: any[]) => any>(
  telefunc: F,
  context: ClientCallContext,
  ...args: Parameters<F>
): { result: ReturnType<F>; consumed: boolean } {
  globalObject.pendingContext = context
  try {
    const result = telefunc(...args)
    return { result, consumed: contextConsumerOf(result) === context }
  } finally {
    globalObject.pendingContext = null
  }
}

// Global because the caller may wrap the telefunc in a closure — e.g. `() => onGetPosts()` —
// and there's no way to thread context through an arbitrary wrapper to the generated stub.
function getPendingContext(): ClientCallContext | null {
  const ctx = globalObject.pendingContext
  globalObject.pendingContext = null
  return ctx
}
