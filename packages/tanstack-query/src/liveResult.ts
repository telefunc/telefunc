// Internal module (NOT a package export): the live-query result-hook logic + its injectable seams.
// The public `./server` entry (server.ts) only registers a hook built from `realDeps`; specs import
// this module directly so no test-only surface ships on the published entry.
export { EXTENSION_NAME, LIVE_QUERY_INFLIGHT_KEY, buildLiveResult, makeLiveResultHook, realDeps }
export type { LiveChannel, ServerDeps }

import { Channel, reserveLiveQuery, takeLiveSources } from 'telefunc'
import type { ClientChannel, LiveSource, Reservation } from 'telefunc'
import { __TQ__CHANNEL_KEY, __TQ__DATA_KEY, type TelefuncLiveWrapper } from './shared.js'

const EXTENSION_NAME = '@telefunc/tanstack-query'

// No connection identity exists at result-hook time — the mux attaches the channel to a
// connection later, at client reconcile. So the in-flight reservation ledger keys on this fixed
// process constant; its bound (`config.channel.liveQueryPerConnection`) caps concurrent
// AWAITING-ATTACH live channels process-wide. The true per-connection cap on attached channels is
// enforced later, at attach, in the mux.
const LIVE_QUERY_INFLIGHT_KEY = '__telefunc_tanstack_live_query_inflight__'

/** The channel shape the hook depends on — the real `Channel` satisfies it, and specs inject a
 *  fake exposing `send`/`onOpen`/`onClose`/`close`/`client`. */
type LiveChannel = {
  send: (message: 'invalidate') => unknown
  onOpen: (callback: () => void) => void
  onClose: (callback: () => void) => void
  close: () => unknown
  client: ClientChannel<never, 'invalidate'>
}

/** Injectable seams so the hook logic is unit-testable with fake sources/limiter/channel. */
type ServerDeps = {
  takeLiveSources: () => LiveSource[]
  reserveLiveQuery: (key: string) => Reservation
  createChannel: () => LiveChannel
}

const realDeps: ServerDeps = {
  takeLiveSources,
  reserveLiveQuery,
  createChannel: () => new Channel<never, 'invalidate'>(),
}

/** Build the always-run result hook from a set of deps. The hook reads ONLY `ctx.result`;
 *  `ctx.data` (the request-carried, forgeable extension payload) is never consulted for liveness. */
function makeLiveResultHook(deps: ServerDeps): (ctx: { result: unknown; data?: unknown }) => unknown {
  return (ctx) => buildLiveResult(ctx.result, deps)
}

/** Turn a settled telefunction result into a live wrapper when the request registered live
 *  sources; otherwise return the result untouched. */
function buildLiveResult(result: unknown, deps: ServerDeps): unknown {
  const sources = deps.takeLiveSources()
  if (sources.length === 0) return result // no live sources → no wrapper

  // Reserve BEFORE acquiring the channel. At the in-flight ceiling this throws
  // `LiveQuotaExceededError`; the hook then disposes the sources it now owns (attempt-all) and
  // lets the typed error surface — no channel, no wrapper.
  const reservation = reserveOrDispose(sources, deps)

  const { channel, teardowns } = subscribeAll(sources, reservation, deps)

  // The reservation covers only the awaiting-attach window: release at attach OR permanent close,
  // whichever comes first (both idempotent).
  reservation.transferToChannel(channel)
  channel.onOpen(() => reservation.release())
  // Every subscription is torn down when the channel permanently closes.
  channel.onClose(() => attemptAll(teardowns))

  return { [__TQ__DATA_KEY]: result, [__TQ__CHANNEL_KEY]: channel.client } satisfies TelefuncLiveWrapper
}

function reserveOrDispose(sources: LiveSource[], deps: ServerDeps): Reservation {
  try {
    return deps.reserveLiveQuery(LIVE_QUERY_INFLIGHT_KEY)
  } catch (err) {
    for (const source of sources) attempt(() => source.dispose?.())
    throw err // LiveQuotaExceededError surfaces, unmasked by cleanup
  }
}

/** Create the single channel and subscribe every source to it. On any failure, unwind so nothing
 *  leaks: tear down the sources that subscribed, dispose the throwing + not-yet-attempted sources,
 *  release the reservation, close the channel — then rethrow. */
function subscribeAll(
  sources: LiveSource[],
  reservation: Reservation,
  deps: ServerDeps,
): { channel: LiveChannel; teardowns: Array<() => void> } {
  const teardowns: Array<() => void> = []
  let channel: LiveChannel | undefined
  try {
    channel = deps.createChannel()
    const sendInvalidate = makeCoalescedSend(channel)
    for (const source of sources) {
      // `subscribe` may invoke `sendInvalidate` synchronously (fenced replay); the send rides the
      // channel's pre-peer buffer until attach.
      teardowns.push(source.subscribe(sendInvalidate))
    }
    return { channel, teardowns }
  } catch (err) {
    // `subscribe()` failure is atomic w.r.t. delivery resources: the source at index
    // `teardowns.length` is the one that threw (it returned no teardown) → dispose it, do not tear
    // it down. Sources before it subscribed successfully → tear down. Sources after it were never
    // attempted → dispose.
    const throwingIndex = teardowns.length
    attemptAll(teardowns)
    for (let i = throwingIndex; i < sources.length; i++) {
      const source = sources[i]
      if (source) attempt(() => source.dispose?.())
    }
    attempt(() => reservation.release())
    const created = channel
    if (created) attempt(() => created.close())
    throw err
  }
}

/** Any source firing coalesces to ONE `send('invalidate')` per microtask window. */
function makeCoalescedSend(channel: { send: (message: 'invalidate') => unknown }): () => void {
  let scheduled = false
  return () => {
    if (scheduled) return
    scheduled = true
    queueMicrotask(() => {
      scheduled = false
      attempt(() => channel.send('invalidate'))
    })
  }
}

function attemptAll(fns: Array<() => void>): void {
  for (const fn of fns) attempt(fn)
}

/** Best-effort cleanup step: a throwing teardown/dispose is logged and never aborts the rest. */
function attempt(fn: () => void): void {
  try {
    fn()
  } catch (err) {
    console.error('[telefunc:tanstack-query] live-source cleanup threw', err)
  }
}
