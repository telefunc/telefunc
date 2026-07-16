// Internal module (NOT a package export): the live-query result-hook logic + its injectable seams.
// The public `./server` entry (server.ts) only registers a hook built from `realDeps`; specs import
// this module directly so no test-only surface ships on the published entry.
export { EXTENSION_NAME, buildLiveResult, makeLiveResultHook, realDeps }
export type { LiveChannel, ServerDeps }

import { Channel, takeLiveSources } from 'telefunc'
import type { ClientChannel, LiveSource } from 'telefunc'
import { __TQ__CHANNEL_KEY, __TQ__DATA_KEY, type TelefuncLiveWrapper } from './shared.js'

const EXTENSION_NAME = '@telefunc/tanstack-query'

/** The channel shape the hook depends on — the real `Channel` satisfies it, and specs inject a
 *  fake exposing `send`/`onOpen`/`onClose`/`close`/`client`. */
type LiveChannel = {
  send: (message: 'invalidate') => unknown
  onOpen: (callback: () => void) => void
  onClose: (callback: () => void) => void
  close: () => unknown
  client: ClientChannel<never, 'invalidate'>
}

/** Injectable seams so the hook logic is unit-testable with fake sources/channel. */
type ServerDeps = {
  takeLiveSources: () => LiveSource[]
  createChannel: () => LiveChannel
}

const realDeps: ServerDeps = {
  takeLiveSources,
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

  const { channel, teardowns } = subscribeAll(sources, deps)

  // Every subscription is torn down when the channel permanently closes.
  channel.onClose(() => attemptAll(teardowns))

  return { [__TQ__DATA_KEY]: result, [__TQ__CHANNEL_KEY]: channel.client } satisfies TelefuncLiveWrapper
}

/** Create the single channel and subscribe every source to it. On any failure, unwind so nothing
 *  leaks: tear down the sources that subscribed, dispose the throwing + not-yet-attempted sources,
 *  close the channel — then rethrow. */
function subscribeAll(sources: LiveSource[], deps: ServerDeps): { channel: LiveChannel; teardowns: Array<() => void> } {
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
