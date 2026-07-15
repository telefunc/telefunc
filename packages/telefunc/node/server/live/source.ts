export { addLiveSource, takeLiveSources, disposeUntakenLiveSources }
export type { LiveSource }

import { assertUsage } from '../../../utils/assert.js'
import { getRawContext } from '../context/context.js'

/** A server-owned source of invalidations for one live query. Identity is derived server-side
 *  (from `liveTag()` or the executed SQL) — the client never names it. `subscribe` begins
 *  delivering invalidations and returns a teardown; `dispose` releases resources acquired at
 *  registration for a source that is never subscribed. A producer that acquires no resource at
 *  registration omits `dispose`. */
type LiveSource = {
  identity: string
  subscribe(onInvalidate: () => void): () => void
  dispose?(): void
}

const LIVE = Symbol.for('telefunc.liveSources')
const MAX_SOURCES_PER_REQUEST = 64

type LiveState = { sources: Map<string, LiveSource>; taken: boolean }

function getLiveState(): LiveState {
  const context = getRawContext()
  assertUsage(context, 'A live source can only be registered inside a telefunction.')
  const existing = context[LIVE] as LiveState | undefined
  if (existing) return existing
  const state: LiveState = { sources: new Map(), taken: false }
  context[LIVE] = state
  return state
}

/** Register a live source for the current request. Identity-deduped, first wins — a duplicate is
 *  disposed immediately (never leaks the rejected registration's resource). At most
 *  `MAX_SOURCES_PER_REQUEST` distinct identities; the overflowing source is disposed before the
 *  assert, so the cap path never leaks the source it rejects. */
function addLiveSource(source: LiveSource): void {
  const { sources } = getLiveState()
  if (sources.has(source.identity)) {
    source.dispose?.()
    return
  }
  if (sources.size >= MAX_SOURCES_PER_REQUEST) {
    source.dispose?.()
    assertUsage(false, `A telefunction registered more than ${MAX_SOURCES_PER_REQUEST} live sources.`)
  }
  sources.set(source.identity, source)
}

/** Take the request's live sources exactly once, transferring teardown ownership to the caller
 *  (the result hook subscribes each and tears them down on channel close). A second call yields []. */
function takeLiveSources(): LiveSource[] {
  const state = getRawContext()?.[LIVE] as LiveState | undefined
  if (!state || state.taken) return []
  state.taken = true
  const sources = [...state.sources.values()]
  state.sources.clear()
  return sources
}

/** Dispose every source still in the request set (the settle path): sources no result hook took, or
 *  ones a later hook added after the take. `takeLiveSources` already removed the taken sources, so
 *  they are never in this set. Attempt-all — one throwing disposer never blocks the rest. */
function disposeUntakenLiveSources(): void {
  const state = getRawContext()?.[LIVE] as LiveState | undefined
  if (!state) return
  for (const source of state.sources.values()) {
    try {
      source.dispose?.()
    } catch (err) {
      console.error('[telefunc:live] a live source disposer threw (continuing):', err)
    }
  }
  state.sources.clear()
}
