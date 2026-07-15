// Non-blocking warming with the atomic drain-loop cut (final-plan §5.5, debate A-F5; the
// adjudicated BLOCKER T5.C4). The telefunction response NEVER awaits this: from activation
// every event is journaled here AND coarse-invalidated by the live graph, so nothing is
// dropped while state is absent (over-fire licensed, R3). Warming ends with ONE muted engine
// feed per input, cut atomically:
//   1. build the σ-pruned baseline + shadow IN MEMORY (scan) — not yet fed;
//   2. drain: swap the journal, re-fetch the PKs it mentions, reconcile (absent→retract,
//      present→replace), and repeat until a swap comes back empty;
//   3. seed each input once from the reconciled baseline;
//   4. flip live SYNCHRONOUSLY — no await between the empty swap and the flip.
// A single-pass drain has an await gap that leaks an event injected mid-refetch into limbo;
// looping until empty + the synchronous flip closes it. Every warm owns a generation: an
// await that resolves after destroy/demote/retire/supersede is inert (never resurrects state,
// never flips a dead or superseded graph).

export { type HydrationExecutor, type WarmingHooks, type Warming, createWarming, JOURNAL_LIMIT }

import type { SeedDescriptor } from '../compile/compile.js'
import type { Row } from '../compile/rowSpace.js'
import type { TableChange } from '../router/events.js'
import { type ShadowIndex, createShadow, matchesResidual, pkOf, pruneRow } from './shadow.js'

/** Injected in the binding layer from the σ residual's opaque `src` handles + raw execution;
 *  `graph/` imports only this interface (R1 — zero drizzle here). */
type HydrationExecutor = {
  /** The σ-scoped, column-pruned baseline for one input. */
  scan(descriptor: SeedDescriptor): Promise<Row[]>
  /** The current σ-matching rows for a specific set of PKs (the drain re-fetch). */
  fetchByKeys(descriptor: SeedDescriptor, keys: Row[]): Promise<Row[]>
}

type WarmingHooks = {
  /** The synchronous cut (steps 3–4): seed each input from its reconciled baseline, then
   *  flip live — the implementer MUST NOT await inside this. */
  onComplete(shadows: Map<string, ShadowIndex>): void
  /** Journal overflow, state-row overflow, or a hydration error → demote to coarse (bounded,
   *  sound). Never called after abort. */
  onDemote(reason: string): void
}

/** Warming journal bound (§5.5 step 1). */
const JOURNAL_LIMIT = 10_000

type Warming = {
  /** Kick off the background scan+drain (fire-and-forget; the response never awaits it). */
  start(): void
  /** Append a warming-window event; overflow demotes to coarse. */
  journal(change: TableChange): void
  /** Supersede this generation — a later async completion becomes inert. */
  abort(): void
}

function createWarming(params: {
  descriptors: SeedDescriptor[]
  executor: HydrationExecutor
  maxStateRows: number
  hooks: WarmingHooks
  /** Drain strategy. Production is `'loop-until-empty'` (default): keep swapping the journal and
   *  re-fetching until a swap returns empty, so an event journaled during a re-fetch await is
   *  caught by the next pass before the flip. `'single-pass'` stops after the first swap and is
   *  TEST-ONLY (hydrate.spec T5.C4) — it deliberately leaks that mid-fetch event into limbo, the
   *  discriminating counterexample that proves the loop is load-bearing. */
  drainMode?: 'loop-until-empty' | 'single-pass'
}): Warming {
  const { descriptors, executor, maxStateRows, hooks } = params
  const drainMode = params.drainMode ?? 'loop-until-empty'
  const shadows = new Map<string, ShadowIndex>()
  let journalBuffer: TableChange[] = []
  let aborted = false
  let demoted = false

  const swapJournal = (): TableChange[] => {
    const taken = journalBuffer
    journalBuffer = []
    return taken
  }

  const demote = (reason: string): void => {
    if (aborted || demoted) return
    demoted = true
    hooks.onDemote(reason)
  }

  async function run(): Promise<void> {
    try {
      for (const descriptor of descriptors) {
        const shadow = createShadow()
        for (const raw of await executor.scan(descriptor)) admit(descriptor, shadow, raw)
        if (aborted) return
        shadows.set(descriptor.inputId, shadow)
        if (shadow.size > maxStateRows) return demote('state-row-limit')
      }

      while (true) {
        const batch = swapJournal()
        if (batch.length === 0) break
        for (const descriptor of descriptors) {
          const keys = keyRowsFor(descriptor, batch)
          if (keys.length === 0) continue
          const fetched = await executor.fetchByKeys(descriptor, keys)
          if (aborted) return
          reconcile(descriptor, shadows.get(descriptor.inputId)!, keys, fetched)
          if (shadows.get(descriptor.inputId)!.size > maxStateRows) return demote('state-row-limit')
        }
        // TEST-ONLY seam: the naive single-pass drain flips after ONE swap, so an event journaled
        // during the re-fetch await above is never re-swapped → lost to limbo (hydrate.spec T5.C4).
        if (drainMode === 'single-pass') break
      }

      // The atomic cut: no await from the empty swap above to onComplete below.
      if (aborted || demoted) return
      for (const shadow of shadows.values()) shadow.complete = true
      hooks.onComplete(shadows)
    } catch {
      demote('hydration-error')
    }
  }

  function admit(descriptor: SeedDescriptor, shadow: ShadowIndex, raw: Row): void {
    if (!matchesResidual(descriptor, raw)) return
    const pk = pkOf(descriptor, raw)
    if (pk !== undefined) shadow.put(pk, pruneRow(descriptor, raw))
  }

  return {
    start() {
      void run()
    },
    journal(change) {
      if (aborted) return
      if (journalBuffer.length >= JOURNAL_LIMIT) return demote('journal-overflow')
      journalBuffer.push(change)
    },
    abort() {
      aborted = true
    },
  }
}

/** The distinct PK rows a batch touches for one input (its re-fetch key set). */
function keyRowsFor(descriptor: SeedDescriptor, batch: TableChange[]): Row[] {
  const seen = new Set<string>()
  const keys: Row[] = []
  for (const change of batch) {
    if (change.table !== descriptor.table) continue
    const keyRow = change.new ?? change.old ?? change.key
    if (!keyRow) continue
    const pk = pkOf(descriptor, keyRow)
    if (pk === undefined || seen.has(pk)) continue
    seen.add(pk)
    keys.push(keyRow)
  }
  return keys
}

/** Reconcile the in-memory shadow against the DB's current state for the drained keys:
 *  a key still σ-matching is replaced with its fresh tuple, one that vanished is retracted. */
function reconcile(descriptor: SeedDescriptor, shadow: ShadowIndex, keys: Row[], fetched: Row[]): void {
  const current = new Map<string, Row>()
  for (const raw of fetched) {
    if (!matchesResidual(descriptor, raw)) continue
    const pk = pkOf(descriptor, raw)
    if (pk !== undefined) current.set(pk, pruneRow(descriptor, raw))
  }
  for (const keyRow of keys) {
    const pk = pkOf(descriptor, keyRow)!
    const now = current.get(pk)
    if (now !== undefined) shadow.put(pk, now)
    else shadow.remove(pk)
  }
}
