// One compiled query as a live runtime object: the warming / live / coarse / retired /
// destroyed state machine (final-plan §5.5; the ONLY legal transitions are §3.C-TT). It
// classifies each routed change through the escalation ladder — inline old > shadow resolve >
// provably-irrelevant drop > coarse — feeds the engine per input (so a self-join feeds each
// alias exactly once), keeps each shadow in lockstep, and fires AT MOST ONCE per batch. Two
// triggers, two responses: a gap / shadow discontinuity RE-WARMS (new generation, same plan);
// schema drift RETIRES (terminal, never rehydrated). Firing is reported to the caller (the
// router owns per-identity notification); `inspect()` is a bounded snapshot with one reason.

export { type LiveGraph, type LiveGraphSpec, type GraphState, type ApplyOutcome, type Inspection, createLiveGraph }

import type { Change, CompiledGraph, SeedDescriptor, StatefulGraph } from '../compile/compile.js'
import type { TableChange } from '../router/events.js'
import { canonicalValue } from '../utils/canonical.js'
import { type HydrationExecutor, type Warming, createWarming } from './hydrate.js'
import { type ShadowIndex, matchesResidual, pkOf, pruneRow } from './shadow.js'

type GraphState = 'warming' | 'live' | 'coarse' | 'retired' | 'destroyed'
type ApplyOutcome = { invalidated: boolean }

/** A fired invalidation's precision — the one stable reason `inspect()` reports. */
type FireKind = 'exact' | 'dirty' | 'coarse'

type Inspection = {
  state: GraphState
  reason: string
  counters: {
    warms: number
    demotions: number
    resyncs: number
    exactFires: number
    dirtyFires: number
    coarseFires: number
    stateRows: number
  }
}

type LiveGraphSpec =
  | { kind: 'coarse'; instanceKey: string; tables: string[]; reason?: string }
  | { kind: 'stateless'; instanceKey: string; tables: string[]; instantiate: () => CompiledGraph }
  | {
      kind: 'stateful'
      instanceKey: string
      tables: string[]
      instantiate: () => StatefulGraph
      executor: HydrationExecutor
      maxStateRows: number
      /** rls / no-pk → born coarse (never hydrates row state). */
      bornCoarse?: string
    }

type LiveGraph = {
  readonly instanceKey: string
  readonly tables: string[]
  state(): GraphState
  invalidationSeq(): number
  apply(changes: TableChange[]): ApplyOutcome
  /** Gap / source resync / shadow discontinuity → re-warm the SAME plan (new generation). */
  rewarm(): void
  /** Schema/relation drift → terminal; the registry recompiles on the next read. */
  retire(): void
  /** Refcount 0 → terminal; frees state immediately. */
  destroy(): void
  inspect(): Inspection
}

function createLiveGraph(spec: LiveGraphSpec): LiveGraph {
  let state: GraphState = 'coarse'
  let reason = 'born'
  let seq = 0
  const counters = { warms: 0, demotions: 0, resyncs: 0, exactFires: 0, dirtyFires: 0, coarseFires: 0 }
  const watched = new Set(spec.tables)

  // Stateful-only runtime.
  const shadows = new Map<string, ShadowIndex>()
  const byTable = new Map<string, SeedDescriptor[]>()
  let graph: StatefulGraph | undefined
  let warming: Warming | undefined

  // Stateless-only runtime.
  const stateless = spec.kind === 'stateless' ? spec.instantiate() : undefined

  function fire(kind: FireKind): void {
    seq++
    if (kind === 'exact') counters.exactFires++
    else if (kind === 'dirty') counters.dirtyFires++
    else counters.coarseFires++
    reason = kind
  }

  function startWarming(): void {
    // Supersede any prior generation first (T5.C7): a re-warm replaces `warming`, so the old
    // one's late scan/fetch/onComplete must be made inert or it would seed the NEW graph.
    warming?.abort()
    const stateful = spec as Extract<LiveGraphSpec, { kind: 'stateful' }>
    graph = stateful.instantiate()
    // A no-PK input can never shadow-resolve a key-only retraction → born coarse (T5.A6).
    if (graph.seeds.some((descriptor) => descriptor.primaryKey.length === 0)) {
      graph = undefined
      state = 'coarse'
      reason = 'no-pk'
      return
    }
    shadows.clear()
    byTable.clear()
    for (const descriptor of graph.seeds) {
      const list = byTable.get(descriptor.table) ?? []
      list.push(descriptor)
      byTable.set(descriptor.table, list)
    }
    warming = createWarming({
      descriptors: graph.seeds,
      executor: stateful.executor,
      maxStateRows: stateful.maxStateRows,
      hooks: {
        onComplete(built) {
          // The atomic cut (T5.C4 steps 3–4): seed each input from its reconciled baseline,
          // then flip live — synchronous, no await.
          for (const descriptor of graph!.seeds)
            graph!.seedInput(descriptor.inputId, built.get(descriptor.inputId)!.rows())
          graph!.flushSeed()
          for (const [inputId, shadow] of built) shadows.set(inputId, shadow)
          state = 'live'
          reason = 'warm-complete'
        },
        onDemote(why) {
          demote(why)
        },
      },
    })
    state = 'warming'
    reason = 'warming'
    counters.warms++
    warming.start()
  }

  function resyncNow(): void {
    counters.resyncs++
    startWarming()
  }

  function demote(why: string): void {
    warming?.abort()
    warming = undefined
    graph = undefined
    shadows.clear()
    byTable.clear()
    state = 'coarse'
    reason = why
    counters.demotions++
  }

  // ── apply per state ───────────────────────────────────────────────

  function coarseApply(changes: TableChange[]): ApplyOutcome {
    const hit = changes.some((change) => watched.has(change.table) && substantive(change))
    if (hit) fire('coarse')
    return { invalidated: hit }
  }

  function warmingApply(changes: TableChange[]): ApplyOutcome {
    let hit = false
    for (const change of changes) {
      if (!watched.has(change.table) || !substantive(change)) continue
      warming!.journal(change)
      hit = true
    }
    // Every warming-window change coarse-invalidates AND is journaled (T5.C2) — no silent drop.
    if (hit) fire('coarse')
    return { invalidated: hit }
  }

  function statelessApply(changes: TableChange[]): ApplyOutcome {
    // A key-only retraction cannot be decided without state → coarse (sound); everything else
    // resolves in row space via the compiled stateless evaluator.
    const commit: Change[] = []
    let coarse = false
    for (const change of changes) {
      if (change.kind !== 'insert' && change.old === undefined) coarse = true
      else commit.push({ table: change.table, kind: change.kind, old: change.old, new: change.new })
    }
    const result = commit.length > 0 ? stateless!.apply(commit) : { data: false, dirty: false, invalidated: false }
    const invalidated = result.invalidated || coarse
    if (invalidated) fire(coarse ? 'coarse' : result.data ? 'exact' : 'dirty')
    return { invalidated }
  }

  function statefulApply(changes: TableChange[]): ApplyOutcome {
    for (const change of changes) {
      for (const descriptor of byTable.get(change.table) ?? []) {
        const resolved = resolveOld(descriptor, shadows.get(descriptor.inputId)!, change)
        if (resolved.kind === 'coarse') {
          // Shadow discontinuity: coarse-invalidate once, then re-warm to restore exact state.
          fire('coarse')
          resyncNow()
          return { invalidated: true }
        }
        if (resolved.kind === 'drop') continue
        const resolvedChange: Change = { table: change.table, kind: change.kind, old: resolved.old, new: change.new }
        graph!.feedInput(descriptor.inputId, resolvedChange)
        updateShadow(descriptor, shadows.get(descriptor.inputId)!, resolvedChange)
      }
    }
    const result = graph!.runBatch()
    if (result.invalidated) fire(result.data ? 'exact' : 'dirty')
    if (overStateBound()) demote('state-row-limit')
    return { invalidated: result.invalidated }
  }

  function overStateBound(): boolean {
    const limit = (spec as Extract<LiveGraphSpec, { kind: 'stateful' }>).maxStateRows
    for (const shadow of shadows.values()) if (shadow.size > limit) return true
    return false
  }

  // ── lifecycle ─────────────────────────────────────────────────────

  if (spec.kind === 'stateless') {
    state = 'live'
    reason = 'born-live'
  } else if (spec.kind === 'coarse') {
    state = 'coarse'
    reason = spec.reason ?? 'coarse-plan'
  } else if (spec.bornCoarse) {
    state = 'coarse'
    reason = spec.bornCoarse
  } else {
    startWarming()
  }

  return {
    instanceKey: spec.instanceKey,
    tables: spec.tables,
    state: () => state,
    invalidationSeq: () => seq,
    apply(changes) {
      if (state === 'retired' || state === 'destroyed') return { invalidated: false }
      if (state === 'coarse') return coarseApply(changes)
      if (state === 'warming') return warmingApply(changes)
      return stateless ? statelessApply(changes) : statefulApply(changes)
    },
    rewarm() {
      // Only a stateful graph re-warms; stateless/coarse graphs hold no state to refresh.
      if (spec.kind !== 'stateful' || state === 'retired' || state === 'destroyed' || state === 'coarse') return
      resyncNow()
    },
    retire() {
      warming?.abort()
      warming = undefined
      graph = undefined
      shadows.clear()
      state = 'retired'
      reason = 'drift'
    },
    destroy() {
      warming?.abort()
      warming = undefined
      graph = undefined
      shadows.clear()
      state = 'destroyed'
      reason = 'refcount-zero'
    },
    inspect() {
      let stateRows = 0
      for (const shadow of shadows.values()) stateRows += shadow.size
      return { state, reason, counters: { ...counters, stateRows } }
    },
  }
}

// ── change classification ───────────────────────────────────────────

type Resolved = { kind: 'ok'; old?: Record<string, unknown> } | { kind: 'drop' } | { kind: 'coarse' }

/** Resolve the OLD side of a change for one input (the escalation ladder). Insert → no old;
 *  `change.old` present → inline (no consult); key-only → shadow: hit resolves exactly, a
 *  drop on complete state is provably irrelevant, a miss on incomplete state is coarse. */
function resolveOld(descriptor: SeedDescriptor, shadow: ShadowIndex, change: TableChange): Resolved {
  if (change.kind === 'insert') return { kind: 'ok' }
  if (change.old !== undefined) return { kind: 'ok', old: change.old }
  const keyRow = change.key ?? change.new
  const pk = keyRow ? pkOf(descriptor, keyRow) : undefined
  if (pk === undefined) return { kind: 'coarse' }
  const match = shadow.resolve(pk)
  if (match.kind === 'hit') return { kind: 'ok', old: match.old }
  if (match.kind === 'drop') return change.kind === 'delete' ? { kind: 'drop' } : { kind: 'ok' }
  return { kind: 'coarse' }
}

/** Keep the shadow in lockstep with the engine feed: the old tuple leaves, a σ-matching new
 *  tuple enters. */
function updateShadow(descriptor: SeedDescriptor, shadow: ShadowIndex, change: Change): void {
  if (change.old !== undefined) {
    const pk = pkOf(descriptor, change.old)
    if (pk !== undefined) shadow.remove(pk)
  }
  if (change.new !== undefined && matchesResidual(descriptor, change.new)) {
    const pk = pkOf(descriptor, change.new)
    if (pk !== undefined) shadow.put(pk, pruneRow(descriptor, change.new))
  }
}

/** Whether a change actually changes anything (a pure replay update never invalidates). */
function substantive(change: TableChange): boolean {
  if (change.kind !== 'update') return true
  if (!change.old || !change.new) return true
  return canonicalValue(sorted(change.old)) !== canonicalValue(sorted(change.new))
}

function sorted(row: Record<string, unknown>): Array<[string, unknown]> {
  return Object.keys(row)
    .sort()
    .map((key) => [key, row[key]])
}
