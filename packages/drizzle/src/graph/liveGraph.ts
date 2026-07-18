// One compiled query as a live runtime object: the seeding / live / coarse / destroyed
// state machine. A stateful graph SEEDS synchronously-from-the-caller's-view —
// the registry blocks acquire on the seed — so it is PRECISE from its first live tick; there is no
// warming tier and no coarse-during-seed window. `seeding` is internal and transient: it exists
// only while the initial scan is in flight (before acquire returns), during which routed changes
// are BUFFERED and replayed once, as a PK-keyed upsert against the seeded shadow, in the synchronous
// cut. Once live, each routed change is classified through the escalation ladder — inline old >
// shadow resolve > provably-irrelevant drop — and fires AT MOST ONCE per batch. A seed error, a
// state-row overflow, or a caught apply-fault (the router faults a throwing graph) DEMOTES to coarse
// (sound over-fire). Firing is reported to the caller (the router owns per-identity notification).

export { type LiveGraph, type LiveGraphSpec, type ApplyOutcome, createLiveGraph }

import type { Change, CompiledGraph, SeedDescriptor, StatefulGraph } from '../compile/compile.js'
import type { RowChange, TableChange } from '../router/events.js'
import { rowChanged } from '../compile/rowSpace.js'
import { type HydrationExecutor, type Seed, createSeed } from './hydrate.js'
import { type ShadowIndex, matchesResidual, pkOf, pruneRow } from './shadow.js'

type GraphState = 'seeding' | 'live' | 'coarse' | 'destroyed'
type ApplyOutcome = { invalidated: boolean }

type LiveGraphSpec =
  | { kind: 'coarse'; instanceKey: string; tables: string[] }
  | { kind: 'stateless'; instanceKey: string; tables: string[]; instantiate: () => CompiledGraph }
  | {
      kind: 'stateful'
      instanceKey: string
      tables: string[]
      instantiate: () => StatefulGraph
      executor: HydrationExecutor
      maxStateRows: number
      /** rls (true / 'unknown') → born coarse (never hydrates row state). */
      bornCoarse?: boolean
    }

type LiveGraph = {
  readonly instanceKey: string
  readonly tables: string[]
  state(): GraphState
  invalidationSeq(): number
  /** Resolves when the initial seed has landed (→ live) or demoted (→ coarse); the registry
   *  awaits this so acquire returns a precise graph. Terminal transitions resolve it too. */
  ready(): Promise<void>
  apply(changes: TableChange[]): ApplyOutcome
  /** A routed apply() threw (a latent bug left state possibly corrupt) → permanently demote to
   *  coarse so every subsequent change coarse-fires (sound over-fire); the router surfaces the error. */
  fault(): void
  /** An explicit coarse event (an image-less mutation the source can't represent precisely) →
   *  intentionally demote to coarse (distinct from fault's apply-throw, same coarse outcome). */
  coarsen(): void
  /** Refcount 0 → terminal; frees state immediately. */
  destroy(): void
}

function createLiveGraph(spec: LiveGraphSpec): LiveGraph {
  let state: GraphState = 'coarse'
  let seq = 0
  const watched = new Set(spec.tables)

  let resolveSeed!: () => void
  const seedDone = new Promise<void>((resolve) => {
    resolveSeed = resolve
  })

  // Stateful-only runtime.
  const shadows = new Map<string, ShadowIndex>()
  const byTable = new Map<string, SeedDescriptor[]>()
  let graph: StatefulGraph | undefined
  let seed: Seed | undefined
  let oneShot: TableChange[] = [] // changes routed during the scan (the seed-race buffer)

  // Stateless-only runtime.
  const stateless = spec.kind === 'stateless' ? spec.instantiate() : undefined

  // Advance the invalidation sequence — the redeem fence's cursor. Every fire (exact, dirty, or a
  // coarse/fault/coarsen demotion) must bump it so a demotion landing in a read window is caught.
  function fire(): void {
    seq++
  }

  function startSeeding(): void {
    const stateful = spec as Extract<LiveGraphSpec, { kind: 'stateful' }>
    graph = stateful.instantiate()
    // A no-PK input can never shadow-resolve a key-only retraction → born coarse.
    if (graph.seeds.some((descriptor) => descriptor.primaryKey.length === 0)) {
      graph = undefined
      state = 'coarse'
      resolveSeed()
      return
    }
    shadows.clear()
    byTable.clear()
    oneShot = []
    for (const descriptor of graph.seeds) {
      // Indexed by ROUTING identity (not the SQL name), because that is what `change.table` carries.
      const list = byTable.get(descriptor.relationId) ?? []
      list.push(descriptor)
      byTable.set(descriptor.relationId, list)
    }
    seed = createSeed({
      descriptors: graph.seeds,
      executor: stateful.executor,
      maxStateRows: stateful.maxStateRows,
      hooks: {
        onComplete(built) {
          // The synchronous cut: reconcile the one-shot seed-race buffer against the
          // freshly-scanned shadows, then seed the engine from the final baseline in ONE muted
          // flush — NO await anywhere between here and `state = 'live'`.
          for (const change of oneShot)
            for (const descriptor of byTable.get(change.table) ?? [])
              replaySeedRace(descriptor, built.get(descriptor.inputId)!, change)
          for (const descriptor of graph!.seeds)
            graph!.seedInput(descriptor.inputId, built.get(descriptor.inputId)!.rows())
          graph!.flushSeed()
          for (const [inputId, shadow] of built) shadows.set(inputId, shadow)
          oneShot = []
          state = 'live'
          resolveSeed()
        },
        onDemote() {
          demote()
        },
      },
    })
    state = 'seeding'
    seed.start()
  }

  function demote(): void {
    seed?.abort()
    seed = undefined
    graph = undefined
    shadows.clear()
    byTable.clear()
    oneShot = []
    state = 'coarse'
    resolveSeed()
  }

  // ── apply per state ───────────────────────────────────────────────

  function coarseApply(changes: TableChange[]): ApplyOutcome {
    const hit = changes.some((change) => watched.has(change.table) && rowChanged(change))
    if (hit) fire()
    return { invalidated: hit }
  }

  function seedingApply(changes: TableChange[]): ApplyOutcome {
    // Precise buffer, NOT coarse-invalidate: no subscriber exists yet (attach follows acquire's
    // blocked seed), and the read-in-progress is covered by the initial-read fence. Each buffered
    // change is replayed exactly once in the synchronous cut.
    for (const change of changes) if (watched.has(change.table) && rowChanged(change)) oneShot.push(change)
    return { invalidated: false }
  }

  function statelessApply(changes: RowChange[]): ApplyOutcome {
    // A key-only retraction cannot be decided without state → coarse (sound); everything else
    // resolves in row space via the compiled stateless evaluator.
    const commit: Change[] = []
    let coarse = false
    for (const change of changes) {
      if (change.kind !== 'insert' && change.old === undefined) coarse = true
      else commit.push({ table: change.table, kind: change.kind, old: change.old, new: change.new })
    }
    const result = commit.length > 0 ? stateless!.apply(commit) : { invalidated: false }
    const invalidated = result.invalidated || coarse
    if (invalidated) fire()
    return { invalidated }
  }

  function statefulApply(changes: RowChange[]): ApplyOutcome {
    for (const change of changes) {
      for (const descriptor of byTable.get(change.table) ?? []) {
        const resolved = resolveOld(descriptor, shadows.get(descriptor.inputId)!, change)
        if (resolved.kind === 'coarse') {
          // A key-only retraction whose key lacks resolvable PK columns can't be applied without
          // guessing (shadow.resolve is never 'coarse' post-seed — state is complete). Fire coarse
          // once and demote: skipping it would leave the shadow/engine stale (unsound).
          fire()
          demote()
          return { invalidated: true }
        }
        if (resolved.kind === 'drop') continue
        const resolvedChange: Change = { table: change.table, kind: change.kind, old: resolved.old, new: change.new }
        graph!.feedInput(descriptor.inputId, resolvedChange)
        updateShadow(descriptor, shadows.get(descriptor.inputId)!, resolvedChange)
      }
    }
    const result = graph!.runBatch()
    if (result.invalidated) fire()
    if (overStateBound()) demote()
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
    resolveSeed()
  } else if (spec.kind === 'coarse') {
    state = 'coarse'
    resolveSeed()
  } else if (spec.bornCoarse) {
    state = 'coarse'
    resolveSeed()
  } else {
    startSeeding()
  }

  return {
    instanceKey: spec.instanceKey,
    tables: spec.tables,
    state: () => state,
    invalidationSeq: () => seq,
    ready: () => seedDone,
    apply(changes) {
      if (state === 'destroyed') return { invalidated: false }
      if (state === 'coarse') return coarseApply(changes)
      if (state === 'seeding') return seedingApply(changes)
      // The router routes coarse markers to coarsen(), never to apply; the row-space state machine is
      // coarse-free (a coarse marker reaching here would be a router bug).
      return stateless ? statelessApply(changes as RowChange[]) : statefulApply(changes as RowChange[])
    },
    fault() {
      // A routed apply() threw (a latent bug left state possibly corrupt): permanently demote to
      // coarse so every subsequent change coarse-fires (sound over-fire) — the precise state can no
      // longer be trusted. The router surfaces the error (structured log).
      if (state === 'destroyed') return
      fire() // advance seq so a fault landing in the read window is caught by the redeem fence
      demote()
    },
    coarsen() {
      // An explicit coarse event (an image-less mutation the source can't represent precisely):
      // intentionally demote to coarse via the SAME path as fault(). Feeds no row — the router calls
      // this INSTEAD of apply(), so precise state is never fed a fabrication.
      if (state === 'destroyed') return
      fire() // advance seq so a coarse event in the read window is caught by the redeem fence
      demote()
    },
    destroy() {
      seed?.abort()
      seed = undefined
      graph = undefined
      shadows.clear()
      state = 'destroyed'
      resolveSeed()
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

/** One-shot seed-race guard — NOT warming; do not add passes. A change routed during the scan may
 *  or may not already be reflected in the scan's consistent snapshot; replay it against the seeded
 *  shadow so the outcome is idempotent regardless. Retract whatever the snapshot holds for the OLD-
 *  image PK (the change's OWN old key — NOT the new PK, or a PK-CHANGING update would strand the old
 *  row), then insert the new tuple under its OWN PK if it σ-matches. Per op: INSERT → insert new;
 *  DELETE → retract old; UPDATE → retract old.pk + insert new.pk (correct even when the PK changes).
 *  Reconciles the SHADOW only — the engine is seeded once from the final shadow, so it never sees an
 *  intermediate retract/insert and a net-zero replay is invisible. */
function replaySeedRace(descriptor: SeedDescriptor, shadow: ShadowIndex, change: TableChange): void {
  const oldKeyRow = change.old ?? change.key
  if (oldKeyRow !== undefined) {
    const oldPk = pkOf(descriptor, oldKeyRow)
    if (oldPk !== undefined) shadow.remove(oldPk)
  }
  if (change.new !== undefined && matchesResidual(descriptor, change.new)) {
    const newPk = pkOf(descriptor, change.new)
    if (newPk !== undefined) shadow.put(newPk, pruneRow(descriptor, change.new))
  }
}
