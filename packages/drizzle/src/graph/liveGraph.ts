// One compiled query as a live runtime object: the seeding / live / coarse / destroyed
// state machine. A stateful graph SEEDS synchronously-from-the-caller's-view —
// the registry blocks acquire on the seed — so it is PRECISE from its first live tick; there is no
// warming tier and no coarse-during-seed window. While a scan is in flight, routed changes are
// BUFFERED and replayed once, as a PK-keyed upsert against the seeded shadow, in the synchronous cut.
// Once live, each routed change is classified through the escalation ladder — inline old > shadow
// resolve > provably-irrelevant drop — and fires AT MOST ONCE per batch. Firing is reported to the
// caller (the router owns per-identity notification).
//
// RESEED — `seeding` is reachable AFTER `live`, and coarse is no longer a one-way door. An image-less
// event (raw SQL anywhere, a detected transport gap) used to demote a graph permanently: `coarsen()`
// aborts the seed, drops the operator graph and sets `coarse` with no path back, so ONE raw statement
// on any instance cost every watching graph its precision for the rest of its life. `reseed()` instead
// invalidates and then rebuilds the baseline from the database, keeping this graph's identity, its
// router registration and its subscribers while replacing its internals. `ready()` is re-armed per
// cycle so an acquire arriving mid-reseed still blocks until precision is back.
//
// Three things bound it. A STATELESS graph never reseeds — it holds no history, so an invalidation is
// the whole remedy and no query is run. A coarse event arriving while a seed is IN FLIGHT degrades to
// a plain demote, which makes the mechanism loop-free by construction (a reseed must finish before
// another can begin, and a demoted graph is terminal) and stops a burst queueing re-hydrates. And a
// reseed that cannot run — no usable PK, a scan overflow, an executor error — lands on `demote()`
// exactly as before, so the failure path is the old behaviour rather than a new one.
//
// COST MODEL, stated rather than discovered: a sustained coarse rate SLOWER than a reseed takes (an
// app running raw SQL every few seconds) never overlaps, so it never demotes — it pays one re-hydrate
// per affected graph per event. That is the same order as the client refetch each coarse event already
// triggers, and it is the price of coarse being recoverable at all.

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
      /** Notify this identity's subscribers from OUTSIDE a routed batch.
       *
       *  Every other invalidation reaches subscribers because the router notifies each affected identity
       *  after a synchronous `apply()`/`reseed()` reports it. A reseed's cut is asynchronous and belongs to
       *  no batch, so nothing would carry it: `fire()` alone advances the sequence, which the read fence
       *  reads but a subscriber never sees. The registry supplies this so the cut can reach the same sinks
       *  the router does. */
      notifyIdentity?: () => void
    }

type LiveGraph = {
  readonly instanceKey: string
  readonly tables: string[]
  state(): GraphState
  invalidationSeq(): number
  /** Resolves when the CURRENT seed cycle has landed (→ live) or demoted (→ coarse); the registry
   *  awaits this so acquire returns a precise graph. Re-armed by each reseed, so an acquire arriving
   *  mid-reseed blocks until precision is back rather than reading a resolved promise from the last
   *  cycle. Terminal transitions resolve it too. */
  ready(): Promise<void>
  apply(changes: TableChange[]): ApplyOutcome
  /** A routed apply() threw (a latent bug left state possibly corrupt) → permanently demote to
   *  coarse so every subsequent change coarse-fires (sound over-fire); the router surfaces the error. */
  fault(): void
  /** An explicit coarse event (an image-less mutation the source can't represent precisely) →
   *  RECOVER rather than surrender: invalidate now, then rebuild precise state from the database.
   *  See the reseed section in the header for what it costs and when it falls back to demoting. */
  reseed(): void
  /** Unconditionally demote to coarse, with no attempt to recover. Internal to the terminal paths
   *  (a seed that cannot run, a fault); a routed coarse event goes through `reseed()`. */
  coarsen(): void
  /** Refcount 0 → terminal; frees state immediately. */
  destroy(): void
}

function createLiveGraph(spec: LiveGraphSpec): LiveGraph {
  let state: GraphState = 'coarse'
  let seq = 0
  const watched = new Set(spec.tables)

  let resolveSeed!: () => void
  let seedDone = new Promise<void>((resolve) => {
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
    // Re-arm the readiness promise for THIS cycle. A reseed makes `seeding` reachable after `live`, so a
    // concurrent `registry.acquire` must block on the current cycle rather than on a promise the previous
    // one already resolved.
    seedDone = new Promise<void>((resolve) => {
      resolveSeed = resolve
    })
    const stateful = spec as Extract<LiveGraphSpec, { kind: 'stateful' }>
    // A fresh operator graph, deliberately. The old one accumulated its state from the PREVIOUS baseline
    // plus every delta since; reusing it against a newly-scanned baseline would double-count aggregates.
    // What survives a reseed is this LiveGraph's identity, its router registration and its subscribers —
    // not its internals. Anyone "optimising" this back into a reuse breaks every aggregate.
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
          const raced = oneShot.length > 0
          for (const change of oneShot)
            for (const descriptor of byTable.get(change.table) ?? [])
              replaySeedRace(descriptor, built.get(descriptor.inputId)!, change)
          for (const descriptor of graph!.seeds)
            graph!.seedInput(descriptor.inputId, built.get(descriptor.inputId)!.rows())
          graph!.flushSeed()
          for (const [inputId, shadow] of built) shadows.set(inputId, shadow)
          oneShot = []
          state = 'live'
          // The seqAtRead fence, aimed at SUBSCRIBERS rather than at the reader. A change that landed
          // during the scan was buffered and folded into the baseline above — silently, because the buffer
          // does not fire. During an INITIAL seed that is right: nobody is subscribed yet, and the reader is
          // covered by the redeem fence. During a RESEED it is not: subscribers exist, and a write that
          // committed after their refetch would be absorbed into the new baseline and never announced.
          //
          // `fire()` alone is NOT enough here, and that distinction cost a review round: it advances the
          // sequence the read fence consults, but subscribers are reached by the ROUTER's notify pass, and
          // this cut runs asynchronously outside any routed batch. A test asserting the sequence therefore
          // passes while no client is ever told. Hence the explicit identity notification.
          // Resolve THIS cycle before notifying anyone. `notifyIdentity` runs subscriber callbacks, and a
          // subscriber is free to cause another coarse event synchronously — which starts a new cycle and
          // REBINDS `resolveSeed` to it. Notifying first would then resolve the new cycle's promise instead
          // of this one: waiters on the finished cycle hang, and waiters on the running one are released
          // early against a graph that is seeding again.
          resolveSeed()
          if (raced) {
            fire()
            stateful.notifyIdentity?.()
          }
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
    reseed() {
      if (state === 'destroyed') return
      fire() // whatever happens below, subscribers must learn NOW that this graph's result may have moved
      // A STATELESS graph holds no accumulated state, so an image-less mutation cannot have corrupted
      // anything: its precision comes from the compiled evaluator, not from history. It needs the
      // invalidation above and nothing else — no demotion, no database round trip.
      if (stateless) return
      // Nothing to recover: an already-coarse graph has no precise state to rebuild, and a graph that
      // never had one (born coarse / no usable PK) would only demote again.
      if (state === 'coarse') return
      // STORM GUARD. A coarse event arriving while a seed is already in flight degrades to a plain demote.
      // That is what keeps this loop-free by construction — a reseed must finish before another can start,
      // and a demoted graph is terminal — and what stops a burst of coarse events queueing re-hydrates.
      if (state === 'seeding') {
        demote()
        return
      }
      startSeeding()
    },
    coarsen() {
      // Unconditional demotion, for the paths that genuinely cannot recover. Feeds no row — precise state
      // is never fed a fabrication.
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
