export { createStatefulVariant }

import type { Change, SeedDescriptor, StatefulGraph } from '../compile/compile.js'
import type { RowChange, TableChange } from '../router/events.js'
import { type HydrationExecutor, type Seed, createSeed } from './hydrate.js'
import { type ShadowIndex, matchesResidual, pkOf, pruneRow } from './shadow.js'
import { rowChanged } from '../compile/rowSpace.js'
import type { ApplyOutcome, GraphCore, GraphVariant } from './liveGraph.js'

// THE STATEFUL VARIANT — the seed / reseed / demote machine, and the only variant that keeps row history.
//
// A stateful graph SEEDS synchronously-from-the-caller's-view — the registry blocks acquire on the seed —
// so it is PRECISE from its first live tick; there is no warming tier and no coarse-during-seed window.
// While a scan is in flight, routed changes are BUFFERED and replayed once, as a PK-keyed upsert against
// the seeded shadow, in the synchronous cut. Once live, each routed change is classified through the
// escalation ladder — inline old > shadow resolve > provably-irrelevant drop — and fires AT MOST ONCE per
// batch.
//
// RESEED — `seeding` is reachable AFTER `live`, so coarse is not a one-way door. An image-less event
// (raw SQL anywhere, a detected transport gap) invalidates and then rebuilds the baseline from the
// database, keeping this graph's identity, its router registration and its subscribers while replacing
// its internals. Readiness is re-armed per cycle so an acquire arriving mid-reseed still blocks until
// precision is back.
//
// Reseeding rather than `coarsen()`, which aborts the seed, drops the operator graph and sets `coarse`
// with no path back: that makes ONE raw statement on any instance cost every watching graph its
// precision for the rest of its life.
//
// Two things bound it HERE (the third — that a stateless graph never reseeds — lives with that variant).
// A coarse event arriving while a seed is IN FLIGHT degrades to a plain demote, which makes the mechanism
// loop-free by construction (a reseed must finish before another can begin, and a demoted graph is
// terminal) and stops a burst queueing re-hydrates. And a reseed that cannot run — no usable PK, a scan
// overflow, an executor error — lands on demotion exactly as before, so the failure path is the old
// behaviour rather than a new one.
//
// COST MODEL, stated rather than discovered: a sustained coarse rate SLOWER than a reseed takes (an
// app running raw SQL every few seconds) never overlaps, so it never demotes — it pays one re-hydrate
// per affected graph per event. That is the same order as the client refetch each coarse event already
// triggers, and it is the price of coarse being recoverable at all.

type StatefulSpec = {
  instantiate: () => StatefulGraph
  executor: HydrationExecutor
  maxStateRows: number
  /** rls (true / 'unknown') → born coarse (never hydrates row state). */
  bornCoarse?: boolean
  /** Notify this identity's subscribers from OUTSIDE a routed batch.
   *
   *  Every other invalidation reaches subscribers because the router notifies each affected identity
   *  after a synchronous `apply()`/`reseed()` reports it. A reseed's cut is asynchronous and belongs to
   *  no batch, so nothing would carry it: firing alone advances the sequence, which the read fence
   *  reads but a subscriber never sees. The registry supplies this so the cut can reach the same sinks
   *  the router does. */
  notifyIdentity?: () => void
}

function createStatefulVariant(spec: StatefulSpec, core: GraphCore): GraphVariant {
  const shadows = new Map<string, ShadowIndex>()
  const byTable = new Map<string, SeedDescriptor[]>()
  let graph: StatefulGraph | undefined
  let seed: Seed | undefined
  let oneShot: TableChange[] = [] // changes routed during the scan (the seed-race buffer)

  // What a DEMOTE frees. Registered with the core so every demotion path — a routed fault, an explicit
  // coarsen, a scan overflow, a key that cannot be resolved — frees the same state through one door.
  core.setTeardown(() => {
    seed?.abort()
    seed = undefined
    graph = undefined
    shadows.clear()
    byTable.clear()
    oneShot = []
  })

  function startSeeding(): void {
    // Re-arm the readiness promise for THIS cycle. A reseed makes `seeding` reachable after `live`, so a
    // concurrent `registry.acquire` must block on the current cycle rather than on a promise the previous
    // one already resolved.
    core.armSeedCycle()
    // A fresh operator graph, deliberately. The old one accumulated its state from the PREVIOUS baseline
    // plus every delta since; reusing it against a newly-scanned baseline would double-count aggregates.
    // What survives a reseed is this LiveGraph's identity, its router registration and its subscribers —
    // not its internals. Anyone "optimising" this back into a reuse breaks every aggregate.
    graph = spec.instantiate()
    // A no-PK input can never shadow-resolve a key-only retraction → born coarse.
    if (graph.seeds.some((descriptor) => descriptor.primaryKey.length === 0)) {
      graph = undefined
      core.toState('coarse')
      core.settle()
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
      executor: spec.executor,
      maxStateRows: spec.maxStateRows,
      hooks: {
        onComplete(built) {
          // The synchronous cut: reconcile the one-shot seed-race buffer against the
          // freshly-scanned shadows, then seed the engine from the final baseline in ONE muted
          // flush — NO await anywhere between here and `live`.
          const raced = oneShot.length > 0
          for (const change of oneShot)
            for (const descriptor of byTable.get(change.table) ?? [])
              replaySeedRace(descriptor, built.get(descriptor.inputId)!, change)
          for (const descriptor of graph!.seeds)
            graph!.seedInput(descriptor.inputId, built.get(descriptor.inputId)!.rows())
          graph!.flushSeed()
          for (const [inputId, shadow] of built) shadows.set(inputId, shadow)
          oneShot = []
          core.toState('live')
          // The seqAtRead fence, aimed at SUBSCRIBERS rather than at the reader. A change that landed
          // during the scan was buffered and folded into the baseline above — silently, because the buffer
          // does not fire. During an INITIAL seed that is right: nobody is subscribed yet, and the reader is
          // covered by the redeem fence. During a RESEED it is not: subscribers exist, and a write that
          // committed after their refetch would be absorbed into the new baseline and never announced.
          //
          // Firing alone is NOT enough here, and that distinction cost a review round: it advances the
          // sequence the read fence consults, but subscribers are reached by the ROUTER's notify pass, and
          // this cut runs asynchronously outside any routed batch. A test asserting the sequence therefore
          // passes while no client is ever told. Hence the explicit identity notification.
          // Settle THIS cycle before notifying anyone. `notifyIdentity` runs subscriber callbacks, and a
          // subscriber is free to cause another coarse event synchronously — which starts a new cycle and
          // REBINDS the cycle's resolver. Notifying first would then settle the new cycle instead of this
          // one: waiters on the finished cycle hang, and waiters on the running one are released early
          // against a graph that is seeding again.
          core.settle()
          if (raced) {
            core.fire()
            spec.notifyIdentity?.()
          }
        },
        onDemote() {
          core.demote()
        },
      },
    })
    core.toState('seeding')
    seed.start()
  }

  // BORN: a graph the binding marked coarse (RLS true / unknown) never hydrates row state; everything else
  // starts its first seed cycle immediately.
  if (spec.bornCoarse) {
    core.toState('coarse')
    core.settle()
  } else {
    startSeeding()
  }

  return {
    applySeeding(changes) {
      // Precise buffer, NOT coarse-invalidate: no subscriber exists yet (attach follows acquire's
      // blocked seed), and the read-in-progress is covered by the initial-read fence. Each buffered
      // change is replayed exactly once in the synchronous cut.
      for (const change of changes) if (core.watches(change.table) && rowChanged(change)) oneShot.push(change)
      return { invalidated: false }
    },

    applyLive(changes: RowChange[]): ApplyOutcome {
      for (const change of changes) {
        for (const descriptor of byTable.get(change.table) ?? []) {
          const resolved = resolveOld(descriptor, shadows.get(descriptor.inputId)!, change)
          if (resolved.kind === 'coarse') {
            // A key-only retraction whose key lacks resolvable PK columns can't be applied without
            // guessing (shadow.resolve is never 'coarse' post-seed — state is complete). Fire coarse
            // once and demote: skipping it would leave the shadow/engine stale (unsound).
            core.fire()
            core.demote()
            return { invalidated: true }
          }
          if (resolved.kind === 'drop') continue
          const resolvedChange: Change = {
            table: change.table,
            kind: change.kind,
            old: resolved.old,
            new: change.new,
          }
          graph!.feedInput(descriptor.inputId, resolvedChange)
          updateShadow(descriptor, shadows.get(descriptor.inputId)!, resolvedChange)
        }
      }
      const result = graph!.runBatch()
      if (result.invalidated) core.fire()
      if (overStateBound()) core.demote()
      return { invalidated: result.invalidated }
    },

    reseed() {
      // Nothing to recover: an already-coarse graph has no precise state to rebuild, and a graph that
      // never had one (born coarse / no usable PK) would only demote again.
      if (core.state() === 'coarse') return
      // STORM GUARD. A coarse event arriving while a seed is already in flight degrades to a plain demote.
      // That is what keeps this loop-free by construction — a reseed must finish before another can start,
      // and a demoted graph is terminal — and what stops a burst of coarse events queueing re-hydrates.
      if (core.state() === 'seeding') {
        core.demote()
        return
      }
      startSeeding()
    },

    // DESTROY frees a deliberately NARROWER set than demote: `byTable`/`oneShot` are left alone, because a
    // destroyed graph answers `apply` before reaching either and nothing can observe them again. Kept
    // exactly as it was rather than unified with the demote teardown — this is a relocation, not a tidy-up.
    dispose() {
      seed?.abort()
      seed = undefined
      graph = undefined
      shadows.clear()
    },
  }

  function overStateBound(): boolean {
    for (const shadow of shadows.values()) if (shadow.size > spec.maxStateRows) return true
    return false
  }
}

// ── change classification (stateful only — nothing else keeps a shadow) ─────────────────────────────

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
