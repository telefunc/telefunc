// One compiled query as a live runtime object. THREE genuine runtime variants share one protocol and one
// state machine, and `createLiveGraph` is the dispatcher over them:
//
//   COARSE     — no row state at all. Every watched row change fires; nothing is ever resolved.
//   STATELESS  — a compiled evaluator decides membership per change, holding no history. Never reseeds:
//                with no accumulated state there is nothing an image-less mutation can have corrupted.
//   STATEFUL   — keeps a shadow of the rows it depends on, so it can resolve a key-only retraction and
//                answer aggregates. This is the seed / reseed / demote machine, and it lives in
//                statefulLiveGraph.ts: a reader of the two cases above should never have to traverse it.
//
// What is SHARED sits here, because it is genuinely shared rather than merely similar: the
// seeding/live/coarse/destroyed state machine, the invalidation sequence every fire advances, the
// per-cycle readiness promise the registry blocks acquire on, the coarse apply that EVERY variant falls
// back to once demoted, and the demotion door itself. A variant supplies only what differs — how it
// applies a change while live, what an image-less event means to it, and what it has to free.

export { type LiveGraph, type LiveGraphSpec, type ApplyOutcome, type GraphCore, type GraphVariant, createLiveGraph }

import type { CompiledGraph, StatefulGraph, Change } from '../compile/compile.js'
import type { RlsStatus } from '../../ir/types.js'
import type { RowChange, TableChange } from '../../router/events.js'
import { rowChanged } from '../compile/rowSpace.js'
import { assertUsage } from '../../utils/assert.js'
import type { HydrationExecutor } from './hydrate.js'
import { createStatefulVariant } from './statefulLiveGraph.js'

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
      /** RLS status stays three-valued until the stateful variant makes the born-state decision. */
      rlsStatus?: RlsStatus
      /** Notify this identity's subscribers from OUTSIDE a routed batch — see statefulLiveGraph.ts. */
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
   *  RECOVER rather than surrender where the variant can: invalidate now, then rebuild precise state
   *  from the database. What that costs, and when it falls back to demoting, is the stateful variant's. */
  reseed(): void
  /** Unconditionally demote to coarse, with no attempt to recover. Internal to the terminal paths
   *  (a seed that cannot run, a fault); a routed coarse event goes through `reseed()`. */
  coarsen(): void
  /** Refcount 0 → terminal; frees state immediately. */
  destroy(): void
}

/** The shared state machine, as the variants are allowed to drive it. Handing this to a variant rather
 *  than letting each keep its own `state`/`seq`/readiness is what makes "every fire advances the sequence"
 *  and "every demotion settles the current cycle" true by construction instead of by repetition. */
type GraphCore = {
  state(): GraphState
  toState(next: GraphState): void
  invalidationSeq(): number
  ready(): Promise<void>
  /** Whether a change's table is one this graph watches. */
  watches(table: string): boolean
  /** Advance the invalidation sequence — the redeem fence's cursor. Every fire (exact, dirty, or a
   *  coarse/fault/coarsen demotion) must bump it so a demotion landing in a read window is caught. */
  fire(): void
  /** Re-arm the readiness promise for a NEW seed cycle. */
  armSeedCycle(): void
  /** Resolve the CURRENT cycle's readiness. */
  settle(): void
  /** Register what this variant frees when it is demoted. */
  setTeardown(free: () => void): void
  /** Free the variant's state and land on coarse, settling the current cycle. The one demotion door. */
  demote(): void
}

/** What ONE variant supplies over the shared core. */
type GraphVariant = {
  /** apply() while LIVE — OMITTED by a variant that is never live. `coarseApply` already answers for
   *  the `coarse` state, so a born-coarse variant supplying this could only ever be supplying dead code. */
  applyLive?(changes: RowChange[]): ApplyOutcome
  /** apply() while SEEDING — only the stateful variant can be in that state. */
  applySeeding?(changes: TableChange[]): ApplyOutcome
  /** An image-less mutation arrived and the sequence has ALREADY been advanced. What, if anything, this
   *  variant does beyond that invalidation. */
  reseed(): void
  /** Free state on DESTROY. (Demote-time freeing is registered via `core.setTeardown`; the two are
   *  deliberately allowed to differ.) */
  dispose(): void
}

function createLiveGraph(spec: LiveGraphSpec): LiveGraph {
  const core = createGraphCore(spec.tables)
  const variant =
    spec.kind === 'coarse'
      ? createCoarseVariant(core)
      : spec.kind === 'stateless'
        ? createStatelessVariant(spec.instantiate(), core)
        : createStatefulVariant(spec, core)
  return assembleLiveGraph(spec, core, variant)
}

// ── the shared state machine ────────────────────────────────────────

function createGraphCore(tables: string[]): GraphCore {
  let state: GraphState = 'coarse'
  let seq = 0
  const watched = new Set(tables)
  let teardown: () => void = () => {}

  let resolveSeed!: () => void
  let seedDone = new Promise<void>((resolve) => {
    resolveSeed = resolve
  })

  const core: GraphCore = {
    state: () => state,
    toState: (next) => {
      state = next
    },
    watches: (table) => watched.has(table),
    invalidationSeq: () => seq,
    ready: () => seedDone,
    fire: () => {
      seq++
    },
    armSeedCycle: () => {
      seedDone = new Promise<void>((resolve) => {
        resolveSeed = resolve
      })
    },
    settle: () => resolveSeed(),
    setTeardown: (free) => {
      teardown = free
    },
    demote: () => {
      teardown()
      state = 'coarse'
      resolveSeed()
    },
  }
  return core
}

/** Every variant falls back to this once demoted: a watched row change fires, nothing is resolved. */
function coarseApply(core: GraphCore, changes: TableChange[]): ApplyOutcome {
  const hit = changes.some((change) => core.watches(change.table) && rowChanged(change))
  if (hit) core.fire()
  return { invalidated: hit }
}

// ── the two variants that keep no seed machine ──────────────────────

/** Born coarse and never anything else: no operator graph, no row state, nothing to free, and an
 *  image-less event has nothing to recover — the invalidation `reseed()` already fired IS the remedy. */
function createCoarseVariant(core: GraphCore): GraphVariant {
  core.toState('coarse')
  core.settle()
  return {
    reseed: () => {},
    dispose: () => {},
  }
}

/** A compiled evaluator, no history. It is LIVE from construction — there is no baseline to scan, so
 *  there is no seed to block acquire on.
 *
 *  It NEVER reseeds, and that is a soundness argument rather than an optimisation: its precision comes
 *  from the compiled evaluator, not from accumulated state, so an image-less mutation cannot have
 *  corrupted anything. The invalidation `reseed()` already fired is the whole remedy — no demotion, no
 *  database round trip. */
function createStatelessVariant(evaluator: CompiledGraph, core: GraphCore): GraphVariant {
  core.toState('live')
  core.settle()
  return {
    applyLive(changes: RowChange[]): ApplyOutcome {
      // A key-only retraction cannot be decided without state → coarse (sound); everything else
      // resolves in row space via the compiled stateless evaluator.
      const commit: Change[] = []
      let coarse = false
      for (const change of changes) {
        if (change.kind !== 'insert' && change.old === undefined) coarse = true
        else commit.push({ table: change.table, kind: change.kind, old: change.old, new: change.new })
      }
      const result = commit.length > 0 ? evaluator.apply(commit) : { invalidated: false }
      const invalidated = result.invalidated || coarse
      if (invalidated) core.fire()
      return { invalidated }
    },
    reseed: () => {},
    dispose: () => {},
  }
}

// ── the shared protocol ─────────────────────────────────────────────

function assembleLiveGraph(spec: LiveGraphSpec, core: GraphCore, variant: GraphVariant): LiveGraph {
  return {
    instanceKey: spec.instanceKey,
    tables: spec.tables,
    state: core.state,
    invalidationSeq: core.invalidationSeq,
    ready: core.ready,
    apply(changes) {
      const state = core.state()
      if (state === 'destroyed') return { invalidated: false }
      if (state === 'coarse') return coarseApply(core, changes)
      // The router routes coarse markers to coarsen(), never to apply; the row-space state machine is
      // coarse-free (a coarse marker reaching here would be a router bug).
      // `applySeeding` exists exactly where `seeding` is reachable — only the stateful variant. The
      // fallback is the same answer that variant gives: a buffered change reports no invalidation.
      if (state === 'seeding') return variant.applySeeding?.(changes) ?? { invalidated: false }
      // `live` is reachable only for a variant that supplies `applyLive`; a born-coarse one never leaves
      // `coarse` and was answered above. Asserted rather than softened into a coarse fallback, which would
      // turn a compiler bug into a silent permanent over-fire.
      assertUsage(variant.applyLive, 'live graph: a variant with no applyLive reached the live state')
      return variant.applyLive(changes as RowChange[])
    },
    fault() {
      // A routed apply() threw (a latent bug left state possibly corrupt): permanently demote to
      // coarse so every subsequent change coarse-fires (sound over-fire) — the precise state can no
      // longer be trusted. The router surfaces the error (structured log).
      if (core.state() === 'destroyed') return
      core.fire() // advance seq so a fault landing in the read window is caught by the redeem fence
      core.demote()
    },
    reseed() {
      if (core.state() === 'destroyed') return
      core.fire() // whatever the variant does below, subscribers must learn NOW that the result may have moved
      variant.reseed()
    },
    coarsen() {
      // Unconditional demotion, for the paths that genuinely cannot recover. Feeds no row — precise state
      // is never fed a fabrication.
      if (core.state() === 'destroyed') return
      core.fire() // advance seq so a coarse event in the read window is caught by the redeem fence
      core.demote()
    },
    destroy() {
      variant.dispose()
      core.toState('destroyed')
      core.settle()
    },
  }
}
