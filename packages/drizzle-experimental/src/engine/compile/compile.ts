// QueryShape → GraphPlan. Classification comes first: an `untrackable` CoarseShape (no
// relation could be recovered) is a TYPED REJECTION — compiling it to an inputless graph
// would subscribe to nothing and never fire, a silent miss — while a recoverable
// CoarseShape becomes a live coarse graph that invalidates on any change to its tables. A
// SelectShape is either stateless (predicate-only — evaluated in row space, no operator
// state, no D2) or stateful (joins/aggregates/distinct/window/set-ops/EXISTS — a db-ivm
// dataflow, BUILT IN statefulCompiler.ts: it has its own dependencies, stage vocabulary and
// soundness audit, and a caller of this module needs none of them). Both paths feed changes through the shared
// input adapter and report invalidation = a non-empty data delta OR a non-empty dirty
// delta. Stages that cannot be evaluated exactly tap the dirty witness (over-fire, never
// miss); a whole-plan structural degradation falls back to the live coarse graph.

export {
  type GraphPlan,
  type CompiledGraph,
  type StatefulGraph,
  type FireResult,
  type SeedDescriptor,
  type Change,
  compileQuery,
  coarsePlan,
}

import { conjunctsOf } from '../../ir/predicateAlgebra.js'
import type { Predicate, QueryShape, SelectShape } from '../../ir/types.js'
import { assertUsage } from '../../utils/assert.js'
import { joinsExact } from './joinStage.js'
import { type Change, type InputPlan, applyChange, pushdownOf } from './pushdown.js'
import { projectFnOf } from './projectStage.js'
import { type Row, rowChanged } from './rowSpace.js'
import { isAggregate, semiJoinable, statefulPlan } from './statefulCompiler.js'

/** The outward fire signal. The dirty witness still computes data-delta vs dirty-delta INTERNALLY
 *  (that is how `invalidated` is decided and is what the oracle differential pins); only the split is
 *  not exposed — the caller acts on `invalidated` alone. */
type FireResult = { invalidated: boolean }

/** Everything the runtime needs to hydrate ONE stateful input. `residual` is the σ as an opaque
 *  hydration handle — its predicate leaves still carry the drizzle `src`, which the binding layer
 *  re-emits as WHERE. */
type SeedDescriptor = {
  inputId: string
  table: string
  /** The relation's schema when the query names a schema-qualified table (pgSchema): the seed reads
   *  `"schema"."table"`, so a same-named relation in another schema is never read via the search path. */
  schema?: string
  /** The relation's ROUTING identity (ir/relation.ts) — what incoming changes are matched on. Distinct
   *  from `table`/`schema`, which address the relation in the seed's SQL. */
  relationId: string
  alias: string
  primaryKey: string[]
  columns: string[] | '*'
  residual: Predicate
}

/** A compiled graph: `apply` feeds a whole commit and reports invalidation (coarse/stateless
 *  graphs are exactly this). */
type CompiledGraph = { apply(commit: Change[]): FireResult }

/** The stateful seam. `seedInput` + `flushSeed` feed a σ-pruned baseline with notifications MUTED
 *  (state, not change); `feedInput` + `runBatch` give per-input control so a self-join feeds each
 *  alias exactly once, never fanning across aliases. `feedDirtyWitness` feeds the dirty-only inputs that
 *  have no seed and no dataflow sink (subquery inner tables) — the live driver feeds seeds by descriptor,
 *  so witnesses need their own door or a change to one is silently dropped. */
type StatefulGraph = CompiledGraph & {
  readonly seeds: SeedDescriptor[]
  seedInput(inputId: string, rows: Row[]): void
  flushSeed(): void
  feedInput(inputId: string, change: Change): void
  feedDirtyWitness(change: Change): void
  runBatch(): FireResult
}
type GraphPlan = {
  tables: string[]
  stateless: boolean
  coarse: boolean
  instantiate(): CompiledGraph
}

function compileQuery(shape: QueryShape): GraphPlan {
  if (shape.kind === 'coarse') {
    assertUsage(
      !shape.untrackable,
      `untrackable read cannot be compiled to a live graph (no routing relation recovered): ${shape.reason}`,
    )
    return coarsePlan(shape.tables)
  }
  return compileSelect(shape)
}

function compileSelect(shape: SelectShape): GraphPlan {
  if (shape.setOps.length > 0) {
    if (shape.setOps.some((op) => op.right.kind !== 'select')) return coarsePlan(shape.tables)
    return statefulPlan(shape)
  }
  if (isStateless(shape)) return statelessPlan(shape)
  if (shape.joins.length > 0 && !joinsExact(shape)) return coarsePlan(shape.tables)
  return statefulPlan(shape)
}

// ── Coarse (live) plan ──────────────────────────────────────────────

/** A recoverable coarse graph: any change to a watched table invalidates (dirty). */
function coarsePlan(tables: string[]): GraphPlan {
  const watched = new Set(tables)
  return {
    tables,
    stateless: true,
    coarse: true,
    instantiate: () => ({
      apply(commit) {
        const invalidated = commit.some((change) => watched.has(change.table) && rowChanged(change))
        return { invalidated }
      },
    }),
  }
}

// ── Stateless (row-space) plan ──────────────────────────────────────

function statelessPlan(shape: SelectShape): GraphPlan {
  const { inputs } = pushdownOf(shape)
  const baseReal = new Set(inputs.map((plan) => plan.relationId))
  const extraTables = new Set(shape.tables.filter((table) => !baseReal.has(table)))
  const windowDirty = hasLimit(shape)
  const projectFn = projectFnOf(shape)
  return {
    tables: shape.tables,
    stateless: true,
    coarse: false,
    instantiate: () => statelessEvaluator(inputs, extraTables, windowDirty, projectFn),
  }
}

function statelessEvaluator(
  inputs: InputPlan[],
  extraTables: Set<string>,
  windowDirty: boolean,
  projectFn: (row: Row) => string,
): CompiledGraph {
  return {
    apply(commit) {
      const acc = new Map<string, number>()
      let dirty = false
      for (const change of commit) {
        if (extraTables.has(change.table) && rowChanged(change)) dirty = true
        for (const plan of inputs) {
          if (plan.relationId !== change.table) continue
          const delta = applyChange(plan, change)
          if (delta.dirty) dirty = true
          if (windowDirty) {
            if (delta.data.length > 0) dirty = true
            continue // LIMIT without a total order: the subset is unspecified → dirty only
          }
          for (const [row, multiplicity] of delta.data) {
            const key = projectFn(row)
            const sum = (acc.get(key) ?? 0) + multiplicity
            if (sum === 0) acc.delete(key)
            else acc.set(key, sum)
          }
        }
      }
      return { invalidated: acc.size > 0 || dirty }
    },
  }
}

function isStateless(shape: SelectShape): boolean {
  return (
    shape.joins.length === 0 &&
    shape.setOps.length === 0 &&
    !isAggregate(shape) &&
    shape.distinct.on === false &&
    !(hasLimit(shape) && shape.orderBy.length > 0) &&
    !hasPositiveExists(shape.where)
  )
}

function hasLimit(shape: SelectShape): boolean {
  return shape.limit !== undefined || shape.offset !== undefined
}

function hasPositiveExists(where: Predicate | undefined): boolean {
  if (!where) return false
  return conjunctsOf(where).some(
    (conjunct) => conjunct.kind === 'exists' && !conjunct.negated && semiJoinable(conjunct),
  )
}
