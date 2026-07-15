// QueryShape → GraphPlan. Classification comes first: an `untrackable` CoarseShape (no
// relation could be recovered) is a TYPED REJECTION — compiling it to an inputless graph
// would subscribe to nothing and never fire, a silent miss — while a recoverable
// CoarseShape becomes a live coarse graph that invalidates on any change to its tables. A
// SelectShape is either stateless (predicate-only — evaluated in row space, no operator
// state, no D2) or stateful (joins/aggregates/distinct/window/set-ops/EXISTS — a db-ivm
// dataflow built in SQL stage order). Both paths feed captured changes through the shared
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
}

import { conjunctsOf } from '../extract/predicate.js'
import {
  type IStreamBuilder,
  type RootStreamBuilder,
  D2,
  MultiSet,
  consolidate,
  distinct,
  keyBy,
  map,
  output,
} from '../graph/ivm.js'
import type { Correlation, Predicate, QueryShape, SelectShape } from '../ir/types.js'
import { assertUsage } from '../utils/assert.js'
import { applyAggregate } from './aggregateStage.js'
import { type DirtySink, createDirtySink } from './dirty.js'
import { type ExistsSpec, applyExists, correlationKey } from './existsStage.js'
import { applyJoins, joinsExact } from './joinStage.js'
import { type Change, type InputPlan, applyChange, pushdownOf } from './pushdown.js'
import { projectFnOf, wholeRowFn } from './projectStage.js'
import { type Row, qualifiedKey } from './rowSpace.js'
import { type SetOpBranch, applySetOps } from './setOpsStage.js'
import { applyWindow } from './windowStage.js'

type FireResult = { data: boolean; dirty: boolean; invalidated: boolean }

/** The ticket-5 runtime seam (T5.A0): everything the graph runtime needs to hydrate ONE
 *  stateful input — a stable id, the real relation + alias, its PK (retraction key), the
 *  demanded columns, and the σ residual as the opaque hydration handle (its predicate
 *  leaves still carry the drizzle `src`, which the binding layer re-emits as WHERE). */
type SeedDescriptor = {
  inputId: string
  table: string
  alias: string
  primaryKey: string[]
  columns: string[] | '*'
  residual: Predicate
  shadowNeed: boolean
}

/** A compiled graph — the ticket-4 surface, unchanged (coarse/stateless graphs are exactly
 *  this). `apply` feeds a whole commit and reports invalidation. */
type CompiledGraph = { apply(commit: Change[]): FireResult }

/** The ticket-5 seam on a STATEFUL compiled graph. `seeds` lists the inputs to hydrate;
 *  `seedInput` + `flushSeed` feed a σ-pruned baseline with notifications MUTED (state, not
 *  change); `feedInput` + `runBatch` give the live graph per-input control (so a self-join
 *  feeds each alias exactly once, never fanning across aliases). `apply` remains the
 *  table-fanned floor path — `feedInput`/`runBatch` compose from the same primitives. */
type StatefulGraph = CompiledGraph & {
  readonly seeds: SeedDescriptor[]
  seedInput(inputId: string, rows: Row[]): void
  flushSeed(): void
  feedInput(inputId: string, change: Change): void
  runBatch(): FireResult
}
type GraphPlan = {
  tables: string[]
  stateless: boolean
  coarse: boolean
  inputs: Array<{ alias: string; table: string; columns: string[] | '*'; shadowNeed: boolean }>
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
    inputs: tables.map((table) => ({ alias: table, table, columns: '*' as const, shadowNeed: false })),
    instantiate: () => ({
      apply(commit) {
        const dirty = commit.some((change) => watched.has(change.table) && changed(change))
        return { data: false, dirty, invalidated: dirty }
      },
    }),
  }
}

// ── Stateless (row-space) plan ──────────────────────────────────────

function statelessPlan(shape: SelectShape): GraphPlan {
  const { inputs } = pushdownOf(shape)
  const baseReal = new Set(inputs.map((plan) => plan.table))
  const extraTables = new Set(shape.tables.filter((table) => !baseReal.has(table)))
  const windowDirty = hasLimit(shape)
  const projectFn = projectFnOf(shape)
  return {
    tables: shape.tables,
    stateless: true,
    coarse: false,
    inputs: inputs.map(descriptor),
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
        if (extraTables.has(change.table) && changed(change)) dirty = true
        for (const plan of inputs) {
          if (plan.table !== change.table) continue
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
      const data = acc.size > 0
      return { data, dirty, invalidated: data || dirty }
    },
  }
}

// ── Stateful (dataflow) plan ────────────────────────────────────────

function statefulPlan(shape: SelectShape): GraphPlan {
  const { inputs } = pushdownOf(shape)
  return {
    tables: shape.tables,
    stateless: false,
    coarse: false,
    inputs: inputs.map(descriptor),
    instantiate: () => statefulGraph(shape),
  }
}

type FeedInput = { plan: InputPlan; builder: RootStreamBuilder<Row> }
type SeedInput = { descriptor: SeedDescriptor; plan: InputPlan; builder: RootStreamBuilder<Row> }

function statefulGraph(shape: SelectShape): StatefulGraph {
  const graph = new D2()
  const dirty = createDirtySink()
  const registry = new Map<string, FeedInput[]>()
  const genesis: RootStreamBuilder<Row>[] = []
  let dataFired = false
  let pendingDirty = false

  const combined =
    shape.setOps.length > 0
      ? buildSetOps(graph, shape, registry, genesis, dirty)
      : buildBranch(graph, shape, registry, genesis, dirty)
  if (combined) {
    combined.pipe(
      consolidate(),
      output((data) => {
        if (data.getInner().length > 0) dataFired = true
      }),
    )
  }
  dirty.buildTerminal()
  graph.finalize()

  // Materialize each ungrouped aggregate's unit-group zero row (a net-zero poke touches the
  // reduce key so its baseline is the count-0 / sum-NULL record) before any real input.
  for (const input of genesis)
    input.sendData(
      new MultiSet([
        [{}, 1],
        [{}, -1],
      ]),
    )
  graph.run()
  dataFired = false
  dirty.reset()

  // The seedable inputs are every real db-backed feed (a self-join yields one per alias); the
  // dirty-only DISCARD feeds and the internal aggregate genesis inputs hold no hydrated state.
  const seedable = new Map<string, SeedInput>()
  for (const feeds of registry.values())
    for (const feed of feeds)
      if (feed.builder !== DISCARD) {
        assertUsage(!seedable.has(feed.plan.alias), `duplicate seed input id: ${feed.plan.alias}`)
        seedable.set(feed.plan.alias, { descriptor: descriptorOf(feed.plan), plan: feed.plan, builder: feed.builder })
      }

  // The one place a captured change becomes an engine feed: project + σ-filter via the input
  // adapter, buffer the pruned delta, and remember any row-space dirty. `apply` fans a whole
  // commit table-wise; `feedInput` gives the live graph per-alias control (no cross-alias fan).
  const pump = (plan: InputPlan, builder: RootStreamBuilder<Row>, change: Change): void => {
    const delta = applyChange(plan, change)
    if (delta.dirty) pendingDirty = true
    if (delta.data.length > 0) builder.sendData(new MultiSet(delta.data))
  }
  const runBatch = (): FireResult => {
    graph.run()
    if (pendingDirty) dirty.signal()
    const d = dirty.fired()
    const result = { data: dataFired, dirty: d, invalidated: dataFired || d }
    dataFired = false
    pendingDirty = false
    dirty.reset()
    return result
  }

  return {
    apply(commit) {
      for (const change of commit)
        for (const feed of registry.get(change.table) ?? []) pump(feed.plan, feed.builder, change)
      return runBatch()
    },
    seeds: [...seedable.values()].map((input) => input.descriptor),
    // A MUTED baseline feed for ONE input: raw rows are projected + σ-filtered exactly like a
    // captured insert (reusing the input adapter), then buffered — no `run`, no notification.
    seedInput(inputId, rows) {
      const input = seedable.get(inputId)
      assertUsage(input, `seedInput: unknown input id ${inputId}`)
      const data: Array<[Row, number]> = []
      for (const raw of rows)
        for (const entry of applyChange(input.plan, { table: input.plan.table, kind: 'insert', new: raw }).data)
          data.push(entry)
      if (data.length > 0) input.builder.sendData(new MultiSet(data))
    },
    // Drain the buffered baselines into operator state, still muted (state, not change).
    flushSeed() {
      graph.run()
      dataFired = false
      pendingDirty = false
      dirty.reset()
    },
    feedInput(inputId, change) {
      const input = seedable.get(inputId)
      assertUsage(input, `feedInput: unknown input id ${inputId}`)
      pump(input.plan, input.builder, change)
    },
    runBatch,
  }
}

function descriptorOf(plan: InputPlan): SeedDescriptor {
  return {
    inputId: plan.alias,
    table: plan.table,
    alias: plan.alias,
    primaryKey: plan.primaryKey,
    columns: plan.columns,
    residual: plan.residual,
    shadowNeed: plan.shadowNeed,
  }
}

/** Build one SELECT branch into its projected-string stream (or `undefined` for a dirty-
 *  only degradation), registering every input it feeds. Stage order = SQL order: FROM/JOIN
 *  → WHERE residual → EXISTS → GROUP BY/HAVING → DISTINCT → ORDER/LIMIT → projection. */
function buildBranch(
  graph: D2,
  shape: SelectShape,
  registry: Map<string, FeedInput[]>,
  genesis: RootStreamBuilder<Row>[],
  dirty: DirtySink,
): IStreamBuilder<string> | undefined {
  const { specs, rest } = extractExists(shape)
  const trimmed: SelectShape = { ...shape, where: rest }
  const { inputs, crossResidual } = pushdownOf(trimmed)

  const streams = new Map<string, IStreamBuilder<Row>>()
  const covered = new Set<string>()
  for (const plan of inputs) {
    const builder = graph.newInput<Row>()
    register(registry, plan, builder)
    streams.set(plan.alias, builder)
    covered.add(plan.table)
  }

  const existsSpecs = specs.map((leaf) => buildExistsSpec(graph, leaf, registry, covered))
  // Scalar/negated-subquery inner tables (not a base input, not a set-op branch) fire dirty
  // on any change — the §5.4 scalar-subquery row.
  for (const table of subqueryInnerTables(trimmed)) if (!covered.has(table)) registerDirtyTable(registry, table)

  const joined = applyJoins(trimmed, streams, crossResidual, dirty)
  if (joined.kind === 'degrade') {
    for (const stream of streams.values()) dirty.tap(stream)
    return undefined
  }
  const afterExists = applyExists(joined.stream, existsSpecs, dirty)

  // Aggregate output flows THROUGH the shared window → projection pipeline in SQL order —
  // there is no aggregate-specific terminal, so GROUP BY … ORDER BY … LIMIT is a topK over
  // the aggregate records exactly like any other row stream.
  const aggregated = isAggregate(shape) ? applyAggregate(graph, shape, afterExists, dirty) : undefined
  if (aggregated?.genesis) genesis.push(aggregated.genesis)
  const rows = aggregated ? aggregated.stream : afterExists

  const windowed = applyWindow(shape, rows, dirty)
  const projected = windowed.pipe(map(projectRowWith(shape)))
  if (shape.distinct.on === true) return dedupeStrings(projected)
  if (shape.distinct.on === 'columns') dirty.tap(projected) // DISTINCT ON → dirty at its input
  return projected
}

/** SQL DISTINCT over the projected tuples: 1→2 multiplicity edges are silent. */
function dedupeStrings(stream: IStreamBuilder<string>): IStreamBuilder<string> {
  return stream.pipe(
    keyBy((value: string) => value),
    distinct(),
    map(([, value]) => value),
  )
}

function buildSetOps(
  graph: D2,
  shape: SelectShape,
  registry: Map<string, FeedInput[]>,
  genesis: RootStreamBuilder<Row>[],
  dirty: DirtySink,
): IStreamBuilder<string> | undefined {
  const main = buildBranch(graph, { ...shape, setOps: [] }, registry, genesis, dirty)
  const branches: SetOpBranch[] = []
  for (const op of shape.setOps) {
    const right = op.right
    if (right.kind !== 'select') return dirtyOnly(main, dirty)
    const stream = buildBranch(graph, right, registry, genesis, dirty)
    if (!stream) return dirtyOnly(main, dirty)
    branches.push({ kind: op.type, stream })
  }
  if (!main) return undefined
  return applySetOps(main, branches, dirty)
}

/** A dirty-only fallback: the union already tapped the branches it could build; tap the
 *  main branch too so any change invalidates. */
function dirtyOnly(main: IStreamBuilder<string> | undefined, dirty: DirtySink): undefined {
  if (main) dirty.tap(main)
  return undefined
}

// ── EXISTS extraction ───────────────────────────────────────────────

type ExistsLeaf = Extract<Predicate, { kind: 'exists' }>

/** Pull the semi-joinable EXISTS/IN leaves out of WHERE (single equi-correlation, single
 *  inner base table). The rest — negated exists, scalar/complex subqueries — stay in the
 *  predicate and widen + tap dirty through the normal residual path. */
function extractExists(shape: SelectShape): { specs: ExistsLeaf[]; rest?: Predicate } {
  if (!shape.where) return { specs: [] }
  const specs: ExistsLeaf[] = []
  const rest: Predicate[] = []
  for (const conjunct of conjunctsOf(shape.where)) {
    if (conjunct.kind === 'exists' && !conjunct.negated && semiJoinable(conjunct)) specs.push(conjunct)
    else rest.push(conjunct)
  }
  return { specs, rest: rest.length ? conjunction(rest) : undefined }
}

function semiJoinable(leaf: ExistsLeaf): boolean {
  return (
    leaf.inner !== undefined &&
    leaf.inner.kind === 'select' &&
    leaf.inner.joins.length === 0 &&
    leaf.inner.setOps.length === 0 &&
    leaf.correlations?.length === 1
  )
}

function buildExistsSpec(
  graph: D2,
  leaf: ExistsLeaf,
  registry: Map<string, FeedInput[]>,
  covered: Set<string>,
): ExistsSpec {
  const inner = leaf.inner as SelectShape
  const correlation = leaf.correlations![0] as Correlation
  const { inputs } = pushdownOf(inner)
  const innerPlan = inputs.find((plan) => plan.alias === inner.from.alias)!
  const builder = graph.newInput<Row>()
  register(registry, innerPlan, builder)
  covered.add(innerPlan.table)
  const innerKeyName = qualifiedKey(correlation.inner.table, correlation.inner.column)
  const innerKeys = builder.pipe(keyBy((row: Row) => correlationKey(row, innerKeyName)))
  return { negated: false, outerKey: qualifiedKey(correlation.outer.table, correlation.outer.column), innerKeys }
}

// ── Shared helpers ──────────────────────────────────────────────────

function descriptor(plan: InputPlan): GraphPlan['inputs'][number] {
  return { alias: plan.alias, table: plan.table, columns: plan.columns, shadowNeed: plan.shadowNeed }
}

function register(registry: Map<string, FeedInput[]>, plan: InputPlan, builder: RootStreamBuilder<Row>): void {
  const list = registry.get(plan.table) ?? []
  list.push({ plan, builder })
  registry.set(plan.table, list)
}

/** Register an inner subquery / scalar-subquery table as a dirty-only input: it has no
 *  dataflow sink, but a change to it fires the dirty witness (§5.4 scalar-subquery row). */
function registerDirtyTable(registry: Map<string, FeedInput[]>, table: string): void {
  const plan: InputPlan = {
    alias: table,
    table,
    primaryKey: [],
    columns: '*',
    residual: { kind: 'true' },
    gate: { kind: 'true' },
    dirtyActive: true,
    shadowNeed: false,
  }
  register(registry, plan, DISCARD)
}

// A dirty-only input needs no dataflow sink; feeding it sends no data (the adapter yields
// data rows, but they are discarded), only the adapter's dirty flag matters.
const DISCARD = { sendData() {} } as unknown as RootStreamBuilder<Row>

/** The inner tables of scalar / negated / non-decorrelatable subqueries referenced by this
 *  branch (opaque or exists leaves in WHERE/HAVING/ON, opaque projection items). Excludes
 *  set-op branch tables, which are built as their own branches. */
function subqueryInnerTables(shape: SelectShape): Set<string> {
  const tables = new Set<string>()
  const visit = (pred: Predicate | undefined): void => {
    if (!pred) return
    if (pred.kind === 'and' || pred.kind === 'or') pred.parts.forEach(visit)
    else if (pred.kind === 'not') visit(pred.operand)
    else if (pred.kind === 'unknown' || pred.kind === 'exists') for (const table of pred.tables) tables.add(table)
  }
  visit(shape.where)
  visit(shape.having)
  for (const join of shape.joins) visit(join.on)
  for (const item of shape.projection.items)
    if (item.kind === 'opaque') for (const table of item.tables) tables.add(table)
  return tables
}

function isAggregate(shape: SelectShape): boolean {
  return shape.groupBy.length > 0 || shape.groupByOpaque || shape.projection.items.some((item) => item.kind === 'agg')
}

function projectRowWith(shape: SelectShape): (row: Row) => string {
  return isAggregate(shape) ? wholeRowFn : projectFnOf(shape)
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

function changed(change: Change): boolean {
  if (!change.old || !change.new) return true
  return JSON.stringify(sortedEntries(change.old)) !== JSON.stringify(sortedEntries(change.new))
}

function sortedEntries(row: Row): Array<[string, unknown]> {
  return Object.keys(row)
    .sort()
    .map((key) => [key, row[key]])
}

function conjunction(parts: Predicate[]): Predicate {
  return parts.length === 1 ? parts[0]! : { kind: 'and', parts }
}
