export { statefulPlan, isAggregate, semiJoinable }

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
import type { Correlation, Predicate, SelectShape } from '../ir/types.js'
import { applyAggregate } from './aggregateStage.js'
import { type DirtySink, createDirtySink } from './dirty.js'
import { type InputAudit, createInputAudit } from './inputAudit.js'
import { type ExistsSpec, applyExists, correlationKey } from './existsStage.js'
import { applyJoins } from './joinStage.js'
import { type Change, type InputPlan, applyChange, pushdownOf } from './pushdown.js'
import { projectFnOf, wholeRowFn } from './projectStage.js'
import { type Row, qualifiedKey } from './rowSpace.js'
import { type SetOpBranch, applySetOps } from './setOpsStage.js'
import { applyWindow } from './windowStage.js'
import { assertUsage } from '../utils/assert.js'
import type { FireResult, GraphPlan, SeedDescriptor, StatefulGraph } from './compile.js'

// THE STATEFUL COMPILER — a SelectShape whose semantics need operator STATE (joins, aggregates, distinct,
// window functions, set-ops, EXISTS) becomes a db-ivm dataflow, built in SQL stage order.
//
// This is a different job from the one `compile.ts` does, and it changes for different reasons. The public
// compiler classifies a shape and hands back a plan; this builds a dataflow, and it carries its own
// dependencies (D2, every operator stage), its own stage vocabulary, and its own soundness audit — the
// input audit and the dirty witness that make a stage which cannot be evaluated exactly OVER-fire rather
// than miss. A caller needs only `compileQuery`; nothing outside this module needs any of that.
//
// It imports ONLY TYPES from compile.ts, so the dependency is one-directional at runtime: the shape
// classifiers the public compiler shares with the dataflow builder (`isAggregate`, `semiJoinable`) live
// HERE and are re-used there, rather than the other way round, precisely to keep it that way.

// ── Stateful (dataflow) plan ────────────────────────────────────────

function statefulPlan(shape: SelectShape): GraphPlan {
  return {
    tables: shape.tables,
    stateless: false,
    coarse: false,
    instantiate: () => statefulGraph(shape),
  }
}

type FeedInput = { plan: InputPlan; builder?: RootStreamBuilder<Row> }
type SeedInput = { descriptor: SeedDescriptor; plan: InputPlan; builder: RootStreamBuilder<Row> }

function statefulGraph(shape: SelectShape): StatefulGraph {
  const graph = new D2()
  const audit = createInputAudit()
  const dirty = createDirtySink(audit)
  const registry = new Map<string, FeedInput[]>()
  const genesis: RootStreamBuilder<Row>[] = []
  let dataFired = false
  let pendingDirty = false

  const combined =
    shape.setOps.length > 0
      ? buildSetOps(graph, shape, registry, genesis, dirty, audit)
      : buildBranch(graph, shape, registry, genesis, dirty, audit)
  audit.consume(combined)
  if (combined) {
    combined.pipe(
      consolidate(),
      output((data) => {
        if (data.getInner().length > 0) dataFired = true
      }),
    )
  }
  dirty.buildTerminal()
  // Every fed input must be observable BEFORE the graph is sealed — see inputAudit.ts.
  audit.assertNoStrandedInputs()
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
      if (feed.builder !== undefined) {
        assertUsage(!seedable.has(feed.plan.alias), `duplicate seed input id: ${feed.plan.alias}`)
        seedable.set(feed.plan.alias, { descriptor: descriptorOf(feed.plan), plan: feed.plan, builder: feed.builder })
      }

  // The one place a captured change becomes an engine feed: project + σ-filter via the input
  // adapter, buffer the pruned delta, and remember any row-space dirty. `apply` fans a whole
  // commit table-wise; `feedInput` gives the live graph per-alias control (no cross-alias fan).
  const pump = (plan: InputPlan, builder: RootStreamBuilder<Row> | undefined, change: Change): void => {
    const delta = applyChange(plan, change)
    if (delta.dirty) pendingDirty = true
    if (builder && delta.data.length > 0) builder.sendData(new MultiSet(delta.data))
  }
  const runBatch = (): FireResult => {
    graph.run()
    if (pendingDirty) dirty.signal()
    const d = dirty.fired()
    const result = { invalidated: dataFired || d }
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
        for (const entry of applyChange(input.plan, { table: input.plan.relationId, kind: 'insert', new: raw }).data)
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
    schema: plan.schema,
    relationId: plan.relationId,
    alias: plan.alias,
    primaryKey: plan.primaryKey,
    columns: plan.columns,
    residual: plan.residual,
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
  audit: InputAudit,
): IStreamBuilder<string> | undefined {
  const { specs, rest } = extractExists(shape)
  const trimmed: SelectShape = { ...shape, where: rest }
  const { inputs, crossResidual } = pushdownOf(trimmed)

  const streams = new Map<string, IStreamBuilder<Row>>()
  const covered = new Set<string>()
  for (const plan of inputs) {
    const builder = graph.newInput<Row>()
    register(registry, plan, builder)
    audit.declare(builder, plan.relationId)
    streams.set(plan.alias, builder)
    covered.add(plan.relationId)
  }

  const existsRoots: Array<IStreamBuilder<Row>> = []
  const existsSpecs = specs.map((leaf) => buildExistsSpec(graph, leaf, registry, covered, audit, existsRoots))
  // Scalar/negated-subquery inner tables (not a base input, not a set-op branch) fire dirty
  // on any change — the scalar-subquery row.
  for (const table of subqueryInnerTables(trimmed)) if (!covered.has(table)) registerDirtyTable(registry, table)

  const joined = applyJoins(trimmed, streams, crossResidual, dirty)
  if (joined.kind === 'degrade') {
    // Tap EVERY stream built for this branch, not just the base inputs. The EXISTS inner roots were
    // already created and registered above, so leaving them untapped strands them exactly like the
    // set-op arms once were: fed on every change, observed by nothing. The audit catches this shape now,
    // but the tap is what makes the degradation SOUND.
    for (const stream of [...streams.values(), ...existsRoots]) dirty.tap(stream)
    return undefined
  }
  const afterExists = applyExists(joined.stream, existsSpecs)

  // Aggregate output flows THROUGH the shared window → projection pipeline in SQL order —
  // there is no aggregate-specific terminal, so GROUP BY … ORDER BY … LIMIT is a topK over
  // the aggregate records exactly like any other row stream.
  const aggregated = isAggregate(shape) ? applyAggregate(graph, shape, afterExists, dirty) : undefined
  if (aggregated?.genesis) genesis.push(aggregated.genesis)
  const rows = aggregated ? aggregated.stream : afterExists

  const windowed = applyWindow(shape, rows, dirty)
  const projected = windowed.pipe(map(projectRowWith(shape)))
  // Consuming this branch's output accounts for every root behind it. Linking (rather than marking them
  // accounted here) is what makes an ABANDONED branch detectable: the roots stay unaccounted until some
  // caller actually consumes the stream.
  const out = shape.distinct.on === true ? dedupeStrings(projected) : projected
  audit.link(out, [...streams.values(), ...existsRoots])
  if (shape.distinct.on === 'columns') {
    audit.link(projected, [...streams.values(), ...existsRoots])
    dirty.tap(projected) // DISTINCT ON → dirty at its input
  }
  return out
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
  audit: InputAudit,
): IStreamBuilder<string> | undefined {
  const main = buildBranch(graph, { ...shape, setOps: [] }, registry, genesis, dirty, audit)
  const branches: SetOpBranch[] = []
  let degrade = false
  for (const op of shape.setOps) {
    // Non-select arms are already coarse-planned upstream; here every arm is a select. An arm that
    // can't be built exactly, or one carrying per-arm ORDER/LIMIT/OFFSET (which selects WHICH rows
    // the arm contributes — unsound to ignore under a downstream UNION distinct, where a bounded
    // arm's row swap can add a value new to the union), forces a dirty degradation.
    const stream =
      op.right.kind === 'select' ? buildBranch(graph, op.right, registry, genesis, dirty, audit) : undefined
    if (stream) branches.push({ kind: op.type, stream })
    if (!stream || op.orderBy.length > 0 || op.limit !== undefined || op.offset !== undefined) degrade = true
  }
  // A degraded MAIN must still fall through to `dirtyOnly`, not return early. Returning here left every
  // successfully-built ARM untapped: its inputs were registered and fed, but nothing consumed the stream —
  // no output pipe (the caller only pipes a defined stream) and no dirty tap — so a change to an arm-only
  // table reported `invalidated: false` on a plan that was otherwise PRECISE. That is a missed
  // invalidation, not a safe over-fire. `dirtyOnly` was already written for this case: its `main` parameter
  // is optional and it guards with `if (main)`.
  if (degrade || !main) return dirtyOnly(main, branches, dirty)
  const combined = applySetOps(main, branches, dirty)
  audit.link(combined ?? main, [main, ...branches.map((branch) => branch.stream)])
  return combined
}

/** A dirty-only fallback: tap the main branch and every built set-op branch so any change
 *  invalidates (a branch that degraded internally already tapped its own streams). */
function dirtyOnly(main: IStreamBuilder<string> | undefined, branches: SetOpBranch[], dirty: DirtySink): undefined {
  if (main) dirty.tap(main)
  for (const branch of branches) dirty.tap(branch.stream)
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
  audit: InputAudit,
  roots: Array<IStreamBuilder<Row>>,
): ExistsSpec {
  const inner = leaf.inner as SelectShape
  const correlation = leaf.correlations![0] as Correlation
  const { inputs } = pushdownOf(inner)
  const innerPlan = inputs.find((plan) => plan.alias === inner.from.alias)!
  const builder = graph.newInput<Row>()
  register(registry, innerPlan, builder)
  audit.declare(builder, innerPlan.relationId)
  roots.push(builder)
  covered.add(innerPlan.relationId)
  const innerKeyName = qualifiedKey(correlation.inner.table, correlation.inner.column)
  const innerKeys = builder.pipe(keyBy((row: Row) => correlationKey(row, innerKeyName)))
  return { outerKey: qualifiedKey(correlation.outer.table, correlation.outer.column), innerKeys }
}

// ── Shared helpers ──────────────────────────────────────────────────

function register(registry: Map<string, FeedInput[]>, plan: InputPlan, builder?: RootStreamBuilder<Row>): void {
  const list = registry.get(plan.relationId) ?? []
  list.push({ plan, builder })
  registry.set(plan.relationId, list)
}

/** Register an inner subquery / scalar-subquery relation as a dirty-only input: it has no
 *  dataflow sink, but a change to it fires the dirty witness (the scalar-subquery row). Keyed by
 *  routing identity; it is never seeded, so it needs no SQL-addressable name. */
function registerDirtyTable(registry: Map<string, FeedInput[]>, relationId: string): void {
  const plan: InputPlan = {
    alias: relationId,
    table: relationId,
    relationId,
    primaryKey: [],
    columns: '*',
    residual: { kind: 'true' },
    gate: { kind: 'true' },
    dirtyActive: true,
  }
  register(registry, plan) // no builder: a dirty-only input has no dataflow sink; only its adapter's dirty flag matters
}

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

function conjunction(parts: Predicate[]): Predicate {
  return parts.length === 1 ? parts[0]! : { kind: 'and', parts }
}
