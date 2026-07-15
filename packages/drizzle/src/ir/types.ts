// The shared, ORM-agnostic intermediate representation. Nothing in ir/ imports
// drizzle-orm: binding/ and extract/ produce these shapes; ir/eval, and later
// compile/graph/, consume them. A drizzle SQL handle rides along opaquely as
// `SqlSource` so later hydration can rebuild residual WHERE with drizzle's own
// and(...) — the engine never inspects it, which keeps this boundary drizzle-free.

export type Dialect = 'pg' | 'mysql' | 'sqlite'

/** Three-valued truth. `undefined` is SQL's third value (a comparison touching
 *  NULL, a not-yet-captured column, or an operand the compiler could not read). */
export type Tri = true | false | undefined

/** Opaque handle to the producer's source node (a drizzle `SQL`), carried for
 *  later residual reconstruction. Typed `unknown` to keep ir/ drizzle-free. */
export type SqlSource = unknown

// ── Tables & columns ────────────────────────────────────────────────

/** A base table as it appears in a query: its real (unaliased) name — the name
 *  write events arrive under — plus the alias it is referenced by, its schema,
 *  and the database names of its primary-key columns (empty when none is usable). */
export type TableRef = {
  name: string
  alias: string
  schema?: string
  primaryKey: string[]
}

/** A reference to one column, resolved to its database name and owning table alias. */
export type ColRef = {
  table: string // owning table's alias (matches TableRef.alias)
  column: string // database column name
}

// ── Scalar operands ─────────────────────────────────────────────────

export type ScalarExpr =
  | { kind: 'col'; ref: ColRef }
  | { kind: 'lit'; value: unknown } // a bound value (Param) or bare literal
  | { kind: 'placeholder'; name: string } // resolved from execute-time bindings; unknown until then
  | { kind: 'unknown' } // subquery / unreadable operand — opaque per row

// ── Predicate ───────────────────────────────────────────────────────

export type CompareOp = '=' | '<>' | '>' | '>=' | '<' | '<='

export type Predicate =
  | { kind: 'true' }
  | { kind: 'false' }
  | { kind: 'and'; parts: Predicate[] }
  | { kind: 'or'; parts: Predicate[] }
  | { kind: 'not'; operand: Predicate }
  | { kind: 'compare'; op: CompareOp; left: ScalarExpr; right: ScalarExpr; src?: SqlSource }
  | { kind: 'in'; expr: ScalarExpr; values: ScalarExpr[]; negated: boolean; src?: SqlSource }
  | {
      kind: 'like'
      expr: ScalarExpr
      pattern: string | null
      caseInsensitive: boolean
      negated: boolean
      src?: SqlSource
    }
  | { kind: 'isNull'; expr: ScalarExpr; negated: boolean; src?: SqlSource }
  | { kind: 'between'; expr: ScalarExpr; low: ScalarExpr; high: ScalarExpr; src?: SqlSource }
  | { kind: 'exists'; negated: boolean; tables: string[]; src?: SqlSource } // decorrelated at compile; unknown per row
  | { kind: 'unknown'; tables: string[]; src?: SqlSource } // an atom the parser couldn't read

// ── Query shape ─────────────────────────────────────────────────────

export type AggFn = 'count' | 'sum' | 'avg' | 'min' | 'max'
export type AggCall = { fn: AggFn; arg?: ColRef; star: boolean; distinct: boolean }

export type ProjItem =
  | { kind: 'col'; ref: ColRef; as?: string }
  | { kind: 'agg'; call: AggCall; as?: string }
  | { kind: 'opaque'; columns: ColRef[]; tables: string[]; window: boolean; as?: string }

/** `star` marks a whole-row select (no explicit projection list). */
export type Projection = { items: ProjItem[]; star: boolean }

export type OrderKey = {
  expr: { kind: 'col'; ref: ColRef } | { kind: 'opaque'; columns: ColRef[] }
  direction: 'asc' | 'desc'
  nulls?: 'first' | 'last'
}

export type Bound = { kind: 'value'; value: number } | { kind: 'placeholder'; name: string }

export type Distinct = { on: false } | { on: true } | { on: 'columns'; columns: ColRef[] }

export type JoinShape = {
  type: 'inner' | 'left' | 'right' | 'full' | 'cross'
  table: TableRef
  on?: Predicate
}

export type SetOpKind = 'union' | 'unionAll' | 'intersect' | 'intersectAll' | 'except' | 'exceptAll'

export type SetOp = {
  type: SetOpKind
  right: QueryShape
  orderBy: OrderKey[]
  limit?: Bound
  offset?: Bound
}

export type SelectShape = {
  kind: 'select'
  dialect: Dialect
  from: TableRef
  joins: JoinShape[]
  where?: Predicate
  projection: Projection
  groupBy: ColRef[]
  groupByOpaque: boolean // a GROUP BY expression the parser couldn't reduce to a column
  having?: Predicate
  distinct: Distinct
  orderBy: OrderKey[]
  limit?: Bound
  offset?: Bound
  setOps: SetOp[]
  window: boolean // a window function appears in the projection
  tables: string[] // every real table name this shape reads (routing dependency set)
}

/** The sound fallback: the shape could not be read precisely, so anything touching
 *  one of `tables` invalidates. Produced on shape-assertion failure or untrackable reads. */
export type CoarseShape = {
  kind: 'coarse'
  tables: string[]
  reason: string
}

export type QueryShape = SelectShape | CoarseShape

// ── Row view (evaluation input) ─────────────────────────────────────

/** One cell of a row: `present: false` is a column that was never captured
 *  (MISSING → widens to unknown); `present: true, value: null` is a real SQL NULL. */
export type Cell = { present: false } | { present: true; value: unknown }

export interface RowView {
  get(column: string): Cell
}
