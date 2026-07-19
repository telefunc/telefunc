import { eq, placeholder } from 'drizzle-orm'
import { boolean, integer, pgTable, QueryBuilder, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { integer as sInteger, sqliteTable, text as sText } from 'drizzle-orm/sqlite-core'
import { drizzle as sqliteDrizzle } from 'drizzle-orm/node-sqlite'
import type { MySqlAsyncSelectBuilder } from 'drizzle-orm/mysql-core/async/select'
import { it } from 'vitest'
import type { Live } from 'telefunc'
import { reactiveDrizzle } from './reactiveDrizzle.js'

// A COMPILE-TIME contract, checked by the package's own `tsc` (tsconfig.spec.json). It exercises the
// SHIPPING `reactiveDrizzle` return type — not a copy of the HKTs — against REAL Drizzle rc.4 db types,
// on both supported dialects (pg + sqlite). The proof IS the typecheck: every function below
// either type-checks or is pinned with `@ts-expect-error`, and none is ever executed. If a pin stops erroring — someone widened a row type to `any`, or leaked
// `.live()` onto the base db — the directive becomes unused and tsc fails here (TS2578: fail-closed).
//
// Positives assign a CONCRETE field type (`const done: boolean = …`): were a row type silently `any`, the
// negative pins below would go unused and fail closed. So the two directions pin each other.

const pgTodos = pgTable('todos', {
  id: text('id').primaryKey(),
  title: text('title').notNull(),
  done: boolean('done').notNull(),
  rank: integer('rank').notNull(),
})
const pgComments = pgTable('comments', {
  id: text('id').primaryKey(),
  todoId: text('todo_id').notNull(),
  body: text('body').notNull(),
})
const sqliteTodos = sqliteTable('todos', {
  id: sText('id').primaryKey(),
  done: sInteger('done', { mode: 'boolean' }).notNull(),
  rank: sInteger('rank').notNull(),
})

declare const pgBase: ReturnType<typeof pgDrizzle>
declare const sqliteBase: ReturnType<typeof sqliteDrizzle>

// ── PostgreSQL: whole-row + projection resolve; `await` (no `.live()`) stays plain rows ──
async function pgPaths() {
  const db = reactiveDrizzle(pgBase)
  const q = db.select().from(pgTodos).where(eq(pgTodos.done, true)).orderBy(pgTodos.rank)

  const plain = await q // no `.live()` → plain rows
  const plainDone: boolean = plain[0]!.done
  // @ts-expect-error forgetting `.live()` yields a plain row array, not a Live
  const notLive: Live<typeof plain> = plain

  const result = await q.live() // Live<Todo[]>
  const done: boolean = result.data[0]!.done
  const title: string = result.data[0]!.title
  // @ts-expect-error teeth: `done` is boolean, not number
  const wrong: number = result.data[0]!.done

  const proj = await db.select({ id: pgTodos.id, done: pgTodos.done }).from(pgTodos).live()
  const id: string = proj.data[0]!.id
  const projDone: boolean = proj.data[0]!.done
  // @ts-expect-error teeth: `title` was not projected
  proj.data[0]!.title

  void [plainDone, notLive, done, title, wrong, id, projDone]
}

// ── SQLite: the extra `TRunResult` HKT slot must not disturb row inference ──
async function sqlitePaths() {
  const db = reactiveDrizzle(sqliteBase)
  const whole = await db.select().from(sqliteTodos).live()
  const done: boolean = whole.data[0]!.done // sqlite `{ mode: 'boolean' }` → boolean
  const rank: number = whole.data[0]!.rank
  // @ts-expect-error teeth: `done` is boolean, not number
  const wrong: number = whole.data[0]!.done

  const proj = await db.select({ id: sqliteTodos.id }).from(sqliteTodos).live()
  const id: string = proj.data[0]!.id
  // @ts-expect-error teeth: `done` was not projected
  proj.data[0]!.done

  void [done, rank, wrong, id]
}

// ── Teeth: MySQL is REJECTED, not merely absent — the type says so and `reactiveDrizzle` throws to match ──
// The constraint reads the db's own async select BUILDER, so the fixture is that builder's real type from
// drizzle's driver-free `mysql-core` (the mysql2 driver is not a dependency of this package). The RUNTIME
// half of the same contract — construction throws with a message naming the supported set — is pinned in
// reactiveDrizzle.spec.ts against a real `MySqlDialect`; a type-only rejection would leave a JS caller with
// a silently non-reactive db.
declare const mysqlBase: { select: (...args: any[]) => MySqlAsyncSelectBuilder<any> }
function mysqlRejected() {
  // @ts-expect-error MySQL has no RETURNING, so no write on it could be captured row by row — unsupported
  reactiveDrizzle(mysqlBase)
}

// ── Driver terminals survive: everything EXCEPT `.live()` is the ordinary async builder ──
async function pgDriverTerminals() {
  const db = reactiveDrizzle(pgBase)
  const q = db
    .select()
    .from(pgTodos)
    .where(eq(pgTodos.rank, placeholder('rank')))
  const prepared = q.prepare('by_rank') // async prepare survives
  await prepared.execute({ rank: 1 })
  const rows = await q.execute({ rank: 2 }) // execute keeps its placeholder-values parameter
  const one: boolean = rows[0]!.done
  void one
}
function sqliteDriverTerminals() {
  const base = sqliteBase.select().from(sqliteTodos)
  const reactive = reactiveDrizzle(sqliteBase).select().from(sqliteTodos)
  // node-sqlite is SYNC: run/all/get/values return VALUES, not Promises. Assigning the reactive terminal's
  // return type into the base builder's proves they match EXACTLY — a Promise mismatch (the old bug) fails
  // here. (Name-existence would have passed even with the wrong sync/async mode.)
  const run: ReturnType<typeof base.run> = null as unknown as ReturnType<typeof reactive.run>
  const all: ReturnType<typeof base.all> = null as unknown as ReturnType<typeof reactive.all>
  const get: ReturnType<typeof base.get> = null as unknown as ReturnType<typeof reactive.get>
  const values: ReturnType<typeof base.values> = null as unknown as ReturnType<typeof reactive.values>
  const execute: ReturnType<typeof base.execute> = null as unknown as ReturnType<typeof reactive.execute>
  const prepared: ReturnType<typeof base.prepare> = null as unknown as ReturnType<typeof reactive.prepare>
  void [run, all, get, values, execute, prepared]
}
// ── Left-join nullability + `$dynamic()` survive the reactive chain to `.live()` ──
async function joinNullabilityAndDynamic() {
  const db = reactiveDrizzle(pgBase)
  const joined = await db
    .select({ todoId: pgTodos.id, commentBody: pgComments.body })
    .from(pgTodos)
    .leftJoin(pgComments, eq(pgComments.todoId, pgTodos.id))
    .$dynamic()
    .live()
  const todoId: string = joined.data[0]!.todoId
  const commentBody: string | null = joined.data[0]!.commentBody
  // @ts-expect-error teeth: a left-joined column stays nullable
  const unsound: string = joined.data[0]!.commentBody
  void [todoId, commentBody, unsound]
}

// ── A transaction is a reactive db too; its row inference must not widen ──
async function transactionTyping() {
  await pgBase.transaction(async (tx) => {
    const result = await reactiveDrizzle(tx).select().from(pgTodos).live()
    const title: string = result.data[0]!.title
    // @ts-expect-error teeth: transaction rows keep their exact type
    const wrong: number = result.data[0]!.title
    void [title, wrong]
  })
}

// ── Teeth: a bare QueryBuilder has no session to execute against — reactiveDrizzle rejects it ──
function standaloneQueryBuilderRejected() {
  // @ts-expect-error a QueryBuilder types a query but cannot run one; a live read has nothing to hydrate
  reactiveDrizzle(new QueryBuilder())
}

// ── Teeth: CTE-prefixed reads go through Drizzle's ordinary with() facade — intentionally NOT reactive ──
function withCteIsNotReactive() {
  const db = reactiveDrizzle(pgBase)
  const sq = db.$with('sq').as(db.select().from(pgTodos))
  // @ts-expect-error `db.with(...)` is the ordinary facade; its select is not wrapped, so `.live()` is absent
  db.with(sq).select().from(sq).live()
}

// ── Teeth: `.live()` exists ONLY on the reactive db's chain, never on the base Drizzle builder ──
function baseDoesNotLeak() {
  // @ts-expect-error `.live()` is synthesized by reactiveDrizzle; the base db's builder has no such method
  pgBase.select().from(pgTodos).live()
}

// ── Teeth: forgetting the terminal cannot satisfy a contract that promises a Live ──
async function forgettingIsVisible(): Promise<Live<(typeof pgTodos.$inferSelect)[]>> {
  const db = reactiveDrizzle(pgBase)
  // @ts-expect-error a chain without `.live()` resolves to rows, not Live — the return type rejects it
  return db.select().from(pgTodos)
}

void [
  pgPaths,
  sqlitePaths,
  mysqlRejected,
  pgDriverTerminals,
  sqliteDriverTerminals,
  joinNullabilityAndDynamic,
  transactionTyping,
  standaloneQueryBuilderRejected,
  withCteIsNotReactive,
  baseDoesNotLeak,
  forgettingIsVisible,
]

// The proof IS the typecheck above; this keeps vitest from reporting an empty suite.
it('reactiveDrizzle terminal `.live()` HKT contract holds at compile time', () => {})
