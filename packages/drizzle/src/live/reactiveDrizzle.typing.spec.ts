import { eq } from 'drizzle-orm'
import { boolean, integer, pgTable, text } from 'drizzle-orm/pg-core'
import { drizzle as pgDrizzle } from 'drizzle-orm/pglite'
import { integer as sInteger, sqliteTable, text as sText } from 'drizzle-orm/sqlite-core'
import { drizzle as sqliteDrizzle } from 'drizzle-orm/node-sqlite'
import { int as myInt, mysqlTable, varchar } from 'drizzle-orm/mysql-core'
import { drizzle as mysqlDrizzle } from 'drizzle-orm/mysql2'
import { it } from 'vitest'
import type { Live } from 'telefunc'
import { reactiveDrizzle } from './reactiveDrizzle.js'

// A COMPILE-TIME contract, checked by the package's own `tsc` (tsconfig.spec.json). It exercises the
// SHIPPING `reactiveDrizzle` return type — not a copy of the HKTs — against REAL Drizzle rc.4 db types,
// on all three targeted dialects (pg + sqlite + mysql). The proof IS the typecheck: every function below
// either type-checks or is pinned with `@ts-expect-error`, and none is ever executed (so `reactiveDrizzle`
// never runs `acquireCarrier`). If a pin stops erroring — someone widened a row type to `any`, or leaked
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
const sqliteTodos = sqliteTable('todos', {
  id: sText('id').primaryKey(),
  done: sInteger('done', { mode: 'boolean' }).notNull(),
  rank: sInteger('rank').notNull(),
})
const mysqlTodos = mysqlTable('todos', {
  id: varchar('id', { length: 32 }).primaryKey(),
  done: myInt('done').notNull(),
  rank: myInt('rank').notNull(),
})

declare const pgBase: ReturnType<typeof pgDrizzle>
declare const sqliteBase: ReturnType<typeof sqliteDrizzle>
declare const mysqlBase: ReturnType<typeof mysqlDrizzle>

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

// ── MySQL ──
async function mysqlPaths() {
  const db = reactiveDrizzle(mysqlBase)
  const whole = await db.select().from(mysqlTodos).live()
  const done: number = whole.data[0]!.done // mysql `int` → number
  // @ts-expect-error teeth: `done` is number, not boolean
  const wrong: boolean = whole.data[0]!.done

  const proj = await db.select({ id: mysqlTodos.id }).from(mysqlTodos).live()
  const id: string = proj.data[0]!.id

  void [done, wrong, id]
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

void [pgPaths, sqlitePaths, mysqlPaths, baseDoesNotLeak, forgettingIsVisible]

// The proof IS the typecheck above; this keeps vitest from reporting an empty suite.
it('reactiveDrizzle terminal `.live()` HKT contract holds at compile time', () => {})
