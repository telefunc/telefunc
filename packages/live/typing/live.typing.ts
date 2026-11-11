// Type checks of the public API, run by `pnpm run test:types`: each `.live()` is typed by its ORM's own rows, and
// forgetting it is a compile error where the client consumes the telefunction.

import type { Collection } from '@prisma/orm-postgres/orm-client'
import postgres from '@prisma/orm-postgres/runtime'
import { contractFromJson } from '@prisma/orm-postgres/static'
import { eq } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/node-postgres'
import { boolean, integer, pgTable, serial, text } from 'drizzle-orm/pg-core'
import { Kysely, PostgresDialect } from 'kysely'
import pg from 'pg'
import { derived, type Live } from '../src/index.js'
import { watch } from '../src/client.js'
import { liveDrizzle } from '../src/drizzle.js'
import { liveKysely } from '../src/kysely.js'
import { livePrisma } from '../src/prisma.js'
import { live } from '../src/tanstack-query.js'

type Todo = { id: number; userId: number; text: string; done: boolean }
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false
const same = <A, B>(_: Same<A, B>) => {}

const pool = new pg.Pool()
const todos = pgTable('todos', {
  id: serial('id').primaryKey(),
  userId: integer('user_id').notNull(),
  text: text('text').notNull(),
  done: boolean('done').notNull(),
})
const users = pgTable('users', { id: serial('id').primaryKey(), name: text('name').notNull() })

// ── Drizzle: `.live()` stays on every step, with Drizzle's own rows ──
const db = liveDrizzle(drizzle({ client: pool }))
async function drizzleReads() {
  const all = await db.select().from(todos).live()
  same<typeof all, Live<Todo[]>>(true)
  const page = await db
    .select({ id: todos.id, text: todos.text })
    .from(todos)
    .where(eq(todos.userId, 1))
    .orderBy(todos.id)
    .limit(10)
    .live()
  same<typeof page, Live<{ id: number; text: string }[]>>(true)
  const joined = await db.select().from(todos).leftJoin(users, eq(users.id, todos.userId)).live()
  same<typeof joined, Live<{ todos: Todo; users: { id: number; name: string } | null }[]>>(true)
  const distinct = await db.selectDistinct().from(todos).live()
  same<typeof distinct, Live<Todo[]>>(true)
  const distinctOn = await db.selectDistinctOn([todos.userId], { userId: todos.userId }).from(todos).live()
  same<typeof distinctOn, Live<{ userId: number }[]>>(true)
  const open = db.$with('open').as(db.select().from(todos).where(eq(todos.done, false)))
  const fromCte = await db.with(open).select().from(open).live()
  same<typeof fromCte, Live<Todo[]>>(true)
  const fromCteDistinct = await db.with(open).selectDistinct({ id: open.id }).from(open).live()
  same<typeof fromCteDistinct, Live<{ id: number }[]>>(true)
  const inTransaction = await db.transaction((tx) => tx.select({ id: todos.id }).from(todos).live())
  same<typeof inTransaction, Live<{ id: number }[]>>(true)
  const inSavepoint = await db.transaction((tx) => tx.transaction((sp) => sp.selectDistinct().from(todos).live()))
  same<typeof inSavepoint, Live<Todo[]>>(true)
  const plain = await db.select().from(todos).where(eq(todos.done, false))
  same<typeof plain, Todo[]>(true)
  await db.insert(todos).values({ userId: 1, text: 'a', done: false })
}

// ── Kysely ──
type DB = { todos: Todo }
const unwrappedKdb = new Kysely<DB>({ dialect: new PostgresDialect({ pool }) })
const kdb = liveKysely(unwrappedKdb, pool)
async function kyselyReads() {
  const rows = await kdb.selectFrom('todos').select(['id', 'text']).where('userId', '=', 1).orderBy('id').live()
  same<typeof rows, Live<{ id: number; text: string }[]>>(true)
  // @ts-expect-error `.live()` on a Kysely that `liveKysely()` didn't wrap.
  await unwrappedKdb.selectFrom('todos').selectAll().live()
}

// ── Prisma 8 ──
type PrismaModels = { orm: { public: { Todo: Collection<any, 'Todo', Todo> } } }
declare const prisma: PrismaModels & {
  transaction<R>(run: (tx: PrismaModels) => PromiseLike<R>): Promise<R>
  contract: { roots: Record<string, { model: string; namespace: string }> }
}
const pdb = livePrisma(prisma, pool)
async function prismaReads() {
  const filtered = await pdb.orm.public.Todo.where({ userId: 1 } as never).live()
  same<typeof filtered, Live<Todo[]>>(true)
  const whole = await pdb.orm.public.Todo.limit(5).live()
  same<typeof whole, Live<Todo[]>>(true)
  const picked = await pdb.orm.public.Todo.select('id', 'text')
    .orderBy((t: any) => t.id.asc())
    .live()
  // This stand-in client has no contract types, so a selected column's type is unknown.
  same<typeof picked, Live<{ id: unknown; text: unknown }[]>>(true)
}
// Every step that returns a collection keeps `.live()`, with the rows Prisma gives that step.
async function prismaSteps() {
  const { Todo } = pdb.orm.public
  const ordered = await Todo.orderBy((t: any) => t.id.asc()).live()
  same<typeof ordered, Live<Todo[]>>(true)
  const paged = await Todo.orderBy((t: any) => t.id.asc())
    .offset(10)
    .limit(5)
    .live()
  same<typeof paged, Live<Todo[]>>(true)
  const distinct = await Todo.distinct('text').live()
  same<typeof distinct, Live<Todo[]>>(true)
  const after = await Todo.orderBy((t: any) => t.id.asc())
    .cursor({ id: 1 } as never)
    .live()
  same<typeof after, Live<Todo[]>>(true)
  // The stand-in contract has no relations, so `include()` adds none here.
  const included = await Todo.include('tags' as never).live()
  same<typeof included, Live<Todo[]>>(true)
  const selected = await Todo.select('id').live()
  same<typeof selected, Live<{ id: unknown }[]>>(true)
  const picked = await Todo.where({ userId: 1 } as never)
    .select('id', 'text')
    .limit(5)
    .live()
  same<typeof picked, Live<{ id: unknown; text: unknown }[]>>(true)
  const inTransaction = await pdb.transaction((tx) => tx.orm.public.Todo.where({ userId: 1 } as never).live())
  same<typeof inTransaction, Live<Todo[]>>(true)
}
const unwrappedPrisma = postgres({ contract: contractFromJson({}), pg: pool })
const wrappedPrisma = livePrisma(unwrappedPrisma, pool)
async function prismaScope() {
  await wrappedPrisma.orm.public!.Todo!.where({}).live()
  // @ts-expect-error `.live()` on a client that `livePrisma()` didn't wrap.
  await unwrappedPrisma.orm.public!.Todo!.where({}).live()
}

// ── Composing on the server ──
async function onGetSummary(userId: number) {
  const list = await db.select().from(todos).where(eq(todos.userId, userId)).live()
  return derived(() => ({ open: list.data.filter((t) => !t.done).length }))
}
async function onGetTodos(userId: number) {
  return db.select().from(todos).where(eq(todos.userId, userId)).live()
}
async function onGetTodosPlain(userId: number) {
  return db.select().from(todos).where(eq(todos.userId, userId))
}

// ── Client ──
const queryFn = live(onGetTodos, 1)
same<Awaited<ReturnType<typeof queryFn>>, Todo[]>(true)
const summaryFn = live(onGetSummary, 1)
same<Awaited<ReturnType<typeof summaryFn>>, { open: number }>(true)
// @ts-expect-error Forgetting `.live()`: the telefunction returns rows, not a `Live`.
live(onGetTodosPlain, 1)
// @ts-expect-error The arguments are the telefunction's.
live(onGetTodos, 'one')
const stop: () => void = watch(onGetTodos, [1], (rows) => same<typeof rows, Todo[]>(true))
// @ts-expect-error Forgetting `.live()`.
watch(onGetTodosPlain, [1], () => {})

void [drizzleReads, kyselyReads, prismaReads, prismaSteps, prismaScope, stop]
