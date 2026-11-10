// Type checks of the public API, run by `pnpm run test:types`: each `.live()` is typed by its ORM's own rows, and
// forgetting it is a compile error where the client consumes the telefunction.

import type { Collection } from '@prisma/orm-postgres/orm-client'
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
  const plain = await db.select().from(todos).where(eq(todos.done, false))
  same<typeof plain, Todo[]>(true)
  await db.insert(todos).values({ userId: 1, text: 'a', done: false })
}

// ── Kysely ──
type DB = { todos: Todo }
const kdb = liveKysely(new Kysely<DB>({ dialect: new PostgresDialect({ pool }) }), pool)
async function kyselyReads() {
  const rows = await kdb.selectFrom('todos').select(['id', 'text']).where('userId', '=', 1).orderBy('id').live()
  same<typeof rows, Live<{ id: number; text: string }[]>>(true)
}

// ── Prisma 8 ──
declare const prisma: {
  orm: { public: { Todo: Collection<any, 'Todo', Todo> } }
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

void [drizzleReads, kyselyReads, prismaReads, stop]
