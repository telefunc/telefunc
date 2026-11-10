export { createDrizzle }

import {
  and,
  asc,
  between,
  count,
  countDistinct,
  desc,
  eq,
  exists,
  gt,
  inArray,
  like,
  max,
  not,
  relations,
  sql,
  sum,
} from 'drizzle-orm'
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres'
import {
  boolean,
  char,
  except,
  integer,
  numeric,
  pgTable,
  serial,
  text,
  timestamp,
  union,
  uuid,
} from 'drizzle-orm/pg-core'
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite'
import { drizzle as drizzlePostgresJs } from 'drizzle-orm/postgres-js'
import { liveDrizzle } from '@telefunc/live/drizzle'
import type { BroadcastAdapter } from 'telefunc'
import { arrayOf, CODES, COLUMN_TYPES, REFS, type Spelling } from '../spellings.ts'
import { EVENT_TAG, eventQuery } from './eventQuery.ts'
import type { OrmDriver } from './types.ts'

const users = pgTable('users', {
  id: serial('id').primaryKey(),
  name: text('name').notNull(),
  team: integer('team').notNull(),
})
const todos = pgTable('todos', {
  id: serial('id').primaryKey(),
  userId: integer('user_id').notNull(),
  text: text('text').notNull(),
  done: boolean('done').notNull(),
  priority: integer('priority').notNull(),
  amount: numeric('amount').notNull(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull(),
  ref: uuid('ref').notNull(),
  code: char('code', { length: 4 }).notNull(),
})
const tags = pgTable('tags', {
  id: serial('id').primaryKey(),
  todoId: integer('todo_id').notNull(),
  name: text('name').notNull(),
})
const deletedTodos = pgTable('deleted_todos', {
  id: serial('id').primaryKey(),
  userId: integer('user_id').notNull(),
  todoId: integer('todo_id').notNull(),
})
const usersRelations = relations(users, ({ many }) => ({ todos: many(todos) }))
const todosRelations = relations(todos, ({ one }) => ({
  user: one(users, { fields: [todos.userId], references: [users.id] }),
}))
const schema = { users, todos, tags, usersRelations, todosRelations }

function createDrizzle(
  client: unknown,
  kind: 'pg' | 'pglite' | 'postgres-js',
  transport?: BroadcastAdapter,
): OrmDriver {
  const connect = { pg: drizzlePg, pglite: drizzlePglite, 'postgres-js': drizzlePostgresJs }[kind]
  const db = liveDrizzle(connect({ client: client as never, schema }) as never, { transport }) as any
  return {
    reads: {
      byUser: ({ user }) => db.select().from(todos).where(eq(todos.userId, user)).orderBy(todos.id),
      deletedTodos: ({ user }) =>
        db
          .select({ todoId: deletedTodos.todoId })
          .from(deletedTodos)
          .where(eq(deletedTodos.userId, user))
          .orderBy(deletedTodos.todoId),
      recent: ({ user }) =>
        db.select().from(todos).where(eq(todos.userId, user)).orderBy(desc(todos.updatedAt), desc(todos.id)).limit(3),
      priorityTies: ({ user }) =>
        db
          .select({ id: todos.id, priority: todos.priority })
          .from(todos)
          .where(eq(todos.userId, user))
          .orderBy(desc(todos.priority))
          .limit(2),
      firstTexts: () =>
        db.select({ id: todos.id, text: todos.text }).from(todos).orderBy(todos.text, todos.id).limit(3),
      openPage: () =>
        db
          .select({ id: todos.id, text: todos.text })
          .from(todos)
          .where(eq(todos.done, false))
          .orderBy(todos.id)
          .limit(5)
          .offset(3),
      teamJoin: ({ team }) =>
        db
          .select({ id: todos.id, text: todos.text, name: users.name })
          .from(todos)
          .innerJoin(users, eq(users.id, todos.userId))
          .where(eq(users.team, team))
          .orderBy(todos.id),
      userWithTodos: ({ user }) =>
        db.query.users.findMany({ where: eq(users.id, user), with: { todos: { orderBy: asc(todos.id) } } }),
      countByDone: ({ user }) =>
        db
          .select({ done: todos.done, n: count() })
          .from(todos)
          .where(eq(todos.userId, user))
          .groupBy(todos.done)
          .orderBy(todos.done),
      likeIn: ({ user, user2 }) =>
        db
          .select()
          .from(todos)
          .where(and(inArray(todos.userId, [user, user2]), like(todos.text, 'a%')))
          .orderBy(todos.id),
      sumAmount: ({ user }) =>
        db
          .select({ total: sum(todos.amount) })
          .from(todos)
          .where(eq(todos.userId, user)),
      busyUsers: () =>
        db
          .select({ userId: todos.userId, n: count() })
          .from(todos)
          .groupBy(todos.userId)
          .having(gt(count(), 7))
          .orderBy(todos.userId),
      topPriority: () =>
        db
          .select({ userId: todos.userId, top: max(todos.priority) })
          .from(todos)
          .groupBy(todos.userId)
          .orderBy(todos.userId),
      priorityKinds: ({ user }) =>
        db
          .select({ kinds: countDistinct(todos.priority) })
          .from(todos)
          .where(eq(todos.userId, user)),
      todosOrTagged: ({ user }) =>
        union(
          db.select({ id: todos.id }).from(todos).where(eq(todos.userId, user)),
          db.select({ id: tags.todoId }).from(tags).where(eq(tags.name, 'tag 1')),
        ).orderBy(sql`1`),
      untagged: ({ user }) =>
        except(
          db.select({ id: todos.id }).from(todos).where(eq(todos.userId, user)),
          db.select({ id: tags.todoId }).from(tags),
        ).orderBy(sql`1`),
      distinctUsers: () =>
        db.selectDistinct({ userId: todos.userId }).from(todos).where(eq(todos.done, true)).orderBy(todos.userId),
      usersWithOpen: () =>
        db
          .select({ id: users.id })
          .from(users)
          .where(
            exists(
              db
                .select({ one: sql`1` })
                .from(todos)
                .where(and(eq(todos.userId, users.id), not(todos.done))),
            ),
          )
          .orderBy(users.id),
      tagsOfUser: ({ user }) =>
        db
          .select({ id: tags.id, name: tags.name })
          .from(tags)
          .innerJoin(todos, eq(todos.id, tags.todoId))
          .where(eq(todos.userId, user))
          .orderBy(tags.id),
      priorityRange: ({ low, high }) =>
        db
          .select({ id: todos.id })
          .from(todos)
          .where(between(todos.priority, low, high))
          .orderBy(todos.id),
      leftJoinTags: ({ user }) =>
        db
          .select({ id: todos.id, tag: tags.name })
          .from(todos)
          .leftJoin(tags, eq(tags.todoId, todos.id))
          .where(eq(todos.userId, user))
          .orderBy(todos.id, tags.id),
      tagLog: ({ low, high }) =>
        db
          .execute(sql`select todo_id from tag_log where todo_id between ${low * 8} and ${high * 8 + 7} order by id`)
          .then(rowsOf),
      noteLog: () =>
        db.execute(sql`select note, count(*)::int as n from note_log group by note order by note`).then(rowsOf),
      publicNotes: () => db.execute(sql`select id, n from public.notes order by id`).then(rowsOf),
      tenantNotes: () => db.execute(sql`select id, n from tenant.notes order by id`).then(rowsOf),
      eventLog: () =>
        db.execute(sql`select kind, count(*)::int as n from event_log group by kind order by kind`).then(rowsOf),
      counters: () => db.execute(sql`select id, n from counters order by id`).then(rowsOf),
      settled: () => db.execute(sql`select n from settled`).then(rowsOf),
      spelled: ({ spellings: [a] }) =>
        db
          .select({ id: todos.id })
          .from(todos)
          .where(sql`${column(a!)} = ${operand(a!)}`)
          .orderBy(todos.id),
      spelledIn: ({ spellings: [a, b] }) =>
        db
          .select({ id: todos.id })
          .from(todos)
          .where(sql`${column(a!)} in (${operand(a!)}, ${operand(b!)})`)
          .orderBy(todos.id),
      spelledAny: ({ spellings }) =>
        db
          .select({ id: todos.id })
          .from(todos)
          .where(sql`${column(spellings[0]!)} = any(${arrayOf(spellings)})`)
          .orderBy(todos.id),
      // Lists cast to an array of the column's type, `NULL` lists, and Boolean tests that tell `NULL` from `FALSE`.
      listCast: ({ spellings, list }) =>
        db
          .select({ id: todos.id })
          .from(todos)
          .where(sql`${column(spellings[0]!)} = any(${list}${sql.raw(arrayType(spellings[0]!))})`)
          .orderBy(todos.id),
      listIsNotFalse: ({ spellings, list }) =>
        db
          .select({ id: todos.id })
          .from(todos)
          .where(sql`(${column(spellings[0]!)} = any(${list})) is not false`)
          .orderBy(todos.id),
      listAllIsNotTrue: ({ spellings, list }) =>
        db
          .select({ id: todos.id })
          .from(todos)
          .where(sql`(${column(spellings[0]!)} = all(${list}${sql.raw(arrayType(spellings[0]!))})) is not true`)
          .orderBy(todos.id),
    },
    writes: {
      insertTodo: ({ user, value, text, date }) =>
        db.insert(todos).values({
          userId: user,
          text,
          done: value % 2 === 0,
          priority: value % 5,
          amount: String(value),
          updatedAt: date,
        }),
      updateText: ({ todo, text }) => db.update(todos).set({ text }).where(eq(todos.id, todo)),
      toggleDone: ({ todo }) =>
        db
          .update(todos)
          .set({ done: sql`not ${todos.done}` })
          .where(eq(todos.id, todo)),
      updatePriority: ({ todo, value, date }) =>
        db
          .update(todos)
          .set({ priority: value % 5, updatedAt: date })
          .where(eq(todos.id, todo)),
      moveTodo: ({ todo, otherUser }) =>
        db.update(todos).set({ userId: otherUser }).where(eq(todos.id, todo)).returning({ id: todos.id }),
      deleteTodo: ({ todo }) => db.delete(todos).where(eq(todos.id, todo)),
      insertTag: ({ todo, text }) => db.insert(tags).values({ todoId: todo, name: text }),
      deleteTag: ({ tag }) => db.delete(tags).where(eq(tags.id, tag)),
      renameUser: ({ user, text, value }) =>
        db
          .update(users)
          .set({ name: text, team: (value % 3) + 1 })
          .where(eq(users.id, user)),
      upsertTodo: ({ todo, user, text }) =>
        db
          .insert(todos)
          .values({ id: todo, userId: user, text, updatedAt: new Date() })
          .onConflictDoUpdate({ target: todos.id, set: { text } }),
      rawBump: ({ user }) => db.execute(sql`update todos set priority = priority + 1 where user_id = ${user}`),
      txCommit: ({ todo, user, text }) =>
        db.transaction(async (tx: any) => {
          await tx.insert(todos).values({ userId: user, text, updatedAt: new Date() })
          await tx.update(todos).set({ done: true }).where(eq(todos.id, todo))
        }),
      txRollback: ({ user, text }) =>
        db
          .transaction(async (tx: any) => {
            await tx.insert(todos).values({ userId: user, text, updatedAt: new Date() })
            tx.rollback()
          })
          .catch(() => {}),
      savepointRollback: ({ todo, user, text }) =>
        db.transaction(async (tx: any) => {
          await tx.update(todos).set({ text }).where(eq(todos.id, todo))
          await tx
            .transaction(async (inner: any) => {
              await inner.insert(todos).values({ userId: user, text: `${text}!`, updatedAt: new Date() })
              inner.rollback()
            })
            .catch(() => {})
        }),
      insertSelect: ({ user }) =>
        db.execute(
          sql`insert into tags (todo_id, name) select id, 'copied' from todos where user_id = ${user} order by id limit 1`,
        ),
      deleteUser: ({ user }) => db.delete(users).where(eq(users.id, user)),
      // Two statements in one text: sent as written.
      deleteUserRaw: ({ user }) => db.execute(sql.raw(`delete from users where id = ${Number(user)}; select 1`)),
      insertUser: ({ text, value }) => db.insert(users).values({ name: text, team: (value % 3) + 1 }),
      setPath: ({ value }) => db.execute(sql.raw(`set search_path to ${schemaOf(value)}, public`)),
      bumpNote: ({ value }) => db.execute(sql`update notes set n = n + 1 where id = ${1 + (value % 5)}`),
      addEvent: ({ text }) => db.execute(sql`insert into events (kind) values (${text.slice(0, 1)})`),
      eventInTx: ({ todo, text, value }) =>
        db.transaction(async (tx: any) => {
          await tx.execute(sql`insert into events (kind) values (${text.slice(0, 1)})`)
          await tx
            .update(todos)
            .set({ priority: value % 5 })
            .where(eq(todos.id, todo))
        }),
      resetEvents: () => db.execute(sql`insert into resets default values`),
      // One text: the update names the table by the search_path the statement before it sets.
      notesPath: ({ value }) =>
        db.execute(
          sql.raw(
            `set search_path to ${schemaOf(value)}, public; update notes set n = n + 1 where id = ${1 + (value % 5)}`,
          ),
        ),
      ...(kind === 'pg' && {
        eventTag: ({ todo, text }) => eventQuery(client as never, EVENT_TAG, [todo, text]),
      }),
      recode: ({ todo, value }) =>
        db
          .update(todos)
          .set({ code: CODES[value % CODES.length]!, ref: REFS[(value >> 2) % REFS.length]! })
          .where(eq(todos.id, todo)),
    },
    liveRead: () => db.select().from(todos).where(eq(todos.userId, 1)).live(),
    async close() {},
  }
}

const column = ({ column }: Spelling) => sql.raw(`"todos"."${column}"`)

/** A spelling as a parameter or inline, followed by its cast. */
const operand = ({ value, cast, inline }: Spelling) =>
  inline ? sql.raw(`'${value.replace(/'/g, "''")}'${cast}`) : sql`${value}${sql.raw(cast)}`

const schemaOf = (value: number) => (value % 3 === 0 ? 'public' : 'tenant')

/** `db.execute()`'s rows: the result itself on postgres.js, its `rows` elsewhere. */
const rowsOf = (result: any) => result.rows ?? [...result]

const arrayType = ({ column }: Spelling) => `::${COLUMN_TYPES[column]}[]`
