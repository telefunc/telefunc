export { createDrizzle }

import { and, asc, between, count, desc, eq, exists, inArray, like, not, relations, sql, sum } from 'drizzle-orm'
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres'
import { boolean, integer, numeric, pgTable, serial, text, timestamp } from 'drizzle-orm/pg-core'
import { drizzle as drizzlePglite } from 'drizzle-orm/pglite'
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
})
const tags = pgTable('tags', {
  id: serial('id').primaryKey(),
  todoId: integer('todo_id').notNull(),
  name: text('name').notNull(),
})
const usersRelations = relations(users, ({ many }) => ({ todos: many(todos) }))
const todosRelations = relations(todos, ({ one }) => ({
  user: one(users, { fields: [todos.userId], references: [users.id] }),
}))
const schema = { users, todos, tags, usersRelations, todosRelations }

function createDrizzle(client: unknown, kind: 'pg' | 'pglite'): OrmDriver {
  const db = (
    kind === 'pg' ? drizzlePg({ client: client as never, schema }) : drizzlePglite({ client: client as never, schema })
  ) as any
  return {
    reads: {
      byUser: ({ user }) => db.select().from(todos).where(eq(todos.userId, user)).orderBy(todos.id),
      recent: ({ user }) =>
        db.select().from(todos).where(eq(todos.userId, user)).orderBy(desc(todos.updatedAt), desc(todos.id)).limit(3),
      priorityTies: ({ user }) =>
        db
          .select({ id: todos.id, priority: todos.priority })
          .from(todos)
          .where(eq(todos.userId, user))
          .orderBy(desc(todos.priority))
          .limit(2),
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
      insertUser: ({ text, value }) => db.insert(users).values({ name: text, team: (value % 3) + 1 }),
    },
    async close() {},
  }
}
