export { createDrizzleSqlite }

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
  lt,
  max,
  not,
  relations,
  sql,
  sum,
} from 'drizzle-orm'
import { except, integer, real, sqliteTable, text, union } from 'drizzle-orm/sqlite-core'
import { liveDrizzle } from '@telefunc/live/drizzle'
import type { OrmDriver } from './types.ts'

const users = sqliteTable('users', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
  team: integer('team').notNull(),
  nick: text('nick'),
})
const todos = sqliteTable('todos', {
  id: integer('id').primaryKey(),
  userId: integer('user_id').notNull(),
  text: text('text').notNull(),
  done: integer('done', { mode: 'boolean' }).notNull(),
  priority: integer('priority').notNull(),
  amount: real('amount').notNull(),
  updatedAt: text('updated_at').notNull(),
  due: integer('due'),
})
const tags = sqliteTable('tags', {
  id: integer('id').primaryKey(),
  todoId: integer('todo_id').notNull(),
  name: text('name').notNull(),
})
const deletedTodos = sqliteTable('deleted_todos', {
  id: integer('id').primaryKey(),
  userId: integer('user_id').notNull(),
  todoId: integer('todo_id').notNull(),
})
const settings = sqliteTable('settings', {
  id: integer('id').primaryKey(),
  name: text('name').notNull(),
  value: integer('value').notNull(),
})
const usersRelations = relations(users, ({ many }) => ({ todos: many(todos) }))
const todosRelations = relations(todos, ({ one }) => ({
  user: one(users, { fields: [todos.userId], references: [users.id] }),
}))
const schema = { users, todos, tags, usersRelations, todosRelations }

const NICKS = ['ann', 'ANN', 'Bob', 'bob', 'cy', 'Dee']

/** `client` is a better-sqlite3 or bun:sqlite database or a D1 binding, `drizzle` the Drizzle driver for it. */
function createDrizzleSqlite(
  client: { exec(text: string): unknown },
  drizzle: (config: { client: any; schema: typeof schema }) => object,
): OrmDriver {
  const db = liveDrizzle(drizzle({ client, schema }) as any) as any
  const driver: OrmDriver = {
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
      textsBelow: () =>
        db.select({ id: todos.id, text: todos.text }).from(todos).where(lt(todos.text, 'B')).orderBy(todos.id),
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
      // SQLite's `LIKE` ignores ASCII case: 'a%' matches 'A1' too.
      likeIn: ({ user, user2 }) =>
        db
          .select()
          .from(todos)
          .where(and(inArray(todos.userId, [user, user2]), like(todos.text, 'a%')))
          .orderBy(todos.id),
      // No escape character: the backslash is one.
      likeBackslash: () => db.select({ id: todos.id }).from(todos).where(like(todos.text, 'a\\%')).orderBy(todos.id),
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
      usersByTagTodo: () =>
        db
          .select({ id: users.id, tag: tags.id })
          .from(users)
          .innerJoin(tags, eq(tags.todoId, users.id))
          .orderBy(users.id, tags.id),
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
      // `NOCASE`: 'ann' is 'ANN'.
      nickIs: ({ low }) => db.select({ id: users.id }).from(users).where(eq(users.nick, NICKS[low]!)).orderBy(users.id),
      nickFirst: () => db.select({ id: users.id, nick: users.nick }).from(users).orderBy(users.nick, users.id).limit(2),
      // SQLite sorts `NULL` first.
      dueFirst: () =>
        db.select({ id: todos.id, due: todos.due }).from(todos).orderBy(asc(todos.due), todos.id).limit(3),
      dueLast: () =>
        db.select({ id: todos.id, due: todos.due }).from(todos).orderBy(desc(todos.due), todos.id).limit(3),
      // A negative `LIMIT` is none.
      afterThree: () => db.all(sql`select id from todos order by id limit -1 offset 3`),
      // SQLite runs compound operators from the left, `INTERSECT` too: (a UNION b) INTERSECT c. Text, so the counts decide.
      compound: ({ user }) =>
        db.all(
          sql`select text from todos where user_id = ${user} union select name from tags intersect select text from todos where done = 1 order by 1`,
        ),
      // A number bound to a text column compares as text: 10 as '10.0'.
      textIsNumber: () => db.all(sql`select id from todos where text = ${10.5}`),
      // A text bound to an integer column compares as a number.
      priorityIsText: ({ low }) => db.all(sql`select id from todos where priority = ${String(low)} order by id`),
      settings: () => db.select().from(settings).orderBy(settings.id),
    },
    writes: {
      insertTodo: ({ user, value, text, date }) =>
        db
          .insert(todos)
          .values({
            userId: user,
            text: value % 7 === 0 ? `a\\${text}` : text,
            done: value % 2 === 0,
            priority: value % 5,
            amount: value / 10,
            updatedAt: date.toISOString(),
            due: value % 4 === 0 ? null : value % 10,
          })
          .run(),
      updateText: ({ todo, text }) => db.update(todos).set({ text }).where(eq(todos.id, todo)).run(),
      // 10.5 as text: what `textIsNumber` reads.
      textOfNumber: ({ todo }) => db.update(todos).set({ text: '10.5' }).where(eq(todos.id, todo)).run(),
      toggleDone: ({ todo }) =>
        db
          .update(todos)
          .set({ done: sql`not ${todos.done}` })
          .where(eq(todos.id, todo))
          .run(),
      updatePriority: ({ todo, value, date }) =>
        db
          .update(todos)
          .set({ priority: value % 5, updatedAt: date.toISOString() })
          .where(eq(todos.id, todo))
          .run(),
      setDue: ({ todo, value }) =>
        db
          .update(todos)
          .set({ due: value % 3 === 0 ? null : value % 10 })
          .where(eq(todos.id, todo))
          .run(),
      moveTodo: ({ todo, otherUser }) =>
        db.update(todos).set({ userId: otherUser }).where(eq(todos.id, todo)).returning({ id: todos.id }).all(),
      deleteTodo: ({ todo }) => db.delete(todos).where(eq(todos.id, todo)).run(),
      insertTag: ({ todo, text }) => db.insert(tags).values({ todoId: todo, name: text }).run(),
      deleteTag: ({ tag }) => db.delete(tags).where(eq(tags.id, tag)).run(),
      renameUser: ({ user, text, value }) =>
        db
          .update(users)
          .set({ name: text, team: (value % 3) + 1 })
          .where(eq(users.id, user))
          .run(),
      renameNick: ({ user, value }) =>
        db
          .update(users)
          .set({ nick: NICKS[value % NICKS.length] })
          .where(eq(users.id, user))
          .run(),
      upsertTodo: ({ todo, user, text }) =>
        db
          .insert(todos)
          .values({ id: todo, userId: user, text, done: false, priority: 0, amount: 0, updatedAt: '2026-11-11' })
          .onConflictDoUpdate({ target: todos.id, set: { text } })
          .run(),
      rawBump: ({ user }) => db.run(sql`update todos set priority = priority + 1 where user_id = ${user}`),
      // Deletes the setting of that name, then inserts it with a new id.
      replaceSetting: ({ value }) =>
        db.run(sql`replace into settings (name, value) values (${`s${value % 5}`}, ${value})`),
      insertOrIgnore: ({ user, text }) =>
        db.run(sql`insert or ignore into users (id, name, team) values (${user}, ${text}, 1)`),
      insertSelect: ({ user }) =>
        db.run(
          sql`insert into tags (todo_id, name) select id, 'copied' from todos where user_id = ${user} order by id limit 1`,
        ),
      deleteUser: ({ user }) => db.delete(users).where(eq(users.id, user)).run(),
      // Two statements in one text: run as written.
      deleteUserRaw: ({ user }) => client.exec(`delete from users where id = ${Number(user)}; select 1`),
      insertUser: ({ text, value }) =>
        db
          .insert(users)
          .values({ name: text, team: (value % 3) + 1, nick: NICKS[value % NICKS.length] })
          .run(),
      txCommit: ({ todo, user, text }) =>
        db.transaction((tx: any) => {
          tx.insert(todos)
            .values({ userId: user, text, done: false, priority: 1, amount: 1, updatedAt: '2026-11-12' })
            .run()
          tx.update(todos).set({ done: true }).where(eq(todos.id, todo)).run()
        }),
      txRollback: ({ user, text }) => {
        try {
          db.transaction((tx: any) => {
            tx.insert(todos)
              .values({ userId: user, text, done: false, priority: 1, amount: 1, updatedAt: '2026-11-12' })
              .run()
            tx.rollback()
          })
        } catch {}
      },
      savepointRollback: ({ todo, user, text }) =>
        db.transaction((tx: any) => {
          tx.update(todos).set({ text }).where(eq(todos.id, todo)).run()
          try {
            tx.transaction((inner: any) => {
              inner
                .insert(todos)
                .values({ userId: user, text: `${text}!`, done: false, priority: 1, amount: 1, updatedAt: '2026' })
                .run()
              inner.rollback()
            })
          } catch {}
        }),
      // A statement that fails in a transaction undoes only itself: the transaction commits the rest.
      txFailedStatement: ({ todo, value }) =>
        db.transaction((tx: any) => {
          tx.update(todos)
            .set({ priority: value % 5 })
            .where(eq(todos.id, todo))
            .run()
          try {
            tx.run(sql`insert into users (id, name, team) values (1, 'taken', 1)`)
          } catch {}
        }),
    },
    liveRead: () => db.select().from(todos).where(eq(todos.userId, 1)).live(),
    async close() {},
  }
  return db.batch ? withBatches(driver, db) : driver
}

/** D1 refuses `BEGIN`: its transaction is a `batch()`, which commits whole or not at all. */
function withBatches(driver: OrmDriver, db: any): OrmDriver {
  const { txCommit, txRollback, savepointRollback, txFailedStatement, ...writes } = driver.writes
  const newTodo = (user: number, text: string) =>
    db.insert(todos).values({ userId: user, text, done: false, priority: 1, amount: 1, updatedAt: '2026-11-12' })
  return {
    ...driver,
    writes: {
      ...writes,
      txCommit: ({ todo, user, text }) =>
        db.batch([newTodo(user, text), db.update(todos).set({ done: true }).where(eq(todos.id, todo))]),
      // The second statement fails: the first is undone with it.
      txRollback: ({ user, text }) =>
        db.batch([newTodo(user, text), db.run(sql`insert into users (id, name, team) values (1, 'taken', 1)`)]),
    },
  }
}
