export { createPrismaSqlite }

import sqlite from '@prisma/orm-sqlite/runtime'
import { contractFromJson } from '@prisma/orm-sqlite/static'
import contractJson from './prisma-sqlite-contract.json' with { type: 'json' }
import { livePrisma } from '@telefunc/live/prisma-sqlite'
import type { OrmDriver } from './types.ts'

const NICKS = ['ann', 'ANN', 'Bob', 'bob', 'cy', 'Dee']

/** Prisma 8 on SQLite opens its own node:sqlite connections on `path`, one more per transaction. Shapes its API can't
 *  express are left out. */
function createPrismaSqlite(path: string): OrmDriver {
  const db = livePrisma(sqlite({ contract: contractFromJson(contractJson as never), path }) as never, path) as any
  const { Todo, User, Tag, Setting, DeletedTodo } = db.orm
  const raw = (query: unknown) => db.execute(query.affectedCount().build())
  return {
    reads: {
      byUser: ({ user }) =>
        Todo.where((t: any) => t.userId.eq(user))
          .orderBy((t: any) => t.id.asc())
          .all(),
      deletedTodos: ({ user }) =>
        DeletedTodo.select('todoId')
          .where((d: any) => d.userId.eq(user))
          .orderBy((d: any) => d.todoId.asc())
          .all(),
      firstTexts: () =>
        Todo.select('id', 'text')
          .orderBy([(t: any) => t.text.asc(), (t: any) => t.id.asc()])
          .limit(3)
          .all(),
      textsBelow: () =>
        Todo.select('id', 'text')
          .where((t: any) => t.text.lt('B'))
          .orderBy((t: any) => t.id.asc())
          .all(),
      recent: ({ user }) =>
        Todo.where((t: any) => t.userId.eq(user))
          .orderBy([(t: any) => t.updatedAt.desc(), (t: any) => t.id.desc()])
          .limit(3)
          .all(),
      priorityTies: ({ user }) =>
        Todo.select('id', 'priority')
          .where((t: any) => t.userId.eq(user))
          .orderBy((t: any) => t.priority.desc())
          .limit(2)
          .all(),
      openPage: () =>
        Todo.select('id', 'text')
          .where((t: any) => t.done.eq(0))
          .orderBy((t: any) => t.id.asc())
          .limit(5)
          .offset(3)
          .all(),
      userWithTodos: ({ user }) =>
        User.where((u: any) => u.id.eq(user))
          .include('todos', (b: any) => b.orderBy((t: any) => t.id.asc()))
          .all(),
      countByDone: ({ user }) =>
        Todo.where((t: any) => t.userId.eq(user))
          .groupBy('done')
          .aggregate((a: any) => ({ n: a.count() })),
      // SQLite's `LIKE` ignores ASCII case, and has no escape character: `a\%` is `a\` and then anything.
      likeIn: ({ user, user2 }) =>
        Todo.select('id')
          .where((t: any) => t.userId.in([user, user2]))
          .where((t: any) => t.text.like('a%'))
          .orderBy((t: any) => t.id.asc())
          .all(),
      likeBackslash: () =>
        Todo.select('id')
          .where((t: any) => t.text.like('a\\%'))
          .orderBy((t: any) => t.id.asc())
          .all(),
      usersWithOpen: () =>
        User.select('id')
          .where((u: any) => u.todos.some((t: any) => t.done.eq(0)))
          .orderBy((u: any) => u.id.asc())
          .all(),
      priorityRange: ({ low, high }) =>
        Todo.select('id')
          .where((t: any) => t.priority.gte(low))
          .where((t: any) => t.priority.lte(high))
          .orderBy((t: any) => t.id.asc())
          .all(),
      // `nick` ignores ASCII case (`NOCASE`).
      nickIs: ({ low }) =>
        User.select('id')
          .where((u: any) => u.nick.eq(NICKS[low]))
          .orderBy((u: any) => u.id.asc())
          .all(),
      // SQLite sorts `NULL` first.
      dueFirst: () =>
        Todo.select('id', 'due')
          .orderBy([(t: any) => t.due.asc(), (t: any) => t.id.asc()])
          .limit(3)
          .all(),
      settings: () => Setting.orderBy((s: any) => s.id.asc()).all(),
    },
    writes: {
      insertTodo: ({ user, value, text, date }) =>
        Todo.create({
          userId: user,
          text: value % 7 === 0 ? `a\\${text}` : text,
          done: value % 2 === 0 ? 1 : 0,
          priority: value % 5,
          amount: value / 10,
          updatedAt: date.toISOString(),
          due: value % 4 === 0 ? null : value % 10,
        }),
      updateText: ({ todo, text }) => Todo.where({ id: todo }).updateAll({ text }),
      toggleDone: ({ todo }) => raw(db.raw.sql`update todos set done = not done where id = ${todo}`),
      updatePriority: ({ todo, value, date }) =>
        Todo.where({ id: todo }).updateAll({ priority: value % 5, updatedAt: date.toISOString() }),
      setDue: ({ todo, value }) => Todo.where({ id: todo }).updateAll({ due: value % 3 === 0 ? null : value % 10 }),
      moveTodo: ({ todo, otherUser }) => Todo.where({ id: todo }).updateAll({ userId: otherUser }),
      deleteTodo: ({ todo }) => Todo.where({ id: todo }).deleteAll(),
      insertTag: ({ todo, text }) => Tag.create({ todoId: todo, name: text }),
      deleteTag: ({ tag }) => Tag.where({ id: tag }).deleteAll(),
      renameUser: ({ user, text, value }) => User.where({ id: user }).updateAll({ name: text, team: (value % 3) + 1 }),
      renameNick: ({ user, value }) => User.where({ id: user }).updateAll({ nick: NICKS[value % NICKS.length] }),
      upsertTodo: ({ todo, user, text }) =>
        Todo.upsert({
          create: { id: todo, userId: user, text, updatedAt: '2026-11-11T00:00:00.000Z' },
          update: { text },
        }),
      upsertSetting: ({ value }) =>
        Setting.upsert({ create: { name: `s${value % 5}`, value }, update: { value }, conflictOn: { name: true } }),
      rawBump: ({ user }) => raw(db.raw.sql`update todos set priority = priority + 1 where user_id = ${user}`),
      insertSelect: ({ user }) =>
        raw(
          db.raw
            .sql`insert into tags (todo_id, name) select id, 'copied' from todos where user_id = ${user} order by id limit 1`,
        ),
      deleteUser: ({ user }) => User.where({ id: user }).deleteAll(),
      // A todo with an id neither sequence reaches, so no user has it, and a tag on it; a new user owns it, as a racing
      // delete may have left none. `insertTaggedUser` then inserts a user with that id.
      insertOrphanTag: ({ text }) =>
        db.transaction(async (tx: any) => {
          const owner = await tx.orm.User.create({ name: text, team: 1 })
          await tx.orm.Todo.create({ id: 1000000, userId: owner.id, text, updatedAt: '2026-11-12T00:00:00.000Z' })
          await tx.orm.Tag.create({ todoId: 1000000, name: text })
        }),
      // A user with the id some tags' `todo_id` holds: it joins rows of `usersByTagTodo` as it's inserted.
      insertTaggedUser: ({ text }) =>
        raw(
          db.raw
            .sql`insert into users (id, name, team) select g.todo_id, ${text}, 1 from tags g where not exists (select 1 from users u where u.id = g.todo_id) order by g.todo_id desc limit 1`,
        ),
      insertUser: ({ text, value }) =>
        User.create({ name: text, team: (value % 3) + 1, nick: NICKS[value % NICKS.length] }),
      txCommit: ({ todo, user, text }) =>
        db.transaction(async (tx: any) => {
          await tx.orm.Todo.create({
            userId: user,
            text,
            priority: 1,
            amount: 1,
            updatedAt: '2026-11-12T00:00:00.000Z',
          })
          await tx.orm.Todo.where({ id: todo }).updateAll({ done: 1 })
        }),
      txRollback: ({ user, text }) =>
        db
          .transaction(async (tx: any) => {
            await tx.orm.Todo.create({ userId: user, text, updatedAt: '2026-11-12T00:00:00.000Z' })
            throw new Error('roll back')
          })
          .catch(() => {}),
    },
    async close() {
      await db.close?.()
    },
  }
}
