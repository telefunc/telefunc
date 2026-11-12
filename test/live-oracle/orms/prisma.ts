export { createPrisma }

import postgres from '@prisma/orm-postgres/runtime'
import { contractFromJson } from '@prisma/orm-postgres/static'
import contractJson from './prisma-contract.json' with { type: 'json' }
import { livePrisma } from '@telefunc/live/prisma'
import type { BroadcastTransport } from 'telefunc'
import type { OrmDriver } from './types.ts'

/** Prisma 8 connects through the app's `pg.Pool`. Shapes its API can't express are left out. */
function createPrisma(pool: unknown, transport?: BroadcastTransport): OrmDriver {
  const db = livePrisma(
    postgres({ contract: contractFromJson(contractJson as never), pg: pool as never }) as never,
    pool as never,
    { transport },
  ) as any
  const orm = (from: any) => from.orm.public
  const { Todo, User, Tag } = orm(db)
  return {
    reads: {
      gaugesAtFloat: () =>
        db
          .query(
            db.raw.sql`select id from gauges where value = ${String(2 ** 53)}::float8 order by id`
              .returnsRow({ id: 'pg/int4@1' })
              .build(),
          )
          .toArray(),
      gaugesOffFloat: () =>
        db
          .query(
            db.raw.sql`select id from gauges where value <> ${String(2 ** 53)}::float8 order by id`
              .returnsRow({ id: 'pg/int4@1' })
              .build(),
          )
          .toArray(),
      // `tags.todo_id` references `todos`, not `users`: a user just inserted may already have rows here.
      usersByTagTodo: () =>
        db
          .query(
            db.raw.sql`select u.id, g.id as tag from users u join tags g on g.todo_id = u.id order by u.id, g.id`
              .returnsRow({ id: 'pg/int4@1', tag: 'pg/int4@1' })
              .build(),
          )
          .toArray(),
      byUser: ({ user }) =>
        Todo.where((t: any) => t.userId.eq(user))
          .orderBy((t: any) => t.id.asc())
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
          .where((t: any) => t.done.eq(false))
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
      likeIn: ({ user, user2 }) =>
        Todo.where((t: any) => t.userId.in([user, user2]))
          .where((t: any) => t.text.like('a%'))
          .orderBy((t: any) => t.id.asc())
          .all(),
      usersWithOpen: () =>
        User.select('id')
          .where((u: any) => u.todos.some((t: any) => t.done.eq(false)))
          .orderBy((u: any) => u.id.asc())
          .all(),
      priorityRange: ({ low, high }) =>
        Todo.select('id')
          .where((t: any) => t.priority.gte(low))
          .where((t: any) => t.priority.lte(high))
          .orderBy((t: any) => t.id.asc())
          .all(),
    },
    writes: {
      insertTodo: ({ user, value, text, date }) =>
        Todo.create({
          userId: user,
          text,
          done: value % 2 === 0,
          priority: value % 5,
          amount: String(value),
          updatedAt: date,
        }),
      updateText: ({ todo, text }) => Todo.where({ id: todo }).updateAll({ text }),
      toggleDone: ({ todo }) =>
        db.execute(db.raw.sql`update todos set done = not done where id = ${todo}`.affectedCount().build()),
      updatePriority: ({ todo, value, date }) =>
        Todo.where({ id: todo }).updateAll({ priority: value % 5, updatedAt: date }),
      moveTodo: ({ todo, otherUser }) => Todo.where({ id: todo }).updateAll({ userId: otherUser }),
      deleteTodo: ({ todo }) => Todo.where({ id: todo }).deleteAll(),
      insertTag: ({ todo, text }) => Tag.create({ todoId: todo, name: text }),
      deleteTag: ({ tag }) => Tag.where({ id: tag }).deleteAll(),
      renameUser: ({ user, text, value }) => User.where({ id: user }).updateAll({ name: text, team: (value % 3) + 1 }),
      upsertTodo: ({ todo, user, text }) => Todo.upsert({ create: { id: todo, userId: user, text }, update: { text } }),
      rawBump: ({ user }) =>
        db.execute(
          db.raw.sql`update todos set priority = priority + 1 where user_id = ${user}`.affectedCount().build(),
        ),
      // 2^53, 2^53 + 1 or 2^53 + 2: the first two are one float8.
      setGauge: ({ value }) =>
        db.execute(
          db.raw.sql`update gauges set value = ${String(2 ** 53)}::numeric + ${value % 3} where id = ${(value % 2) + 1}`
            .affectedCount()
            .build(),
        ),
      txCommit: ({ todo, user, text }) =>
        db.transaction(async (tx: any) => {
          await orm(tx).Todo.create({ userId: user, text })
          await orm(tx).Todo.where({ id: todo }).updateAll({ done: true })
        }),
      txRollback: ({ user, text }) =>
        db
          .transaction(async (tx: any) => {
            await orm(tx).Todo.create({ userId: user, text })
            throw new Error('roll back')
          })
          .catch(() => {}),
      insertSelect: ({ user }) =>
        db.execute(
          db.raw
            .sql`insert into tags (todo_id, name) select id, 'copied' from todos where user_id = ${user} order by id limit 1`
            .affectedCount()
            .build(),
        ),
      deleteUser: ({ user }) => User.where({ id: user }).deleteAll(),
      insertUser: ({ text, value }) => User.create({ name: text, team: (value % 3) + 1 }),
      // A todo with an id neither sequence reaches, so no user has it, and a tag on it; a new user owns it, as a racing
      // delete may have left none. `insertTaggedUser` then inserts a user with that id.
      insertOrphanTag: ({ text }) =>
        db.execute(
          db.raw
            .sql`with owner as (insert into users (name, team) values (${text}, 1) returning id), todo as (insert into todos (id, user_id, text) select 1000000, id, ${text} from owner returning id) insert into tags (todo_id, name) select id, ${text} from todo`
            .affectedCount()
            .build(),
        ),
      // A user with the id some tags' `todo_id` holds: it joins rows of `usersByTagTodo` as it's inserted.
      insertTaggedUser: ({ text }) =>
        db.execute(
          db.raw
            .sql`insert into users (id, name, team) select g.todo_id, ${text}, 1 from tags g where not exists (select 1 from users u where u.id = g.todo_id) order by g.todo_id desc limit 1`
            .affectedCount()
            .build(),
        ),
    },
    async close() {
      await db.close?.()
    },
  }
}
