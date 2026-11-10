export { createKysely }

import { Kysely, PostgresDialect, sql } from 'kysely'
import { jsonArrayFrom } from 'kysely/helpers/postgres'
import type { OrmDriver } from './types.ts'

function createKysely(client: any, kind: 'pg' | 'pglite'): OrmDriver {
  // PGlite has one session: a "pool" of one connection over it.
  const pool =
    kind === 'pg'
      ? client
      : {
          connect: async () => ({
            query: async (text: string, params: unknown[]) => {
              const result = await client.query(text, params)
              return { ...result, rowCount: result.affectedRows, command: text.trim().split(/\s/)[0]!.toUpperCase() }
            },
            release() {},
          }),
          end: async () => {},
        }
  const db = new Kysely<any>({ dialect: new PostgresDialect({ pool }) })
  return {
    reads: {
      byUser: ({ user }) => db.selectFrom('todos').selectAll().where('user_id', '=', user).orderBy('id').execute(),
      recent: ({ user }) =>
        db
          .selectFrom('todos')
          .selectAll()
          .where('user_id', '=', user)
          .orderBy('updated_at', 'desc')
          .orderBy('id', 'desc')
          .limit(3)
          .execute(),
      priorityTies: ({ user }) =>
        db
          .selectFrom('todos')
          .select(['id', 'priority'])
          .where('user_id', '=', user)
          .orderBy('priority', 'desc')
          .limit(2)
          .execute(),
      openPage: () =>
        db
          .selectFrom('todos')
          .select(['id', 'text'])
          .where('done', '=', false)
          .orderBy('id')
          .limit(5)
          .offset(3)
          .execute(),
      teamJoin: ({ team }) =>
        db
          .selectFrom('todos')
          .innerJoin('users', 'users.id', 'todos.user_id')
          .select(['todos.id', 'todos.text', 'users.name'])
          .where('users.team', '=', team)
          .orderBy('todos.id')
          .execute(),
      userWithTodos: ({ user }) =>
        db
          .selectFrom('users')
          .selectAll('users')
          .select((eb) =>
            jsonArrayFrom(
              eb.selectFrom('todos').selectAll('todos').whereRef('todos.user_id', '=', 'users.id').orderBy('todos.id'),
            ).as('todos'),
          )
          .where('users.id', '=', user)
          .execute(),
      countByDone: ({ user }) =>
        db
          .selectFrom('todos')
          .select(['done', (eb) => eb.fn.countAll().as('n')])
          .where('user_id', '=', user)
          .groupBy('done')
          .orderBy('done')
          .execute(),
      likeIn: ({ user, user2 }) =>
        db
          .selectFrom('todos')
          .selectAll()
          .where('user_id', 'in', [user, user2])
          .where('text', 'like', 'a%')
          .orderBy('id')
          .execute(),
      sumAmount: ({ user }) =>
        db
          .selectFrom('todos')
          .select((eb) => eb.fn.sum('amount').as('total'))
          .where('user_id', '=', user)
          .execute(),
      distinctUsers: () =>
        db.selectFrom('todos').select('user_id').distinct().where('done', '=', true).orderBy('user_id').execute(),
      usersWithOpen: () =>
        db
          .selectFrom('users')
          .select('users.id')
          .where((eb) =>
            eb.exists(
              eb
                .selectFrom('todos')
                .select(sql`1`.as('one'))
                .whereRef('todos.user_id', '=', 'users.id')
                .where('todos.done', '=', false),
            ),
          )
          .orderBy('users.id')
          .execute(),
      tagsOfUser: ({ user }) =>
        db
          .selectFrom('tags')
          .innerJoin('todos', 'todos.id', 'tags.todo_id')
          .select(['tags.id', 'tags.name'])
          .where('todos.user_id', '=', user)
          .orderBy('tags.id')
          .execute(),
      priorityRange: ({ low, high }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where('priority', '>=', low)
          .where('priority', '<=', high)
          .orderBy('id')
          .execute(),
      leftJoinTags: ({ user }) =>
        db
          .selectFrom('todos')
          .leftJoin('tags', 'tags.todo_id', 'todos.id')
          .select(['todos.id', 'tags.name as tag'])
          .where('todos.user_id', '=', user)
          .orderBy('todos.id')
          .orderBy('tags.id')
          .execute(),
    },
    writes: {
      insertTodo: ({ user, value, text, date }) =>
        db
          .insertInto('todos')
          .values({
            user_id: user,
            text,
            done: value % 2 === 0,
            priority: value % 5,
            amount: String(value),
            updated_at: date,
          })
          .execute(),
      updateText: ({ todo, text }) => db.updateTable('todos').set({ text }).where('id', '=', todo).execute(),
      toggleDone: ({ todo }) => db.updateTable('todos').set({ done: sql`not done` }).where('id', '=', todo).execute(),
      updatePriority: ({ todo, value, date }) =>
        db
          .updateTable('todos')
          .set({ priority: value % 5, updated_at: date })
          .where('id', '=', todo)
          .execute(),
      moveTodo: ({ todo, otherUser }) =>
        db.updateTable('todos as t').set({ user_id: otherUser }).where('t.id', '=', todo).returning('t.id').execute(),
      deleteTodo: ({ todo }) => db.deleteFrom('todos').where('id', '=', todo).execute(),
      insertTag: ({ todo, text }) => db.insertInto('tags').values({ todo_id: todo, name: text }).execute(),
      deleteTag: ({ tag }) => db.deleteFrom('tags').where('id', '=', tag).execute(),
      renameUser: ({ user, text, value }) =>
        db
          .updateTable('users')
          .set({ name: text, team: (value % 3) + 1 })
          .where('id', '=', user)
          .execute(),
      upsertTodo: ({ todo, user, text }) =>
        db
          .insertInto('todos')
          .values({ id: todo, user_id: user, text })
          .onConflict((oc) => oc.column('id').doUpdateSet({ text }))
          .execute(),
      rawBump: ({ user }) => sql`update todos set priority = priority + 1 where user_id = ${user}`.execute(db),
      txCommit: ({ todo, user, text }) =>
        db.transaction().execute(async (tx) => {
          await tx.insertInto('todos').values({ user_id: user, text }).execute()
          await tx.updateTable('todos').set({ done: true }).where('id', '=', todo).execute()
        }),
      txRollback: ({ user, text }) =>
        db
          .transaction()
          .execute(async (tx) => {
            await tx.insertInto('todos').values({ user_id: user, text }).execute()
            throw new Error('roll back')
          })
          .catch(() => {}),
      savepointRollback: async ({ todo, user, text }) => {
        const tx = await db.startTransaction().execute()
        await tx.updateTable('todos').set({ text }).where('id', '=', todo).execute()
        const sp = await tx.savepoint('sp1').execute()
        await sp
          .insertInto('todos')
          .values({ user_id: user, text: `${text}!` })
          .execute()
        await sp.rollbackToSavepoint('sp1').execute()
        await tx.commit().execute()
      },
      insertSelect: ({ user }) =>
        db
          .insertInto('tags')
          .columns(['todo_id', 'name'])
          .expression((eb) =>
            eb
              .selectFrom('todos')
              .select(['id', sql.lit('copied').as('name')])
              .where('user_id', '=', user)
              .orderBy('id')
              .limit(1),
          )
          .execute(),
      deleteUser: ({ user }) => db.deleteFrom('users').where('id', '=', user).execute(),
      insertUser: ({ text, value }) =>
        db
          .insertInto('users')
          .values({ name: text, team: (value % 3) + 1 })
          .execute(),
    },
    async close() {},
  }
}
