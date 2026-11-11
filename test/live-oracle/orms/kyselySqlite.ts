export { createKyselySqlite }

import type BetterSqlite3 from 'better-sqlite3'
import { Kysely, SqliteDialect, sql } from 'kysely'
import { jsonArrayFrom } from 'kysely/helpers/sqlite'
import { liveKysely } from '@telefunc/live/kysely'
import type { OrmDriver } from './types.ts'

const NICKS = ['ann', 'ANN', 'Bob', 'bob', 'cy', 'Dee']

function createKyselySqlite(client: BetterSqlite3.Database): OrmDriver {
  const db = liveKysely(new Kysely<any>({ dialect: new SqliteDialect({ database: client }) }), client)
  return {
    reads: {
      byUser: ({ user }) => db.selectFrom('todos').selectAll().where('user_id', '=', user).orderBy('id').execute(),
      deletedTodos: ({ user }) =>
        db.selectFrom('deleted_todos').select('todo_id').where('user_id', '=', user).orderBy('todo_id').execute(),
      firstTexts: () => db.selectFrom('todos').select(['id', 'text']).orderBy('text').orderBy('id').limit(3).execute(),
      textsBelow: () => db.selectFrom('todos').select(['id', 'text']).where('text', '<', 'B').orderBy('id').execute(),
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
        db.selectFrom('todos').select(['id', 'text']).where('done', '=', 0).orderBy('id').limit(5).offset(3).execute(),
      // Kysely writes an offset without a limit as `limit -1 offset ?`... with the -1 bound as a parameter.
      afterThree: () => db.selectFrom('todos').select('id').orderBy('id').limit(-1).offset(3).execute(),
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
              eb
                .selectFrom('todos')
                .select(['todos.id', 'todos.text', 'todos.done', 'todos.priority'])
                .whereRef('todos.user_id', '=', 'users.id')
                .orderBy('todos.id'),
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
      likeBackslash: () => db.selectFrom('todos').select('id').where('text', 'like', 'a\\%').orderBy('id').execute(),
      sumAmount: ({ user }) =>
        db
          .selectFrom('todos')
          .select((eb) => eb.fn.sum('amount').as('total'))
          .where('user_id', '=', user)
          .execute(),
      busyUsers: () =>
        db
          .selectFrom('todos')
          .select(['user_id', (eb) => eb.fn.countAll().as('n')])
          .groupBy('user_id')
          .having((eb) => eb.fn.countAll(), '>', 7)
          .orderBy('user_id')
          .execute(),
      topPriority: () =>
        db
          .selectFrom('todos')
          .select(['user_id', (eb) => eb.fn.max('priority').as('top')])
          .groupBy('user_id')
          .orderBy('user_id')
          .execute(),
      priorityKinds: ({ user }) =>
        db
          .selectFrom('todos')
          .select((eb) => eb.fn.count('priority').distinct().as('kinds'))
          .where('user_id', '=', user)
          .execute(),
      todosOrTagged: ({ user }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where('user_id', '=', user)
          .union(db.selectFrom('tags').select('todo_id as id').where('name', '=', 'tag 1'))
          .orderBy('id')
          .execute(),
      untagged: ({ user }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where('user_id', '=', user)
          .except(db.selectFrom('tags').select('todo_id as id'))
          .orderBy('id')
          .execute(),
      distinctUsers: () =>
        db.selectFrom('todos').select('user_id').distinct().where('done', '=', 1).orderBy('user_id').execute(),
      usersWithOpen: () =>
        db
          .selectFrom('users')
          .select('users.id')
          .where(({ exists, selectFrom }) =>
            exists(
              selectFrom('todos')
                .select(sql`1`.as('one'))
                .whereRef('todos.user_id', '=', 'users.id')
                .where('todos.done', '=', 0),
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
      nickIs: ({ low }) => db.selectFrom('users').select('id').where('nick', '=', NICKS[low]!).orderBy('id').execute(),
      nickFirst: () => db.selectFrom('users').select(['id', 'nick']).orderBy('nick').orderBy('id').limit(2).execute(),
      dueFirst: () => db.selectFrom('todos').select(['id', 'due']).orderBy('due').orderBy('id').limit(3).execute(),
      dueLast: () =>
        db.selectFrom('todos').select(['id', 'due']).orderBy('due', 'desc').orderBy('id').limit(3).execute(),
      settings: () => db.selectFrom('settings').selectAll().orderBy('id').execute(),
    },
    writes: {
      insertTodo: ({ user, value, text, date }) =>
        db
          .insertInto('todos')
          .values({
            user_id: user,
            text: value % 7 === 0 ? `a\\${text}` : text,
            done: value % 2,
            priority: value % 5,
            amount: value / 10,
            updated_at: date.toISOString(),
            due: value % 4 === 0 ? null : value % 10,
          })
          .execute(),
      updateText: ({ todo, text }) => db.updateTable('todos').set({ text }).where('id', '=', todo).execute(),
      toggleDone: ({ todo }) => db.updateTable('todos').set({ done: sql`not done` }).where('id', '=', todo).execute(),
      updatePriority: ({ todo, value, date }) =>
        db
          .updateTable('todos')
          .set({ priority: value % 5, updated_at: date.toISOString() })
          .where('id', '=', todo)
          .execute(),
      setDue: ({ todo, value }) =>
        db
          .updateTable('todos')
          .set({ due: value % 3 === 0 ? null : value % 10 })
          .where('id', '=', todo)
          .execute(),
      moveTodo: ({ todo, otherUser }) =>
        db.updateTable('todos').set({ user_id: otherUser }).where('id', '=', todo).returning('id').execute(),
      deleteTodo: ({ todo }) => db.deleteFrom('todos').where('id', '=', todo).execute(),
      insertTag: ({ todo, text }) => db.insertInto('tags').values({ todo_id: todo, name: text }).execute(),
      deleteTag: ({ tag }) => db.deleteFrom('tags').where('id', '=', tag).execute(),
      renameUser: ({ user, text, value }) =>
        db
          .updateTable('users')
          .set({ name: text, team: (value % 3) + 1 })
          .where('id', '=', user)
          .execute(),
      renameNick: ({ user, value }) =>
        db
          .updateTable('users')
          .set({ nick: NICKS[value % NICKS.length] })
          .where('id', '=', user)
          .execute(),
      upsertTodo: ({ todo, user, text }) =>
        db
          .insertInto('todos')
          .values({ id: todo, user_id: user, text })
          .onConflict((oc) => oc.column('id').doUpdateSet({ text }))
          .execute(),
      replaceSetting: ({ value }) =>
        db
          .replaceInto('settings')
          .values({ name: `s${value % 5}`, value })
          .execute(),
      insertOrIgnore: ({ user, text }) =>
        db.insertInto('users').orIgnore().values({ id: user, name: text, team: 1 }).execute(),
      updateFrom: ({ user }) =>
        db
          .updateTable('todos')
          .from('users')
          .set((eb) => ({ priority: eb.ref('users.team') }))
          .whereRef('users.id', '=', 'todos.user_id')
          .where('users.id', '=', user)
          .execute(),
      deleteUser: ({ user }) => db.deleteFrom('users').where('id', '=', user).execute(),
      insertUser: ({ text, value }) =>
        db
          .insertInto('users')
          .values({ name: text, team: (value % 3) + 1, nick: NICKS[value % NICKS.length] })
          .execute(),
      txCommit: ({ todo, user, text }) =>
        db.transaction().execute(async (tx) => {
          await tx.insertInto('todos').values({ user_id: user, text }).execute()
          await tx.updateTable('todos').set({ done: 1 }).where('id', '=', todo).execute()
        }),
      txRollback: ({ user, text }) =>
        db
          .transaction()
          .execute(async (tx) => {
            await tx.insertInto('todos').values({ user_id: user, text }).execute()
            throw new Error('rolled back')
          })
          .catch(() => {}),
      rawBump: ({ user }) => sql`update todos set priority = priority + 1 where user_id = ${user}`.execute(db),
    },
    liveRead: () => db.selectFrom('todos').selectAll().where('user_id', '=', 1).live(),
    async close() {},
  }
}
