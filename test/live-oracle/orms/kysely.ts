export { createKysely }

import { Kysely, PostgresDialect, sql } from 'kysely'
import { jsonArrayFrom } from 'kysely/helpers/postgres'
import { liveKysely } from '@telefunc/live/kysely'
import type { BroadcastAdapter } from 'telefunc'
import { arrayOf, CODES, COLUMN_TYPES, REFS, type Spelling } from '../spellings.ts'
import { EVENT_TAG, eventQuery } from './eventQuery.ts'
import type { OrmDriver } from './types.ts'

function createKysely(client: any, kind: 'pg' | 'pglite', transport?: BroadcastAdapter): OrmDriver {
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
  const db = liveKysely(new Kysely<any>({ dialect: new PostgresDialect({ pool }) }), client, { transport })
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
      // A pattern sees a `char(4)` value padded with spaces: 'ab' is 'ab  '.
      codeLike: () => db.selectFrom('todos').select('id').where('code', 'like', 'ab__').orderBy('id').execute(),
      codeNotIlike: () => db.selectFrom('todos').select('id').where('code', 'not ilike', 'X').orderBy('id').execute(),
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
      // `tags.todo_id` references `todos`, not `users`: a user just inserted may already have rows here.
      usersByTagTodo: () =>
        db
          .selectFrom('users')
          .innerJoin('tags', 'tags.todo_id', 'users.id')
          .select(['users.id', 'tags.id as tag'])
          .orderBy('users.id')
          .orderBy('tags.id')
          .execute(),
      usersWithTagTodo: () =>
        db
          .selectFrom('users')
          .select('users.id')
          .where((eb) =>
            eb.exists(eb.selectFrom('tags').select(sql`1`.as('one')).whereRef('tags.todo_id', '=', 'users.id')),
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
      tagLog: async ({ low, high }) =>
        (
          await sql`select todo_id from tag_log where todo_id between ${low * 8} and ${high * 8 + 7} order by id`.execute(
            db,
          )
        ).rows,
      noteLog: async () =>
        (await sql`select note, count(*)::int as n from note_log group by note order by note`.execute(db)).rows,
      publicNotes: async () => (await sql`select id, n from public.notes order by id`.execute(db)).rows,
      tenantNotes: async () => (await sql`select id, n from tenant.notes order by id`.execute(db)).rows,
      eventLog: async () =>
        (await sql`select kind, count(*)::int as n from event_log group by kind order by kind`.execute(db)).rows,
      counters: async () => (await sql`select id, n from counters order by id`.execute(db)).rows,
      chained: async () => (await sql`select n from chained`.execute(db)).rows,
      labelled: () =>
        db.selectFrom('todos').select('id').where(sql<boolean>`label_of(text) like 'A%'`).orderBy('id').execute(),
      measuresAtFloat: async () =>
        (await sql`select id from measures where value = ${String(2 ** 53)}::float8 order by id`.execute(db)).rows,
      measuresOffFloat: async () =>
        (await sql`select id from measures where value <> ${String(2 ** 53)}::float8 order by id`.execute(db)).rows,
      settled: async () => (await sql`select n from settled`.execute(db)).rows,
      spelled: ({ spellings: [a] }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where(sql<boolean>`${column(a!)} = ${operand(a!)}`)
          .orderBy('id')
          .execute(),
      spelledIn: ({ spellings: [a, b] }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where(sql<boolean>`${column(a!)} in (${operand(a!)}, ${operand(b!)})`)
          .orderBy('id')
          .execute(),
      spelledAny: ({ spellings }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where(sql<boolean>`${column(spellings[0]!)} = any(${arrayOf(spellings)})`)
          .orderBy('id')
          .execute(),
      // Lists cast to an array of the column's type, `NULL` lists, and Boolean tests that tell `NULL` from `FALSE`.
      listCast: ({ spellings, list }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where(sql<boolean>`${column(spellings[0]!)} = any(${list}${sql.raw(arrayType(spellings[0]!))})`)
          .orderBy('id')
          .execute(),
      listIsNotFalse: ({ spellings, list }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where(sql<boolean>`(${column(spellings[0]!)} = any(${list})) is not false`)
          .orderBy('id')
          .execute(),
      listAllIsNotTrue: ({ spellings, list }) =>
        db
          .selectFrom('todos')
          .select('id')
          .where(
            sql<boolean>`(${column(spellings[0]!)} = all(${list}${sql.raw(arrayType(spellings[0]!))})) is not true`,
          )
          .orderBy('id')
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
      // 2^53, 2^53 + 1 or 2^53 + 2: the first two are one float8.
      setMeasure: ({ value }) =>
        sql`update measures set value = ${String(2 ** 53)}::numeric + ${value % 3} where id = ${(value % 2) + 1}`.execute(
          db,
        ),
      // The update runs in the transaction `COMMIT AND CHAIN` starts: it's announced when that one commits. A refetch
      // in the pause would see the row as it was.
      chainCommit: () =>
        db.transaction().execute(async (tx) => {
          await sql`commit and chain`.execute(tx)
          await sql`update chained set n = n + 1`.execute(tx)
          await new Promise((resolve) => setTimeout(resolve, 100))
        }),
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
        try {
          await tx.updateTable('todos').set({ text }).where('id', '=', todo).execute()
          const sp = await tx.savepoint('sp1').execute()
          await sp
            .insertInto('todos')
            .values({ user_id: user, text: `${text}!` })
            .execute()
          await sp.rollbackToSavepoint('sp1').execute()
          await tx.commit().execute()
        } catch (err) {
          await tx.rollback().execute()
          throw err
        }
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
      // Two statements in one text: sent as written.
      deleteUserRaw: ({ user }) => sql.raw(`delete from users where id = ${Number(user)}; select 1`).execute(db),
      insertUser: ({ text, value }) =>
        db
          .insertInto('users')
          .values({ name: text, team: (value % 3) + 1 })
          .execute(),
      setPath: ({ value }) => sql.raw(`set search_path to ${schemaOf(value)}, public`).execute(db),
      bumpNote: ({ value }) => sql`update notes set n = n + 1 where id = ${1 + (value % 5)}`.execute(db),
      addEvent: ({ text }) => sql`insert into events (kind) values (${text.slice(0, 1)})`.execute(db),
      eventInTx: ({ todo, text, value }) =>
        db.transaction().execute(async (tx) => {
          await sql`insert into events (kind) values (${text.slice(0, 1)})`.execute(tx)
          await tx
            .updateTable('todos')
            .set({ priority: value % 5 })
            .where('id', '=', todo)
            .execute()
        }),
      resetEvents: () => sql`insert into resets default values`.execute(db),
      stampRead: () => sql`select stamp()`.execute(db),
      // One text: the update names the table by the search_path the statement before it sets.
      notesPath: ({ value }) =>
        sql
          .raw(
            `set search_path to ${schemaOf(value)}, public; update notes set n = n + 1 where id = ${1 + (value % 5)}`,
          )
          .execute(db),
      ...(kind === 'pg' && {
        eventTag: ({ todo, text }) => eventQuery(client, EVENT_TAG, [todo, text]),
      }),
      recode: ({ todo, value }) =>
        db
          .updateTable('todos')
          .set({ code: CODES[value % CODES.length], ref: REFS[(value >> 2) % REFS.length] })
          .where('id', '=', todo)
          .execute(),
    },
    liveRead: () => db.selectFrom('todos').selectAll().where('user_id', '=', 1).live(),
    async close() {},
  }
}

const column = ({ column }: Spelling) => sql.ref(`todos.${column}`)

/** A spelling as a parameter or inline, followed by its cast. */
const operand = ({ value, cast, inline }: Spelling) =>
  inline ? sql.raw(`'${value.replace(/'/g, "''")}'${cast}`) : sql`${value}${sql.raw(cast)}`

const schemaOf = (value: number) => (value % 3 === 0 ? 'public' : 'tenant')

const arrayType = ({ column }: Spelling) => `::${COLUMN_TYPES[column]}[]`
