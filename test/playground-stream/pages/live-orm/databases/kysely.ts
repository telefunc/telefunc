export { createKyselyDatabase }

import type { PGlite } from '@electric-sql/pglite'
import { derived, type Live } from '@telefunc/live'
import { liveKysely } from '@telefunc/live/kysely'
import { Kysely, type Generated } from 'kysely'
import { PGliteDialect } from 'kysely-pglite-dialect'
import type { Database } from '.'

type Tables = {
  todos: { id: Generated<number>; text: string }
  notes: { id: Generated<number>; text: string; todo_id: number | null }
}

function createKyselyDatabase(client: PGlite): Database {
  const db = liveKysely(new Kysely<Tables>({ dialect: new PGliteDialect(client) }), client)
  return {
    async getTodos() {
      return texts(await db.selectFrom('todos').select('text').orderBy('id').live())
    },
    async getFirstTodos() {
      return texts(await db.selectFrom('todos').select('text').orderBy('id').limit(2).live())
    },
    async getNotesOf(todoId) {
      return texts(await db.selectFrom('notes').select('text').where('todo_id', '=', todoId).orderBy('id').live())
    },
    async getTodosWithNotes() {
      const rows = await db
        .selectFrom('todos')
        .leftJoin('notes', 'notes.todo_id', 'todos.id')
        .select(['todos.text as todo', 'notes.text as note'])
        .orderBy('todos.id')
        .orderBy('notes.id')
        .live()
      return derived(() => rows.data.map((row) => `${row.todo}: ${row.note ?? '-'}`))
    },
    async addTodo(text) {
      await db.insertInto('todos').values({ text }).execute()
    },
    async addNote(todoId, text) {
      await db.insertInto('notes').values({ text, todo_id: todoId }).execute()
    },
    async addTodoAndNote(text) {
      await db.transaction().execute(async (trx) => {
        const todo = await trx.insertInto('todos').values({ text }).returning('id').executeTakeFirstOrThrow()
        await trx.insertInto('notes').values({ text, todo_id: todo.id }).execute()
      })
    },
    async addTodoThenRollBack(text) {
      await db
        .transaction()
        .execute(async (trx) => {
          await trx.insertInto('todos').values({ text }).execute()
          throw new Error('Roll back')
        })
        .catch(() => {})
    },
  }
}

function texts(rows: Live<{ text: string }[]>): Live<string[]> {
  return derived(() => rows.data.map((row) => row.text))
}
