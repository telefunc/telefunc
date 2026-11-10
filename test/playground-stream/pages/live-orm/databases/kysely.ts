export { createKyselyDatabase }

import type { PGlite } from '@electric-sql/pglite'
import { withLiveQueries } from '@telefunc/live'
import { Kysely, type Generated } from 'kysely'
import { PGliteDialect } from 'kysely-pglite-dialect'
import type { Database } from '.'

type Tables = {
  todos: { id: Generated<number>; text: string }
  notes: { id: Generated<number>; text: string; todo_id: number | null }
}

function createKyselyDatabase(client: PGlite): Database {
  const db = new Kysely<Tables>({ dialect: new PGliteDialect(withLiveQueries(client)) })
  return {
    async getTodos() {
      return (await db.selectFrom('todos').select('text').orderBy('id').execute()).map((todo) => todo.text)
    },
    async getFirstTodos() {
      return (await db.selectFrom('todos').select('text').orderBy('id').limit(2).execute()).map((todo) => todo.text)
    },
    async getNotesOf(todoId) {
      const rows = await db.selectFrom('notes').select('text').where('todo_id', '=', todoId).orderBy('id').execute()
      return rows.map((note) => note.text)
    },
    async getTodosWithNotes() {
      const rows = await db
        .selectFrom('todos')
        .leftJoin('notes', 'notes.todo_id', 'todos.id')
        .select(['todos.text as todo', 'notes.text as note'])
        .orderBy('todos.id')
        .orderBy('notes.id')
        .execute()
      return rows.map((row) => `${row.todo}: ${row.note ?? '-'}`)
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
