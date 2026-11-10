export { createDrizzleDatabase }

import type { PGlite } from '@electric-sql/pglite'
import { withLiveQueries } from '@telefunc/live'
import { eq } from 'drizzle-orm'
import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import type { Database } from '.'

const todos = pgTable('todos', { id: serial().primaryKey(), text: text().notNull() })
const notes = pgTable('notes', { id: serial().primaryKey(), text: text().notNull(), todoId: integer('todo_id') })

function createDrizzleDatabase(client: PGlite): Database {
  const db = drizzle({ client: withLiveQueries(client) })
  return {
    async getTodos() {
      return (await db.select().from(todos).orderBy(todos.id)).map((todo) => todo.text)
    },
    async getFirstTodos() {
      return (await db.select().from(todos).orderBy(todos.id).limit(2)).map((todo) => todo.text)
    },
    async getNotesOf(todoId) {
      return (await db.select().from(notes).where(eq(notes.todoId, todoId)).orderBy(notes.id)).map((note) => note.text)
    },
    async getTodosWithNotes() {
      const rows = await db.select().from(todos).leftJoin(notes, eq(notes.todoId, todos.id)).orderBy(todos.id, notes.id)
      return rows.map((row) => `${row.todos.text}: ${row.notes?.text ?? '-'}`)
    },
    async addTodo(text) {
      await db.insert(todos).values({ text })
    },
    async addNote(todoId, text) {
      await db.insert(notes).values({ text, todoId })
    },
    async addTodoAndNote(text) {
      await db.transaction(async (tx) => {
        const [todo] = await tx.insert(todos).values({ text }).returning()
        await tx.insert(notes).values({ text, todoId: todo!.id })
      })
    },
    async addTodoThenRollBack(text) {
      await db
        .transaction(async (tx) => {
          await tx.insert(todos).values({ text })
          tx.rollback()
        })
        .catch(() => {})
    },
  }
}
