export { createDrizzleDatabase }

import type { PGlite } from '@electric-sql/pglite'
import { derived, type Live } from '@telefunc/live'
import { liveDrizzle } from '@telefunc/live/drizzle'
import { eq } from 'drizzle-orm'
import { integer, pgTable, serial, text } from 'drizzle-orm/pg-core'
import { drizzle } from 'drizzle-orm/pglite'
import type { Database } from '.'

const todos = pgTable('todos', { id: serial().primaryKey(), text: text().notNull() })
const notes = pgTable('notes', { id: serial().primaryKey(), text: text().notNull(), todoId: integer('todo_id') })

function createDrizzleDatabase(client: PGlite): Database {
  const db = liveDrizzle(drizzle({ client }))
  return {
    async getTodos() {
      return texts(await db.select().from(todos).orderBy(todos.id).live())
    },
    async getFirstTodos() {
      return texts(await db.select().from(todos).orderBy(todos.id).limit(2).live())
    },
    async getNotesOf(todoId) {
      return texts(await db.select().from(notes).where(eq(notes.todoId, todoId)).orderBy(notes.id).live())
    },
    async getTodosWithNotes() {
      const rows = await db
        .select()
        .from(todos)
        .leftJoin(notes, eq(notes.todoId, todos.id))
        .orderBy(todos.id, notes.id)
        .live()
      return derived(() => rows.data.map((row) => `${row.todos.text}: ${row.notes?.text ?? '-'}`))
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

function texts(rows: Live<{ text: string }[]>): Live<string[]> {
  return derived(() => rows.data.map((row) => row.text))
}
