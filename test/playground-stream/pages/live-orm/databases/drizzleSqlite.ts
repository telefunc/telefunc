export { createDrizzleSqliteDatabase }

import type BetterSqlite3 from 'better-sqlite3'
import { derived, type Live } from '@telefunc/live'
import { liveDrizzle } from '@telefunc/live/drizzle'
import { eq } from 'drizzle-orm'
import { drizzle } from 'drizzle-orm/better-sqlite3'
import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core'
import type { Database } from '.'

const todos = sqliteTable('todos', { id: integer().primaryKey(), text: text().notNull() })
const notes = sqliteTable('notes', { id: integer().primaryKey(), text: text().notNull(), todoId: integer('todo_id') })

function createDrizzleSqliteDatabase(client: BetterSqlite3.Database): Database {
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
      db.insert(todos).values({ text }).run()
    },
    async addNote(todoId, text) {
      db.insert(notes).values({ text, todoId }).run()
    },
    async addTodoAndNote(text) {
      db.transaction((tx) => {
        const todo = tx.insert(todos).values({ text }).returning().get()
        tx.insert(notes).values({ text, todoId: todo.id }).run()
      })
    },
    async addTodoThenRollBack(text) {
      try {
        db.transaction((tx) => {
          tx.insert(todos).values({ text }).run()
          tx.rollback()
        })
      } catch {}
    },
  }
}

function texts(rows: Live<{ text: string }[]>): Live<string[]> {
  return derived(() => rows.data.map((row) => row.text))
}
