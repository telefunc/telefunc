export { createPrismaDatabase }

import type { PGlite } from '@electric-sql/pglite'
import { PGLiteSocketServer } from '@electric-sql/pglite-socket'
import postgres from '@prisma/orm-postgres/runtime'
import { contractFromJson } from '@prisma/orm-postgres/static'
import { derived, type Live } from '@telefunc/live'
import { livePrisma } from '@telefunc/live/prisma'
import pg from 'pg'
import contractJson from './prisma-contract.json'
import type { Database } from '.'

// `prisma contract emit` also writes the contract's types, but they import Prisma packages an app can't resolve
// (8.0.0-rc.23), so the rows are typed here.
type Todo = { id: number; text: string; notes: Note[] }
type Note = { id: number; text: string; todoId: number | null }

async function createPrismaDatabase(pglite: PGlite): Promise<Database> {
  // Prisma connects through a `pg.Pool`: PGlite answers the Postgres protocol on a free port, one session at a time.
  const server = new PGLiteSocketServer({ db: pglite, port: 0 })
  await server.start()
  const pool = new pg.Pool({ connectionString: `postgres://postgres@${server.getServerConn()}/postgres`, max: 1 })
  const db = livePrisma(postgres({ contract: contractFromJson(contractJson), pg: pool }) as any, pool) as any
  const orm = (from: { orm: any }) => from.orm.public
  return {
    async getTodos() {
      return texts(
        await orm(db)
          .Todo.orderBy((todo: any) => todo.id.asc())
          .live(),
      )
    },
    async getFirstTodos() {
      return texts(
        await orm(db)
          .Todo.orderBy((todo: any) => todo.id.asc())
          .limit(2)
          .live(),
      )
    },
    async getNotesOf(todoId) {
      return texts(
        await orm(db)
          .Note.where((note: any) => note.todoId.eq(todoId))
          .orderBy((note: any) => note.id.asc())
          .live(),
      )
    },
    async getTodosWithNotes() {
      const rows: Live<Todo[]> = await orm(db)
        .Todo.include('notes')
        .orderBy((todo: any) => todo.id.asc())
        .live()
      return derived(() =>
        rows.data.flatMap((todo) =>
          todo.notes.length === 0 ? [`${todo.text}: -`] : todo.notes.map((note) => `${todo.text}: ${note.text}`),
        ),
      )
    },
    async addTodo(text) {
      await orm(db).Todo.create({ text })
    },
    async addNote(todoId, text) {
      await orm(db).Note.create({ text, todoId })
    },
    async addTodoAndNote(text) {
      await db.transaction(async (tx: { orm: any }) => {
        const todo = (await orm(tx).Todo.create({ text })) as Todo
        await orm(tx).Note.create({ text, todoId: todo.id })
      })
    },
    async addTodoThenRollBack(text) {
      await db
        .transaction(async (tx: { orm: any }) => {
          await orm(tx).Todo.create({ text })
          throw new Error('Roll back')
        })
        .catch(() => {})
    },
  }
}

function texts(rows: Live<{ text: string }[]>): Live<string[]> {
  return derived(() => rows.data.map((row) => row.text))
}
