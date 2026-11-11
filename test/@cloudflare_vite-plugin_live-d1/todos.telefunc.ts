import { env } from 'cloudflare:workers'
import { drizzle } from 'drizzle-orm/d1'
import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core'
import { liveDrizzle } from '@telefunc/live/drizzle'

const todos = sqliteTable('todos', { id: integer('id').primaryKey(), text: text('text').notNull() })

const reader = liveDrizzle(drizzle(env.READER_DB))
const writer = liveDrizzle(drizzle(env.WRITER_DB))

export async function onSetup() {
  await env.WRITER_DB.exec('create table if not exists todos (id integer primary key, text text not null)')
}

export async function onTodos() {
  return await reader.select().from(todos).orderBy(todos.id).live()
}

export async function onAddTodo(text: string) {
  await writer.insert(todos).values({ text })
}
