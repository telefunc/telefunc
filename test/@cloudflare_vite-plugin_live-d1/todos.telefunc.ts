import { env } from 'cloudflare:workers'
import { getContext } from 'telefunc'
import { drizzle } from 'drizzle-orm/d1'
import { integer, sqliteTable, text } from 'drizzle-orm/sqlite-core'
import { liveDrizzle } from '@telefunc/live/drizzle'

const todos = sqliteTable('todos', { id: integer('id').primaryKey(), text: text('text').notNull() })
const notes = sqliteTable('notes', { id: integer('id').primaryKey(), text: text('text').notNull() })

const reader = liveDrizzle(drizzle(env.READER_DB))
const writer = liveDrizzle(drizzle(env.WRITER_DB))

// Once: a `create table` is a write to its table, which a later page's setup would make every reader refetch.
export async function onSetup() {
  const tables = await env.WRITER_DB.prepare("select name from sqlite_master where name in ('todos', 'notes')").all()
  if (tables.results.length === 2) return
  await env.WRITER_DB.exec('create table if not exists todos (id integer primary key, text text not null)')
  await env.WRITER_DB.exec('create table if not exists notes (id integer primary key, text text not null)')
}

/** The session Durable Object the page's calls reach. */
export async function onShard() {
  return getContext<{ shard: string }>().shard
}

export async function onTodos() {
  return await reader.select().from(todos).orderBy(todos.id).live()
}

export async function onNotes() {
  return await reader.select().from(notes).orderBy(notes.id).live()
}

export async function onAddTodo(text: string) {
  await writer.insert(todos).values({ text })
}
