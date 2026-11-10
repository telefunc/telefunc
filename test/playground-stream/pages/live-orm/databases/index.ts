export { getDatabase, ORMS }
export type { Orm, Database }

import type { PGlite } from '@electric-sql/pglite'

const ORMS = ['drizzle', 'prisma', 'kysely'] as const
type Orm = (typeof ORMS)[number]

/** The same queries through each ORM, on a PGlite database of its own. */
type Database = {
  getTodos(): Promise<string[]>
  /** The first two todos: a todo added after them is past the boundary. */
  getFirstTodos(): Promise<string[]>
  /** The notes of one todo: the other todo's notes are rows of the same table. */
  getNotesOf(todoId: number): Promise<string[]>
  getTodosWithNotes(): Promise<string[]>
  addTodo(text: string): Promise<void>
  addNote(todoId: number, text: string): Promise<void>
  /** One transaction that commits. */
  addTodoAndNote(text: string): Promise<void>
  /** One transaction that rolls back after its insert. */
  addTodoThenRollBack(text: string): Promise<void>
}

const databases = new Map<Orm, Promise<Database>>()

function getDatabase(orm: Orm): Promise<Database> {
  let database = databases.get(orm)
  if (!database) {
    database = createDatabase(orm)
    databases.set(orm, database)
  }
  return database
}

async function createDatabase(orm: Orm): Promise<Database> {
  const { PGlite } = await import('@electric-sql/pglite')
  const pglite = new PGlite()
  await createTables(pglite)
  if (orm === 'drizzle') return (await import('./drizzle')).createDrizzleDatabase(pglite)
  if (orm === 'prisma') return (await import('./prisma')).createPrismaDatabase(pglite)
  return (await import('./kysely')).createKyselyDatabase(pglite)
}

async function createTables(pglite: PGlite) {
  await pglite.exec(`
    create table todos (id serial primary key, text text not null);
    create table notes (id serial primary key, text text not null, todo_id int references todos (id));
    insert into todos (text) values ('First todo'), ('Second todo');
    insert into notes (text, todo_id) values ('Note of the first todo', 1), ('Note of the second todo', 2);
  `)
}
