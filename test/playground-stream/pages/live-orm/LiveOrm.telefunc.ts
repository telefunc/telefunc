export {
  onGetTodos,
  onGetFirstTodos,
  onGetNotesOf,
  onGetTodosWithNotes,
  onAddTodo,
  onAddNote,
  onAddTodoAndNote,
  onAddTodoThenRollBack,
  onPrepareWriteOutsideRequest,
}

import { getDatabase, type Orm } from './databases'
import { onServerAction } from '../../server-actions'

async function onGetTodos(orm: Orm) {
  return await (await getDatabase(orm)).getTodos()
}

async function onGetFirstTodos(orm: Orm) {
  return await (await getDatabase(orm)).getFirstTodos()
}

async function onGetNotesOf(orm: Orm, todoId: number) {
  return await (await getDatabase(orm)).getNotesOf(todoId)
}

async function onGetTodosWithNotes(orm: Orm) {
  return await (await getDatabase(orm)).getTodosWithNotes()
}

async function onAddTodo(orm: Orm, text: string) {
  await (await getDatabase(orm)).addTodo(text)
}

async function onAddNote(orm: Orm, todoId: number, text: string) {
  await (await getDatabase(orm)).addNote(todoId, text)
}

async function onAddTodoAndNote(orm: Orm, text: string) {
  await (await getDatabase(orm)).addTodoAndNote(text)
}

async function onAddTodoThenRollBack(orm: Orm, text: string) {
  await (await getDatabase(orm)).addTodoThenRollBack(text)
}

/** The test then runs the write from a plain HTTP route, as a cron job would: outside any telefunction. */
async function onPrepareWriteOutsideRequest(orm: Orm, text: string) {
  const database = await getDatabase(orm)
  onServerAction(`live-orm:${orm}`, () => void database.addNote(1, text))
}
