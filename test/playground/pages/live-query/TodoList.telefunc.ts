export { onGetTodos, onAddTodo, onRemoveTodo, onClearTodos }

import { invalidate } from '@telefunc/tanstack-query/server'

type Todo = { id: string; text: string }

let todos: Todo[] = [
  { id: '1', text: 'Buy milk' },
  { id: '2', text: 'Walk the dog' },
]
let nextId = 3

async function onGetTodos() {
  return todos
}

async function onAddTodo(text: string) {
  todos.push({ id: String(nextId++), text })
  invalidate(['todos'])
}

async function onRemoveTodo(id: string) {
  todos = todos.filter((t) => t.id !== id)
  invalidate(['todos'])
}

async function onClearTodos() {
  todos = []
  invalidate(['todos'])
}
