export { getTodoItems }
export { addTodoItem }
export { resetTodoItems }
export { TodoListDurableObject }
export type { TodoItem }

import { DurableObject, env } from 'cloudflare:workers'
const todoItemsInit: readonly TodoItem[] = Object.freeze([{ text: 'Buy milk' }, { text: 'Buy strawberries' }])

type TodoItem = { text: string }

async function getTodoItems(): Promise<TodoItem[]> {
  const todoItemsData = await getTodoItemsData()
  const todoItems = await todoItemsData.getTodoItems()
  return todoItems
}

async function addTodoItem(todoItem: TodoItem): Promise<TodoItem[]> {
  const todoItemsData = await getTodoItemsData()
  const todoItems = await todoItemsData.addTodoItem(todoItem)
  return todoItems
}

async function resetTodoItems() {
  const todoItemsData = await getTodoItemsData()
  await todoItemsData.resetTodoItems()
  const todoItems = await todoItemsData.getTodoItems()
  return todoItems
}

// The namespace is typed with its class in `env.d.ts`.
async function getTodoItemsData(): Promise<DurableObjectStub<TodoListDurableObject>> {
  return env.TO_DO_LIST_DURABLE_OBJECTS.getByName('vike-todo-list-demo')
}

class TodoListDurableObject extends DurableObject {
  async getTodoItems() {
    const stored: unknown = await this.ctx.storage.get('all-todo-items')
    return stored === undefined || stored === null ? Array.from(todoItemsInit) : readTodoItems(stored)
  }
  async addTodoItem(todoItem: TodoItem) {
    const todoItems = await this.getTodoItems()
    todoItems.push(todoItem)
    await this.ctx.storage.put('all-todo-items', todoItems)
    return todoItems
  }
  async resetTodoItems() {
    await this.ctx.storage.put('all-todo-items', null)
  }
}

/** The todo items as `TodoListDurableObject` stores them. */
function readTodoItems(stored: unknown): TodoItem[] {
  if (!Array.isArray(stored)) throw new Error('Test: the stored todo items are no list')
  const items: unknown[] = stored
  return items.map((item) => {
    if (typeof item !== 'object' || item === null || !('text' in item) || typeof item.text !== 'string')
      throw new Error('Test: a stored todo item has no text')
    return { text: item.text }
  })
}
