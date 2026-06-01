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

// wrangler's auto-generated Env types the namespace as `DurableObjectNamespace` without a
// generic when `main` is a virtual module (`vike:server-entry`), so RPC methods aren't
// surfaced. Cast once here to recover the typed stub.
async function getTodoItemsData(): Promise<DurableObjectStub<TodoListDurableObject>> {
  const namespace = env.TO_DO_LIST_DURABLE_OBJECTS as DurableObjectNamespace<TodoListDurableObject>
  return namespace.getByName('vike-todo-list-demo')
}

class TodoListDurableObject extends DurableObject {
  async getTodoItems() {
    let todoItems = (await this.ctx.storage.get('all-todo-items')) as TodoItem[]
    todoItems ??= Array.from(todoItemsInit)
    return todoItems
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
