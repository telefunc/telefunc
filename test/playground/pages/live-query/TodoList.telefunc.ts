export { onGetLocalTodos, onAddLocalTodo, onClearLocalTodos, onGetGlobalTodos, onAddGlobalTodo, onClearGlobalTodos }

type Todo = { id: string; text: string }

// --- Local todos (per-client, in-memory per request context) ---

let localTodos: Todo[] = [{ id: '1', text: 'Local todo 1' }]
let localNextId = 2

async function onGetLocalTodos() {
  return localTodos
}

async function onAddLocalTodo(text: string) {
  localTodos.push({ id: String(localNextId++), text })
}

async function onClearLocalTodos() {
  localTodos = []
  localNextId = 1
}

// --- Global todos (shared across all clients) ---

let globalTodos: Todo[] = [{ id: '1', text: 'Global todo 1' }]
let globalNextId = 2

async function onGetGlobalTodos() {
  return globalTodos
}

async function onAddGlobalTodo(text: string) {
  globalTodos.push({ id: String(globalNextId++), text })
}

async function onClearGlobalTodos() {
  globalTodos = []
  globalNextId = 1
}
