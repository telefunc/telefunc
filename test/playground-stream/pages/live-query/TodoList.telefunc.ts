export { onGetLocalTodos, onAddLocalTodo, onClearLocalTodos, onGetGlobalTodos, onAddGlobalTodo, onClearGlobalTodos }

import IORedis from 'ioredis'

type Todo = { id: string; text: string }
type State = { todos: Todo[]; nextId: number }

// In-memory fallback for `pnpm dev`; clustered docker setup uses Redis so all instances
// see the same list.
const redis = process.env.REDIS_URL ? new IORedis(process.env.REDIS_URL) : null
const LOCAL_KEY = 'playground:live-query:local'
const GLOBAL_KEY = 'playground:live-query:global'

const localFallback: State = { todos: [{ id: '1', text: 'Local todo 1' }], nextId: 2 }
const globalFallback: State = { todos: [{ id: '1', text: 'Global todo 1' }], nextId: 2 }

async function read(key: string, fallback: State): Promise<State> {
  if (!redis) return fallback
  const raw = await redis.get(key)
  if (raw === null) {
    const initial: State = { todos: [...fallback.todos], nextId: fallback.nextId }
    await redis.set(key, JSON.stringify(initial))
    return initial
  }
  return JSON.parse(raw) as State
}

async function write(key: string, fallback: State, state: State): Promise<void> {
  if (!redis) {
    fallback.todos = state.todos
    fallback.nextId = state.nextId
    return
  }
  await redis.set(key, JSON.stringify(state))
}

async function onGetLocalTodos() {
  return (await read(LOCAL_KEY, localFallback)).todos
}

async function onAddLocalTodo(text: string) {
  const state = await read(LOCAL_KEY, localFallback)
  state.todos.push({ id: String(state.nextId++), text })
  await write(LOCAL_KEY, localFallback, state)
}

async function onClearLocalTodos() {
  await write(LOCAL_KEY, localFallback, { todos: [], nextId: 1 })
}

async function onGetGlobalTodos() {
  return (await read(GLOBAL_KEY, globalFallback)).todos
}

async function onAddGlobalTodo(text: string) {
  const state = await read(GLOBAL_KEY, globalFallback)
  state.todos.push({ id: String(state.nextId++), text })
  await write(GLOBAL_KEY, globalFallback, state)
}

async function onClearGlobalTodos() {
  await write(GLOBAL_KEY, globalFallback, { todos: [], nextId: 1 })
}
