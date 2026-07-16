export { TodoList }

import React, { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { createLiveQuery } from '@telefunc/tanstack-query'
import {
  onGetLocalTodos,
  onAddLocalTodo,
  onClearLocalTodos,
  onGetGlobalTodos,
  onAddGlobalTodo,
  onClearGlobalTodos,
} from './TodoList.telefunc'

type LiveQuery = ReturnType<typeof createLiveQuery>

function TodoList({ liveQuery }: { liveQuery: LiveQuery }) {
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div className="grid grid-cols-2 gap-8">
      {hydrated && <span id="hydrated" />}
      <LocalTodos />
      <GlobalTodos liveQuery={liveQuery} />
    </div>
  )
}

function LocalTodos() {
  const [text, setText] = useState('')
  const queryClient = useQueryClient()

  const { data: todos, isLoading } = useQuery({
    queryKey: ['local-todos'],
    queryFn: () => onGetLocalTodos(),
    staleTime: Infinity,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
  })

  // Local liveness is purely client-side (the local column never crosses the wire live): refetch this
  // tab's query on mutation success. No server `Live` primitive — plain TanStack invalidation.
  const invalidateLocal = () => queryClient.invalidateQueries({ queryKey: ['local-todos'] })

  const addTodo = useMutation({
    mutationFn: (text: string) => onAddLocalTodo(text),
    onSuccess: invalidateLocal,
  })

  const clearTodos = useMutation({
    mutationFn: () => onClearLocalTodos(),
    onSuccess: invalidateLocal,
  })

  return (
    <div>
      <h2>Local Todos</h2>
      <p className="text-xs text-zinc-400 mb-2">Invalidation stays on this tab only</p>
      <div className="flex gap-2 mb-4">
        <input
          id="local-todo-input"
          type="text"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && text.trim()) {
              addTodo.mutate(text.trim())
              setText('')
            }
          }}
          placeholder="Add a local todo..."
          className="border border-zinc-300 rounded px-3 py-1.5 text-sm flex-1"
        />
        <button
          id="local-todo-add"
          onClick={() => {
            if (text.trim()) {
              addTodo.mutate(text.trim())
              setText('')
            }
          }}
        >
          Add
        </button>
        <button onClick={() => clearTodos.mutate()}>Clear</button>
      </div>
      <ul id="local-todo-list" className="space-y-1">
        {isLoading ? (
          <li>Loading...</li>
        ) : todos?.length === 0 ? (
          <li className="text-zinc-400 text-sm">No todos yet.</li>
        ) : (
          todos?.map((todo) => <li key={todo.id}>{todo.text}</li>)
        )}
      </ul>
    </div>
  )
}

function GlobalTodos({ liveQuery }: { liveQuery: LiveQuery }) {
  const [text, setText] = useState('')

  // Server-owned liveness: the telefunction returns a `ClientLive<Todo[]>`; `liveQuery` unwraps its
  // data and refetches whenever the server invalidates the global tag. No client-side invalidation.
  const { data: todos, isLoading } = useQuery(
    liveQuery({
      queryKey: ['global-todos'],
      queryFn: () => onGetGlobalTodos(),
      staleTime: Infinity,
      refetchOnMount: false,
      refetchOnWindowFocus: false,
    }),
  )

  // No client-side invalidation: the server calls `Live.invalidate(GLOBAL_TAG)`, which reaches every
  // connected client through its live channel.
  const addTodo = useMutation({
    mutationFn: (text: string) => onAddGlobalTodo(text),
  })

  const clearTodos = useMutation({
    mutationFn: () => onClearGlobalTodos(),
  })

  return (
    <div>
      <h2>Global Todos</h2>
      <p className="text-xs text-zinc-400 mb-2">Invalidation reaches all connected tabs</p>
      <div className="flex gap-2 mb-4">
        <input
          id="global-todo-input"
          type="text"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && text.trim()) {
              addTodo.mutate(text.trim())
              setText('')
            }
          }}
          placeholder="Add a global todo..."
          className="border border-zinc-300 rounded px-3 py-1.5 text-sm flex-1"
        />
        <button
          id="global-todo-add"
          onClick={() => {
            if (text.trim()) {
              addTodo.mutate(text.trim())
              setText('')
            }
          }}
        >
          Add
        </button>
        <button onClick={() => clearTodos.mutate()}>Clear</button>
      </div>
      <ul id="global-todo-list" className="space-y-1">
        {isLoading ? (
          <li>Loading...</li>
        ) : todos?.length === 0 ? (
          <li className="text-zinc-400 text-sm">No todos yet.</li>
        ) : (
          todos?.map((todo) => <li key={todo.id}>{todo.text}</li>)
        )}
      </ul>
    </div>
  )
}
