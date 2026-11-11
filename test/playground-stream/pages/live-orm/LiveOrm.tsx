export { LiveOrm }

import React, { useEffect, useState } from 'react'
import { useMutation, useQueryClient, useSuspenseQuery, type QueryFunction } from '@tanstack/react-query'
import { watch } from '@telefunc/live/client'
import { live } from '@telefunc/live/tanstack-query'
import { withFallback } from 'vike-react-query'
import type { Orm } from './databases'
import {
  onGetTodos,
  onGetFirstTodos,
  onGetNotesOf,
  onGetTodosWithNotes,
  onAddTodo,
  onAddNote,
  onAddTodoAndNote,
  onAddTodoThenRollBack,
  onPrepareWriteOutsideRequest,
} from './LiveOrm.telefunc'

function LiveOrm({ orm }: { orm: Orm }) {
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])

  return (
    <div className="max-w-4xl mx-auto px-8 py-10">
      <h1>Live ORM: {orm}</h1>
      <p className="mb-4 text-sm text-zinc-500">
        Each list refetches when another tab or a server job writes rows it shows, and only then.
      </p>
      {hydrated && <span id="hydrated" />}
      <Writes orm={orm} />
      <div className="grid grid-cols-3 gap-8">
        <LiveList id="todos" queryKey={['live-orm-todos', orm]} queryFn={live(onGetTodos, orm)} />
        <LiveList id="first-todos" queryKey={['live-orm-first-todos', orm]} queryFn={live(onGetFirstTodos, orm)} />
        <LiveList
          id="todos-with-notes"
          queryKey={['live-orm-todos-with-notes', orm]}
          queryFn={live(onGetTodosWithNotes, orm)}
        />
        <LiveList id="notes-of-1" queryKey={['live-orm-notes-of', orm, 1]} queryFn={live(onGetNotesOf, orm, 1)} />
        <LiveList id="notes-of-2" queryKey={['live-orm-notes-of', orm, 2]} queryFn={live(onGetNotesOf, orm, 2)} />
        {hydrated && <WatchedList orm={orm} />}
      </div>
    </div>
  )
}

const LiveList = withFallback(
  ({ id, queryKey, queryFn }: { id: string; queryKey: unknown[]; queryFn: QueryFunction<string[]> }) => {
    // Reading `dataUpdatedAt` re-renders on each fetch, also one returning the same rows. `staleTime: Infinity`: only
    // the server says when the rows are stale; the browser fetches a server-rendered list once, to watch it.
    const { data, dataUpdatedAt } = useSuspenseQuery({ queryKey, queryFn, staleTime: Infinity })
    // Completed fetches, the server's included: a test proves a write did not refetch this list.
    const fetches = useQueryClient().getQueryState(queryKey)?.dataUpdateCount
    return <List id={id} fetches={fetches} items={data} updatedAt={dataUpdatedAt} />
  },
)

/** The notes of the first todo, kept fresh by `watch()` alone: no data library. */
function WatchedList({ orm }: { orm: Orm }) {
  const [state, setState] = useState({ items: [] as string[], fetches: 0 })
  useEffect(() => watch(onGetNotesOf, [orm, 1], (items) => setState((s) => ({ items, fetches: s.fetches + 1 }))), [orm])
  return <List id="watched-notes-of-1" fetches={state.fetches} items={state.items} />
}

function List({
  id,
  fetches,
  items,
  updatedAt,
}: { id: string; fetches: number | undefined; items: string[]; updatedAt?: number }) {
  return (
    <section data-updated-at={updatedAt}>
      <h2>{id}</h2>
      <p className="text-xs text-zinc-400">
        Fetches: <span id={`${id}-fetches`}>{fetches}</span>
      </p>
      <ul id={`${id}-list`} className="space-y-1">
        {items.map((text, i) => (
          <li key={i}>{text}</li>
        ))}
      </ul>
    </section>
  )
}

function Writes({ orm }: { orm: Orm }) {
  const [text, setText] = useState('')
  const writes = {
    'add-todo': useMutation({ mutationFn: () => onAddTodo(orm, text) }),
    'add-note-to-1': useMutation({ mutationFn: () => onAddNote(orm, 1, text) }),
    'add-note-to-2': useMutation({ mutationFn: () => onAddNote(orm, 2, text) }),
    'add-todo-and-note': useMutation({ mutationFn: () => onAddTodoAndNote(orm, text) }),
    'add-todo-then-roll-back': useMutation({ mutationFn: () => onAddTodoThenRollBack(orm, text) }),
    'prepare-write-outside-request': useMutation({ mutationFn: () => onPrepareWriteOutsideRequest(orm, text) }),
  }
  return (
    <div className="flex flex-wrap gap-2 mb-6">
      <input
        id="text"
        type="text"
        value={text}
        onChange={(e) => setText(e.target.value)}
        className="border border-zinc-300 rounded px-3 py-1.5 text-sm"
      />
      {Object.entries(writes).map(([id, write]) => (
        <button key={id} id={id} onClick={() => write.mutate()}>
          {id} {write.isSuccess && <span id={`${id}-done`}>✓</span>}
        </button>
      ))}
    </div>
  )
}
