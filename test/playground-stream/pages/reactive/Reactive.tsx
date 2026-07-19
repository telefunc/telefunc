export { Reactive }

import { QueryClient, QueryClientProvider, useQuery, useQueryClient } from '@tanstack/react-query'
import { live } from '@telefunc/drizzle-experimental/tanstack-query'
import React from 'react'
import { onAddCoarse, onAddTodo, onAddTx, onGetNotes, onGetTodos } from './Reactive.telefunc'

// The CONTROL has to be able to disagree. Every ambient refetch trigger is off — no mount, focus,
// reconnect or staleness refetch, no retries — so the ONLY thing that can refetch a query here is a live
// invalidation arriving from the server. Without this, "the control did not refetch" would prove nothing.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      refetchOnMount: false,
      staleTime: Number.POSITIVE_INFINITY,
      gcTime: Number.POSITIVE_INFINITY,
      retry: false,
    },
  },
})

const TODOS_KEY = ['reactive-todos']
const NOTES_KEY = ['reactive-notes']

/** Count COMPLETED FETCHES per query, not rows.
 *
 *  Row counts cannot serve as the precision control: nothing in this test writes to `notes`, so
 *  `notesCount` stays identical whether or not the notes query refetched — a control that can never
 *  disagree. A fetch counter distinguishes "was not invalidated" from "was invalidated and happened to
 *  read the same rows", which is exactly the over-invalidation this test exists to catch. */
function useFetchCounts() {
  const client = useQueryClient()
  const [counts, setCounts] = React.useState<Record<string, number>>({})
  React.useEffect(() => {
    return client.getQueryCache().subscribe((event) => {
      if (event.type !== 'updated' || event.action.type !== 'success') return
      const key = JSON.stringify(event.query.queryKey)
      setCounts((prev) => ({ ...prev, [key]: (prev[key] ?? 0) + 1 }))
    })
  }, [client])
  return (key: readonly unknown[]) => counts[JSON.stringify(key)] ?? 0
}

function Inner() {
  const [hydrated, setHydrated] = React.useState(false)
  React.useEffect(() => setHydrated(true), [])

  const fetchesOf = useFetchCounts()
  const todos = useQuery({ queryKey: TODOS_KEY, queryFn: live(onGetTodos) })
  // The SYNCHRONOUS-closure spelling, exercised end-to-end alongside the direct form above. It reaches the
  // telefunction inside the same per-call context window, so the query's abort signal still rides along —
  // the two spellings are equivalent, and this lane is where that stops being a unit-test claim.
  const notes = useQuery({ queryKey: NOTES_KEY, queryFn: live(() => onGetNotes()) })

  const state = {
    todosCount: Array.isArray(todos.data) ? todos.data.length : -1,
    notesCount: Array.isArray(notes.data) ? notes.data.length : -1,
    todosFetches: fetchesOf(TODOS_KEY),
    notesFetches: fetchesOf(NOTES_KEY),
  }
  const loaded = state.todosCount >= 0 && state.notesCount >= 0

  return (
    <div>
      <h1>Reactive</h1>
      <button id="add-todo" type="button" onClick={() => void onAddTodo('t')}>
        add todo on instance A
      </button>
      <button id="add-tx" type="button" onClick={() => void onAddTx()}>
        add tx (todos + notes) on instance A
      </button>
      <button id="add-coarse" type="button" onClick={() => void onAddCoarse()}>
        raw-SQL coarse write on instance A
      </button>
      <pre id="reactive-state">{JSON.stringify(state)}</pre>
      {/* `#hydrated` is the playground-wide marker `navigate()` waits for; `#reactive-ready` additionally
          says both live queries have delivered their first result. */}
      {hydrated && <span id="hydrated" />}
      {hydrated && loaded && <span id="reactive-ready" />}
    </div>
  )
}

function Reactive() {
  return (
    <QueryClientProvider client={queryClient}>
      <Inner />
    </QueryClientProvider>
  )
}
