export { Page }

import React from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { createLiveQuery } from '@telefunc/tanstack-query'
import { TodoList } from './TodoList'

const queryClient = new QueryClient()
// One live-query adapter bound to this client (holds the invalidation scheduler + cache-teardown watch).
const liveQuery = createLiveQuery(queryClient)

function Page() {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="max-w-3xl mx-auto px-8 py-10">
        <h1>Live Query</h1>
        <p className="mb-4 text-sm text-zinc-500">
          Local todos invalidate on this tab only. Global todos invalidate across all connected tabs.
        </p>
        <TodoList liveQuery={liveQuery} />
      </div>
    </QueryClientProvider>
  )
}
