export { Page }

import React from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { withTelefunc } from '@telefunc/tanstack-query'
import { TodoList } from './TodoList'

const queryClient = withTelefunc(new QueryClient())

function Page() {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="max-w-3xl mx-auto px-8 py-10">
        <h1>Live Query</h1>
        <p className="mb-4 text-sm text-zinc-500">
          Local todos invalidate on this tab only. Global todos invalidate across all connected tabs.
        </p>
        <TodoList />
      </div>
    </QueryClientProvider>
  )
}
