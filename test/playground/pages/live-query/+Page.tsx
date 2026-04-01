export { Page }

import React from 'react'
import { QueryClientProvider } from '@tanstack/react-query'
import { QueryClient } from '@telefunc/tanstack-query'
import { TodoList } from './TodoList'

const queryClient = new QueryClient()

function Page() {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="max-w-3xl mx-auto px-8 py-10">
        <h1>Live Query</h1>
        <p className="mb-4 text-sm text-zinc-500">
          Open this page in two tabs. Adding or removing todos in one tab should update the other in real time.
        </p>
        <TodoList />
      </div>
    </QueryClientProvider>
  )
}
