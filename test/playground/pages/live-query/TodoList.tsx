export { TodoList }

import React, { useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { onGetTodos, onAddTodo, onRemoveTodo, onClearTodos } from './TodoList.telefunc'

function TodoList() {
  const [text, setText] = useState('')

  const { data: todos, isLoading } = useQuery({
    queryKey: ['todos'],
    queryFn: () => onGetTodos(),
  })

  const addTodo = useMutation({
    mutationFn: (text: string) => onAddTodo(text),
    meta: { invalidates: [['todos']] },
  })

  const removeTodo = useMutation({
    mutationFn: (id: string) => onRemoveTodo(id),
    meta: { invalidates: [['todos']] },
  })

  const clearTodos = useMutation({
    mutationFn: () => onClearTodos(),
    meta: { invalidates: [['todos']] },
  })

  return (
    <div>
      <div className="flex gap-2 mb-4">
        <input
          type="text"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && text.trim()) {
              addTodo.mutate(text.trim())
              setText('')
            }
          }}
          placeholder="Add a todo..."
          className="border border-zinc-300 rounded px-3 py-1.5 text-sm flex-1"
        />
        <button
          onClick={() => {
            if (text.trim()) {
              addTodo.mutate(text.trim())
              setText('')
            }
          }}
        >
          Add
        </button>
        <button onClick={() => clearTodos.mutate()}>Clear all</button>
      </div>

      {isLoading ? (
        <p>Loading...</p>
      ) : (
        <ul className="space-y-1">
          {todos?.map((todo) => (
            <li key={todo.id} className="flex items-center gap-2">
              <span className="flex-1">{todo.text}</span>
              <button onClick={() => removeTodo.mutate(todo.id)} className="text-xs text-red-500">
                Remove
              </button>
            </li>
          ))}
          {todos?.length === 0 && <li className="text-zinc-400 text-sm">No todos yet.</li>}
        </ul>
      )}
    </div>
  )
}
