export { TodoList }

import React, { useEffect, useState } from 'react'
import { useData } from 'vike-react/useData'
import { onNewTodo, onReset } from './TodoList.telefunc'
import type { Data } from './+data.js'

function TodoList() {
  const data = useData<Data>()
  const [todoItems, setTodoItems] = useState(data.todoItemsInitial)
  const [draft, setDraft] = useState('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  return (
    <div id={hydrated ? 'hydrated' : undefined}>
      <ul>
        {todoItems.map((todoItem, i) => (
          <li key={i}>{todoItem.text}</li>
        ))}
        <li>
          <form
            onSubmit={async (ev) => {
              ev.preventDefault()
              const { todoItems } = await onNewTodo({ text: draft })
              setDraft('')
              setTodoItems(todoItems)
            }}
          >
            <input type="text" onChange={(ev) => setDraft(ev.target.value)} value={draft} autoFocus={true} />{' '}
            <button type="submit">Add to-do</button>
          </form>
        </li>
      </ul>
      <button
        onClick={async () => {
          const { todoItems } = await onReset()
          setTodoItems(todoItems)
        }}
      >
        Reset
      </button>
    </div>
  )
}
