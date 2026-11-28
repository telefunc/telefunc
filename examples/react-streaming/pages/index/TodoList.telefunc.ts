export { onNewTodo }
export { onLoad }
export { onProfile }

import { getContext } from 'telefunc'

const database: {
  todoItems: { text: string }[]
} = {
  todoItems: [],
}

async function onLoad() {
  const { todoItems } = database
  // Simulate slow network
  await sleep(3 * 1000)
  return todoItems
}

async function onProfile() {
  await Promise.resolve()
  return getContext<{ name: string }>().name
}

async function onNewTodo({ text }) {
  database.todoItems.push({ text })
  const { todoItems } = database
  return { todoItems }
}

// Initial data
database.todoItems.push({ text: 'Buy milk' })
database.todoItems.push({ text: 'Buy strawberries' })

function sleep(milliseconds: number) {
  return new Promise((r) => setTimeout(r, milliseconds))
}
