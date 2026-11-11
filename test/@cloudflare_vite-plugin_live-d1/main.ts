import { watch } from '@telefunc/live/client'
import { onAddTodo, onSetup, onTodos } from './todos.telefunc'

await onSetup()
const list = document.querySelector<HTMLElement>('#todos')!
let fetches = 0
watch(onTodos, [], (rows) => {
  list.textContent = rows.map((row) => row.text).join(', ')
  list.dataset.fetches = String(++fetches)
})
document.querySelector('#add')!.addEventListener('click', () => {
  void onAddTodo(document.querySelector<HTMLInputElement>('#text')!.value)
})
