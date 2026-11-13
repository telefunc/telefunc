import { watch } from '@telefunc/live/client'
import { onAddTodo, onNotes, onSetup, onShard, onTodos } from './todos.telefunc'

const [shard] = await Promise.all([onShard(), onSetup()])
document.querySelector('#shard')!.textContent = shard
show('#todos', onTodos)
show('#notes', onNotes)
document.querySelector('#add')!.addEventListener('click', () => {
  void onAddTodo(document.querySelector<HTMLInputElement>('#text')!.value)
})

/** Shows the rows in the element `id`, with how often they were fetched. */
function show(id: string, telefunction: typeof onTodos) {
  const list = document.querySelector<HTMLElement>(id)!
  let fetches = 0
  watch(telefunction, [], (rows) => {
    list.textContent = rows.map((row) => row.text).join(', ')
    list.dataset.fetches = String(++fetches)
  })
}
