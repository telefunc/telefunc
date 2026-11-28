import { onGetLobby } from './Lobby.telefunc'

/** The element of index.html that `selector` names. */
function element<Type extends Element>(selector: string, type: { new (): Type; prototype: Type }): Type {
  const found = document.querySelector(selector)
  if (!(found instanceof type)) throw new Error(`index.html has no ${selector}`)
  return found
}

const form = element('#send', HTMLFormElement)
const item = (text: string) => Object.assign(document.createElement('li'), { textContent: text })

const room = await onGetLobby()
room.onChange(() => {
  const names = room.snapshot().participants.map((member) => item(member.meta.name))
  element('#members', HTMLElement).replaceChildren(...names)
})
room.subscribe((message, { from }) => {
  element('#messages', HTMLElement).append(item(from ? `${from.meta.name}: ${message.text}` : message.text))
})

const me = await room.join({ meta: { name: `Guest ${crypto.randomUUID().slice(0, 4)}` } })
form.addEventListener('submit', async (event) => {
  event.preventDefault()
  await me.publish({ text: form.text.value })
  form.reset()
})
