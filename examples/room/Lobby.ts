import { onGetLobby } from './Lobby.telefunc'

const form = document.querySelector<HTMLFormElement>('#send')!
const item = (text: string) => Object.assign(document.createElement('li'), { textContent: text })

const room = await onGetLobby()
room.onChange(() => {
  const names = room.snapshot().participants.map((member) => item(member.meta.name))
  document.querySelector('#members')!.replaceChildren(...names)
})
room.subscribe((message, _info, from) => {
  document.querySelector('#messages')!.append(item(`${from.meta.name}: ${message.text}`))
})

const me = await room.join({ meta: { name: `Guest ${crypto.randomUUID().slice(0, 4)}` } })
form.addEventListener('submit', async (event) => {
  event.preventDefault()
  await me.publish({ text: form.text.value })
  form.reset()
})
