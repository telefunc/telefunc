export { onJoinChat }

import { BroadcastChannel } from 'telefunc'

type ChatMessage = { user: string; text: string; ts: number }

async function onJoinChat(username: string) {
  const chat = new BroadcastChannel<ChatMessage>({ key: 'chat:lobby' })

  chat.onOpen(() => {
    chat.publish({ user: 'system', text: `${username} joined`, ts: Date.now() })
  })

  chat.onClose(() => {
    chat.publish({ user: 'system', text: `${username} left`, ts: Date.now() })
  })

  return chat
}
