export { onJoinChat }

import { BroadcastChannel } from 'telefunc'

type ChatMessage = { user: string; text: string; ts: number }

const encoder = new TextEncoder()
// Binary greetings are encoded into one buffer, reused for each.
const greeting = new Uint8Array(256)

async function onJoinChat(username: string) {
  const chat = new BroadcastChannel<ChatMessage>({ key: 'chat:lobby' })

  chat.onOpen(() => {
    chat.publish({ user: 'system', text: `${username} joined`, ts: Date.now() })
    for (const text of [`welcome ${username}`, `${username} is online`]) {
      const { written } = encoder.encodeInto(text, greeting)
      chat.publishBinary(greeting.subarray(0, written))
    }
  })

  chat.onClose(() => {
    chat.publish({ user: 'system', text: `${username} left`, ts: Date.now() })
  })

  return chat
}
