export { onJoinChat }

import { createChannel } from 'telefunc'

type ChatMessage = { user: string; text: string; ts: number }

async function onJoinChat(username: string) {
  const channel = createChannel<(msg: ChatMessage) => void, (msg: ChatMessage) => void>({ key: 'chat:lobby' })

  channel.onOpen(() => {
    channel.publish({ user: 'system', text: `${username} joined`, ts: Date.now() })
  })

  channel.onClose(() => {
    channel.publish({ user: 'system', text: `${username} left`, ts: Date.now() })
  })

  return { channel: channel.client }
}
