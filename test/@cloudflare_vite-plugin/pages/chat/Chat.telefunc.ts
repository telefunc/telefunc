export { onJoinChat }

import { pubsub } from 'telefunc'

type ChatMessage = { user: string; text: string; ts: number }

async function onJoinChat(username: string) {
  const ps = pubsub<ChatMessage>('chat:lobby')

  ps.onOpen(() => {
    ps.publish({ user: 'system', text: `${username} joined`, ts: Date.now() })
  })

  ps.onClose(() => {
    ps.publish({ user: 'system', text: `${username} left`, ts: Date.now() })
  })

  return ps
}
