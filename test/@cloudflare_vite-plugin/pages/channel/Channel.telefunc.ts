export { onChannelInit }

import { Channel, getContext } from 'telefunc'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' | 'context' } | { type: 'echo'; text: string }
type ClientToServer = (msg: ClientMessage) => Promise<string | undefined>
type ServerToClient = (msg: ServerMessage) => void

async function onChannelInit() {
  const chat = new Channel<ClientToServer, ServerToClient>()
  chat.onClose(() => {
    clearInterval(intervalId)
    console.log('[server] channel closed')
  })
  chat.onOpen(() => {
    console.log('[server] channel opened')
  })
  let count = 0
  const intervalId = setInterval(() => {
    count++
    chat.send({ type: 'tick', count })
  }, 1000)

  chat.listen(async (msg) => {
    if (msg.type === 'context') {
      await Promise.resolve()
      const { user, onClose, signal } = getContext<{ user?: string }>()
      return typeof onClose === 'function' && signal instanceof AbortSignal ? (user ?? 'unscoped') : 'missing lifecycle'
    }
    console.log('[server] received:', msg)
    if (msg.type === 'echo') {
      chat.send({ type: 'echo', text: msg.text })
    }
    if (msg.type === 'ping') {
      chat.send({ type: 'welcome' })
    }
  })

  return {
    channel: chat.client,
    openingUser: getContext<{ user?: string }>().user,
    serverTime: Date.now(),
  }
}
