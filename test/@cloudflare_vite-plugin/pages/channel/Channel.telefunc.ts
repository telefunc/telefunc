export { onChannelInit }

import { Channel } from 'telefunc'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' } | { type: 'echo'; text: string }
type ClientToServer = (msg: ClientMessage) => void
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

  chat.listen((msg) => {
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
    serverTime: Date.now(),
  }
}
