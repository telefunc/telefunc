export { onChannelInit }

import { channel } from 'telefunc'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' } | { type: 'echo'; text: string }
type ClientToServer = (msg: ClientMessage) => void
type ServerToClient = (msg: ServerMessage) => void

async function onChannelInit() {
  const ch = channel<ClientToServer, ServerToClient>()
  ch.onClose(() => {
    clearInterval(intervalId)
    console.log('[server] channel closed')
  })
  ch.onOpen(() => {
    console.log('[server] channel opened')
  })
  let count = 0
  const intervalId = setInterval(() => {
    count++
    ch.send({ type: 'tick', count })
  }, 1000)

  ch.listen((msg) => {
    console.log('[server] received:', msg)
    if (msg.type === 'echo') {
      ch.send({ type: 'echo', text: msg.text })
    }
    if (msg.type === 'ping') {
      ch.send({ type: 'welcome' })
    }
  })

  return {
    channel: ch.client,
    serverTime: Date.now(),
  }
}
