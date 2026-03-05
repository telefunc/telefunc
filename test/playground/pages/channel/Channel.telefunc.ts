export { onChannelInit }

import { createChannel } from 'telefunc'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' } | { type: 'echo'; text: string }

async function onChannelInit() {
  const channel = createChannel<ServerMessage, ClientMessage>()
  channel.onClose(() => {
    clearInterval(intervalId)
    console.log('[server] channel closed')
  })
  let count = 0
  const intervalId = setInterval(() => {
    count++
    channel.send({ type: 'tick', count })
  }, 1000)

  channel.listen((msg) => {
    console.log('[server] received:', msg)
    if (msg.type === 'echo') {
      channel.send({ type: 'echo', text: msg.text })
    }
    if (msg.type === 'ping') {
      channel.send({ type: 'welcome' })
    }
  })

  return {
    channel: channel.client,
    serverTime: Date.now(),
  }
}
