export { onChannelInit }

import { createChannel } from 'telefunc'

type ServerMessage = { type: 'tick'; count: number } | { type: 'echo'; text: string } | { type: 'welcome' }
type ClientMessage = { type: 'ping' } | { type: 'echo'; text: string }
type Ack = string

async function onChannelInit() {
  const channel = createChannel<ServerMessage, ClientMessage, Ack, Ack>({ ack: true })

  channel.onClose(() => {
    clearInterval(intervalId)
    console.log('[server] channel closed')
  })
  channel.onOpen(() => {
    console.log('[server] channel opened')
  })

  channel.listen((msg) => {
    console.log('[server] received:', msg)
    if (msg.type === 'echo') {
      channel.send({ type: 'echo', text: msg.text })
    }
    if (msg.type === 'ping') {
      channel.send({ type: 'welcome' })
    }
    // Return ack value to the client
    return `server-ack:${msg.type}`
  })

  let count = 0
  const intervalId = setInterval(async () => {
    count++
    const ack = await channel.send({ type: 'tick', count })
    console.log(`[server] tick #${count} acked by client:`, ack)
  }, 1000)

  return {
    channel: channel.client,
    serverTime: Date.now(),
  }
}
