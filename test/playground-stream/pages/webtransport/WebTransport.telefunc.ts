export { onWtTwoChannels, onWtLossy }

import { BroadcastChannel, Channel } from 'telefunc'

/** Two channels: `stalled` sends 64 KiB messages, each awaited, which the page never finishes reading, and `ticks` a
 *  number every 20 ms. */
async function onWtTwoChannels() {
  const stalled = new Channel<never, string>()
  const ticks = new Channel<never, number>()
  void sendUntilClosed(stalled)
  let tick = 0
  const timer = setInterval(() => {
    if (ticks.isClosed) clearInterval(timer)
    else void ticks.send(tick++)
  }, 20)
  return { stalled: stalled.client, ticks: ticks.client }
}

async function sendUntilClosed(channel: Channel<never, string>) {
  const message = 'x'.repeat(64 * 1024)
  while (!channel.isClosed) await channel.send(message).catch(() => {})
}

/** Once the page asks, `count` publishes to a lossy broadcast as fast as the server makes them, each its ordinal. */
async function onWtLossy(count: number) {
  const broadcast = new BroadcastChannel<number>({ key: `wt-lossy:${crypto.randomUUID()}`, lossy: true })
  const control = new Channel<(msg: 'start') => string, never>()
  control.listen(() => {
    for (let i = 0; i < count; i++) void broadcast.publish(i)
    return 'started'
  })
  return { broadcast, control: control.client }
}
