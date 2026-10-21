export { onBacklog }

import { Channel } from 'telefunc'
import { cleanupState } from '../../cleanup-state'

type Run = { count: number; awaited: boolean }

/** On each run the page asks for, the server sends `count` numbered 16 KiB messages; cleanupState gets what it refused. */
async function onBacklog() {
  const channel = new Channel<Run, string>()
  channel.listen(async ({ count, awaited }) => {
    const refused: string[] = []
    for (let n = 0; n < count; n++) {
      const sent = channel.send(String(n).padEnd(16 * 1024))
      if (awaited) await sent
      else sent.catch((err: Error) => refused.push(err.name))
    }
    // The refusals of sends nobody awaits settle before a macrotask.
    await new Promise((resolve) => setTimeout(resolve, 0))
    cleanupState[`backlog_${channel.id}`] = JSON.stringify({ sent: count, refused })
  })
  return { channel: channel.client, channelId: channel.id }
}
