export { onBacklog }

import { Channel } from 'telefunc'
import { cleanupState } from '../../cleanup-state'

type Run = { count: number; awaited: boolean }

/** On each run the page asks for, the server sends `count` numbered 16 KiB messages; cleanupState gets how many went
 *  out, and what a send that closed the channel rejected with, if one did. */
async function onBacklog() {
  const channel = new Channel<Run, string>()
  channel.listen(async ({ count, awaited }) => {
    let sent = 0
    let refused: string | null = null
    while (sent < count) {
      const send = channel.send(String(sent).padEnd(16 * 1024))
      if (channel.isClosed) {
        refused = await send.then(
          () => null,
          (err: Error) => err.name,
        )
        break
      }
      sent++
      if (awaited) await send
    }
    cleanupState[`backlog_${channel.id}`] = JSON.stringify({ sent, refused })
  })
  return { channel: channel.client, channelId: channel.id }
}
