export { onBacklog }

import { Channel } from 'telefunc'

/** The page asks for `count` numbered 16 KiB messages, sent nobody awaiting them; the ack answers the refusals. */
async function onBacklog() {
  const channel = new Channel<(count: number) => string[], string>()
  channel.listen(async (count) => {
    const refused: string[] = []
    for (let n = 0; n < count; n++) {
      channel.send(String(n).padEnd(16 * 1024)).catch((err: Error) => refused.push(err.name))
    }
    // The refusals settle before a macrotask.
    await new Promise((resolve) => setTimeout(resolve, 0))
    return refused
  })
  return channel.client
}
