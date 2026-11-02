export { onBacklog }

import { env } from 'cloudflare:workers'
import { Channel } from 'telefunc'

/** The page asks for `count` numbered 16 KiB messages, sent nobody awaiting them; a send that closes the channel ends
 *  the run. `/backlog-run` reports the run to a test, which stalls the page between `started` and the sends. */
async function onBacklog() {
  const channel = new Channel<(count: number) => void, string>()
  channel.listen(async (count) => {
    const run = `backlog:${channel.id}`
    await env.TelefuncKV.put(run, JSON.stringify({ started: true }))
    // The sends go in one turn: they wait until the test reports the page's link stalled.
    while ((await env.TelefuncKV.get(`${run}:stalled`)) === null)
      await new Promise((resolve) => setTimeout(resolve, 50))
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
    }
    await env.TelefuncKV.put(run, JSON.stringify({ sent, refused }))
  })
  return channel.client
}
