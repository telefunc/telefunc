export { onBacklog }

import { env } from 'cloudflare:workers'
import { Channel } from 'telefunc'

/** The page asks for `count` numbered 16 KiB messages, sent nobody awaiting them; the ack answers the refusals.
 *  `/backlog-run` reports the run to a test, which stalls the page between `started` and the sends. */
async function onBacklog() {
  const channel = new Channel<(count: number) => string[], string>()
  channel.listen(async (count) => {
    const run = `backlog:${channel.id}`
    await env.TelefuncKV.put(run, JSON.stringify({ started: true }))
    // The sends go in one turn: they wait until the test reports the page's link stalled.
    while ((await env.TelefuncKV.get(`${run}:stalled`)) === null)
      await new Promise((resolve) => setTimeout(resolve, 50))
    const refused: string[] = []
    for (let n = 0; n < count; n++) {
      channel.send(String(n).padEnd(16 * 1024)).catch((err: Error) => refused.push(err.name))
    }
    // The refusals settle before a macrotask.
    await new Promise((resolve) => setTimeout(resolve, 0))
    await env.TelefuncKV.put(run, JSON.stringify({ sent: count, refused }))
    return refused
  })
  return channel.client
}
