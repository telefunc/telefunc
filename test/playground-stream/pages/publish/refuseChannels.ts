export { refuseChannels }

import { Broadcast } from 'telefunc'
import { redis } from '../../redis'

/** Leaves the keys that start with `!` out of this server's channel permissions, until `onRedisAllowsEveryKey()`, and
 *  resolves once Redis refuses them on the transport's own subscriber connection: Dragonfly applies an ACL change to a
 *  connection already open a moment after it answers. False where Redis still takes such a subscription after 5 s: the
 *  pattern doesn't match the channel names Telefunc subscribes to, which fails the cards that call this. */
async function refuseChannels(): Promise<boolean> {
  if (!redis) throw new Error('The playground runs without Redis')
  await redis.call('ACL', 'SETUSER', 'default', 'resetchannels', '&tf:?:{[^!]*')
  const deadline = Date.now() + 5_000
  while (true) {
    const key = `!refused:${crypto.randomUUID()}`
    const probe = Broadcast.subscribe(key, () => {})
    const refused = await probe.ready.then(
      () => false,
      () => true,
    )
    // Whether Redis itself took it: a `ready` that resolved for a subscription Redis doesn't hold is the bug the cards
    // look for, so they run on and show it.
    const reply = await redis.call('PUBSUB', 'NUMSUB', `tf:t:{${key}}`)
    probe()
    if (refused || !Array.isArray(reply) || Number(reply[1]) === 0) return true
    if (Date.now() > deadline) return false
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}
