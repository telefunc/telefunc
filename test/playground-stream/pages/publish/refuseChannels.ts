export { refuseChannels, allowEveryChannel }

import { Broadcast } from 'telefunc'
import type { Redis } from 'ioredis'
import { broadcastChannel } from '../../../../packages/redis/dist/keys.js'
import { redis } from '../../redis'

/** Leaves the keys that start with `!` out of this server's channel permissions, until `allowEveryChannel()`, and
 *  resolves once Redis refuses them on the transport's own subscriber connection: Dragonfly applies an ACL change to a
 *  connection already open a moment after it answers. False where Redis still takes such a subscription after 5 s: the
 *  pattern doesn't match the channel names Telefunc subscribes to, which fails the cards that call this. */
async function refuseChannels(): Promise<boolean> {
  if (!redis) throw new Error('The playground runs without Redis')
  // A key is its channel's one `{`, encoded, as in `tf:0:t:{room%3A1}`: those whose key starts with `!` are left out.
  await redis.call('ACL', 'SETUSER', 'default', 'resetchannels', '&tf:*{[^!]*')
  const deadline = Date.now() + 5_000
  while (true) {
    if (await refuses(redis, `!refused:${crypto.randomUUID()}`)) return true
    if (Date.now() > deadline) return false
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

/** Gives this server's channel permissions every key again. */
async function allowEveryChannel(): Promise<void> {
  await redis?.call('ACL', 'SETUSER', 'default', 'allchannels')
}

/** Whether Redis refuses a subscription to `key`: its `opened` rejects, or Redis itself doesn't hold it. An `opened` that
 *  resolved for a subscription Redis doesn't hold is the bug the cards look for, so they run on and show it. */
async function refuses(redis: Redis, key: string): Promise<boolean> {
  const probe = Broadcast.subscribe(key, () => {})
  try {
    const refused = await probe.opened.then(
      () => false,
      () => true,
    )
    // A standalone Redis's channel names carry its database (RedisConnection's prefix).
    const channel = broadcastChannel(`tf:${redis.options.db ?? 0}:`, { key, kind: 'text' })
    const reply = await redis.call('PUBSUB', 'NUMSUB', channel)
    return refused || !Array.isArray(reply) || Number(reply[1]) === 0
  } finally {
    probe()
  }
}
