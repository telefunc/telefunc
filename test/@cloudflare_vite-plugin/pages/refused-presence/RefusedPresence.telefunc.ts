export { onSubscribe, onPublish }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** The key's presence is the one KV refuses to write (see +server.ts): returns whether the subscription's `ready`
 *  rejected, and whether the publish made meanwhile settled. */
async function onSubscribe(key: string) {
  received.set(key, [])
  const off = Broadcast.subscribe<string>(key, (message) => received.get(key)!.push(message))
  const [ready, publish] = await Promise.allSettled([off.ready, Broadcast.publish(key, 'hello')])
  return { readyRejected: ready.status === 'rejected', published: publish.status === 'fulfilled' }
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return received.get(key)!
}
