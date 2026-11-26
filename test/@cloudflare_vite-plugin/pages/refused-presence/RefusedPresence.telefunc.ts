export { onSubscribe, onPublish }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** The key's presence is the one KV refuses to write (see +server.ts): returns whether the subscription's `opened`
 *  rejected, and whether the publish made meanwhile settled. */
async function onSubscribe(key: string) {
  const messages: string[] = []
  received.set(key, messages)
  const off = Broadcast.subscribe<string>(key, (message) => messages.push(message))
  const [opened, publish] = await Promise.allSettled([off.opened, Broadcast.publish(key, 'hello')])
  return { openedRejected: opened.status === 'rejected', published: publish.status === 'fulfilled' }
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return received.get(key) ?? []
}
