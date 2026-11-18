export { onSubscribe, onPublish }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** The key's authority refuses its first presence (see +server.ts): the subscriber's `ready` rejects, and it subscribes
 *  again. */
async function onSubscribe(key: string) {
  received.set(key, [])
  const subscribed = Broadcast.subscribe<string>(key, (message) => received.get(key)!.push(message))
  await Broadcast.publish(key, 'hello')
  const ready = await subscribed.ready.then(
    () => 'resolved',
    () => 'rejected',
  )
  return { received: received.get(key)!, ready }
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return received.get(key)!
}
