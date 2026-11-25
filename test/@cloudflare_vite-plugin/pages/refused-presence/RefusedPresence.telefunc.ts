export { onSubscribe, onPublish }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** The key's authority refuses its first presence (see +server.ts): the subscriber's `ready` rejects, and the app
 *  subscribes again. */
async function onSubscribe(key: string) {
  const messages: string[] = []
  received.set(key, messages)
  const onMessage = (message: string) => messages.push(message)
  const subscribed = Broadcast.subscribe<string>(key, onMessage)
  await Broadcast.publish(key, 'hello')
  const ready = await subscribed.ready.then(
    () => 'resolved',
    () => 'rejected',
  )
  if (ready === 'rejected') await Broadcast.subscribe<string>(key, onMessage).ready
  return { received: messages, ready }
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  const messages = received.get(key)
  if (messages === undefined) throw new Error(`Test: nothing subscribed on '${key}'`)
  return messages
}
