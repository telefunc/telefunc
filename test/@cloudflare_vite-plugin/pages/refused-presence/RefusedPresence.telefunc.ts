export { onSubscribe, onPublish }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** The key's authority refuses its first presence (see +server.ts): the subscriber's `opened` rejects, and the app
 *  subscribes again. */
async function onSubscribe(key: string) {
  const messages: string[] = []
  received.set(key, messages)
  const onMessage = (message: string) => messages.push(message)
  const subscribed = Broadcast.subscribe<string>(key, onMessage)
  await Broadcast.publish(key, 'hello')
  const opened = await subscribed.opened.then(
    () => 'resolved',
    () => 'rejected',
  )
  if (opened === 'rejected') await Broadcast.subscribe<string>(key, onMessage).opened
  return { received: messages, opened }
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return received.get(key) ?? []
}
