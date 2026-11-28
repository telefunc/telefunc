export { onSubscribeAndUnsubscribeTwice, onPublish, onReceived }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** Two subscribers on `key`; the first one's unsubscribe runs twice. */
async function onSubscribeAndUnsubscribeTwice(key: string) {
  const messages: string[] = []
  received.set(key, messages)
  const unsubscribe = Broadcast.subscribe(key, () => {})
  Broadcast.subscribe<string>(key, (message) => messages.push(message))
  unsubscribe()
  unsubscribe()
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
}

async function onReceived(key: string) {
  const messages = received.get(key)
  if (messages === undefined) throw new Error(`Test: nothing subscribed on '${key}'`)
  return messages
}
