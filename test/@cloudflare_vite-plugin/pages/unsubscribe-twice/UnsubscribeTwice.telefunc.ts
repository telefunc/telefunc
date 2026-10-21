export { onSubscribeAndUnsubscribeTwice, onPublish, onReceived }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** Two subscribers on `key`; the first one's unsubscribe runs twice. */
async function onSubscribeAndUnsubscribeTwice(key: string) {
  received.set(key, [])
  const unsubscribe = Broadcast.subscribe(key, () => {})
  Broadcast.subscribe<string>(key, (message) => received.get(key)!.push(message))
  unsubscribe()
  unsubscribe()
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
}

async function onReceived(key: string) {
  return received.get(key)!
}
