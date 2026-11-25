export { onResubscribe, onPublish, onReceived }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** The key's last subscriber leaves while its presence is being written, as the next one subscribes. */
async function onResubscribe(key: string) {
  Broadcast.subscribe(key, () => {})()
  const messages: string[] = []
  received.set(key, messages)
  Broadcast.subscribe<string>(key, (message) => messages.push(message))
}

async function onReceived(key: string) {
  return receivedOn(key)
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return receivedOn(key)
}

function receivedOn(key: string): string[] {
  const messages = received.get(key)
  if (messages === undefined) throw new Error(`Test: nothing subscribed on '${key}'`)
  return messages
}
