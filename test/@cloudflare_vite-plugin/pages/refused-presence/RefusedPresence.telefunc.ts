export { onSubscribe, onPublish }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** The key's authority refuses its first presence (see +server.ts). */
async function onSubscribe(key: string) {
  received.set(key, [])
  Broadcast.subscribe<string>(key, (message) => received.get(key)!.push(message))
  await Broadcast.publish(key, 'hello')
  return received.get(key)!
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return received.get(key)!
}
