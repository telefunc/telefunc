export { onResubscribe, onPublish }

import { Broadcast } from 'telefunc'

const received = new Map<string, string[]>()

/** The key's last subscriber leaves while its presence is being written, as the next one subscribes. */
async function onResubscribe(key: string) {
  Broadcast.subscribe(key, () => {})()
  received.set(key, [])
  Broadcast.subscribe<string>(key, (message) => received.get(key)!.push(message))
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return received.get(key)!
}
