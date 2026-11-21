export { onSubscribe, onPublish }

import { Broadcast, config } from 'telefunc'

const received = new Map<string, string[]>()
const reported = new Map<string, string[]>()

/** The key's authority refuses its refreshes for 100 s (see +server.ts). */
async function onSubscribe(key: string) {
  config.broadcast.onDeliveryLost = ({ key, reason }) => {
    reported.get(key)?.push(reason)
  }
  received.set(key, [])
  reported.set(key, [])
  Broadcast.subscribe<string>(key, (message) => received.get(key)!.push(message))
  return onPublish(key, 'before')
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return { published: message, received: received.get(key)!, reported: reported.get(key)! }
}
