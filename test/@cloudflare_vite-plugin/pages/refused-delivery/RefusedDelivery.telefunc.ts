export { onSubscribe, onReceived }

import { Broadcast } from 'telefunc'

/** By key, then by page: the sessions share an isolate under workerd. */
const received = new Map<string, string[]>()

async function onSubscribe(key: string, page: string) {
  received.set(`${key}:${page}`, [])
  Broadcast.subscribe<string>(key, (message) => received.get(`${key}:${page}`)!.push(message))
}

async function onReceived(key: string, page: string) {
  return received.get(`${key}:${page}`)!
}
