export { onSubscribe, onReceived }

import { Broadcast } from 'telefunc'

/** By key, then by page: the sessions share an isolate under workerd. */
const received = new Map<string, string[]>()

async function onSubscribe(key: string, page: string) {
  const messages: string[] = []
  received.set(`${key}:${page}`, messages)
  Broadcast.subscribe<string>(key, (message) => messages.push(message))
}

async function onReceived(key: string, page: string) {
  const messages = received.get(`${key}:${page}`)
  if (messages === undefined) throw new Error(`Test: page '${page}' subscribed nothing on '${key}'`)
  return messages
}
