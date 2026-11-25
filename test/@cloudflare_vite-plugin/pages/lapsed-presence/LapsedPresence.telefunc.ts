export { onSubscribe, onPublish, onRead }

import { Broadcast, config } from 'telefunc'
import { currentCloudflareHostKey } from 'telefunc/__internal'
import { lapsedPresences } from './presenceRecords'

/** A key's subscriber in this session: the incarnation of the session's Durable Object that subscribed, what it got,
 *  and the losses it was told of. */
type Subscriber = { incarnation: string; received: string[]; reported: string[] }
const subscribers = new Map<string, Subscriber>()

async function onSubscribe(key: string) {
  config.broadcast.onDeliveryLost = ({ key, reason }) => {
    subscribers.get(key)?.reported.push(reason)
  }
  const subscriber: Subscriber = { incarnation: currentCloudflareHostKey(), received: [], reported: [] }
  const subscription = Broadcast.subscribe<string>(key, (message) => subscriber.received.push(message))
  await subscription.ready
  subscribers.set(key, subscriber)
  return onPublish(key, 'before')
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return { ...(await onRead(key)), published: message }
}

async function onRead(key: string) {
  const subscriber = subscribers.get(key)
  return {
    received: subscriber?.received ?? [],
    reported: subscriber?.reported ?? [],
    rerecorded: lapsedPresences.get(key)?.rerecorded ?? false,
    evicted: subscriber?.incarnation !== currentCloudflareHostKey(),
  }
}
