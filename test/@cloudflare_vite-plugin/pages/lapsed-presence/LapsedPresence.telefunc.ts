export { onSubscribe, onPublish, onRead }

import { Broadcast, config } from 'telefunc'
import { currentCloudflareHostKey } from 'telefunc/__internal'
import { lapsedPresences } from './presenceRecords'

/** A key's subscriber in this session: the incarnation of the session's Durable Object that subscribed, what it got,
 *  and the losses it was told of. */
type Subscriber = { incarnation: string; received: string[]; reported: string[] }
const subscribers = new Map<string, Subscriber>()

/** The key's authority refuses its refreshes under the lease it first recorded for `LAPSED_PRESENCE_REFUSED_FOR_MS`
 *  (see +server.ts). */
async function onSubscribe(key: string) {
  config.broadcast.onDeliveryLost = ({ key, reason }) => {
    subscribers.get(key)?.reported.push(reason)
  }
  const subscriber: Subscriber = { incarnation: currentCloudflareHostKey(), received: [], reported: [] }
  const subscription = Broadcast.subscribe<string>(key, (message) => subscriber.received.push(message))
  await subscription.opened
  subscribers.set(key, subscriber)
  return onPublish(key, 'before')
}

async function onPublish(key: string, message: string) {
  await Broadcast.publish(key, message)
  return { ...(await onRead(key)), published: message }
}

/** The key's subscriber as this session sees it now. `rerecorded`: the authority recorded its presence again under
 *  its first lease after refusing it. `evicted`: the session's Durable Object isn't the incarnation that subscribed (or
 *  keeps no subscriber), so the subscription ended with it. */
async function onRead(key: string) {
  const subscriber = subscribers.get(key)
  return {
    received: subscriber?.received ?? [],
    reported: subscriber?.reported ?? [],
    rerecorded: lapsedPresences.get(key)?.rerecorded ?? false,
    evicted: subscriber?.incarnation !== currentCloudflareHostKey(),
  }
}
