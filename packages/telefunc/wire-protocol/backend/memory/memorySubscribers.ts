export { addSubscriber, removeSubscriber, deliverTo }
export type { Subscribers }

import type { BackendPayload, BackendReceiver } from '../driver.js'
import type { OrderingInfo } from '../driverOrdering.js'

/** Receivers by key; a change replaces a key's array, so an array read at a publish or commit is its targets. */
type Subscribers = Map<string, readonly BackendReceiver[]>

function addSubscriber(subscribers: Subscribers, key: string, receiver: BackendReceiver): void {
  subscribers.set(key, [...(subscribers.get(key) ?? []), receiver])
}

/** Returns whether `receiver` was among `key`'s subscribers. */
function removeSubscriber(subscribers: Subscribers, key: string, receiver: BackendReceiver): boolean {
  const current = subscribers.get(key)
  if (current === undefined || !current.includes(receiver)) return false
  const rest = current.filter((other) => other !== receiver)
  if (rest.length === 0) subscribers.delete(key)
  else subscribers.set(key, rest)
  return true
}

/** Hands a message to each of `targets`: one that throws is reported, and the others still get it. */
function deliverTo(targets: readonly BackendReceiver[], payload: BackendPayload, ordering: OrderingInfo): void {
  for (const target of targets) {
    try {
      target.deliver(payload, ordering)
    } catch (error) {
      console.error(error)
    }
  }
}
