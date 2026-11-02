export { TransportBroadcastDriver }
export type { BroadcastTransport }

import { isOrdering } from '../../wire/orderingFrame.js'
import {
  DriverAttempt,
  type BackendReceiver,
  type BroadcastDriver,
  type BroadcastPayload,
  type BroadcastRoute,
  type PublishResult,
} from '../driver.js'
import { assertUsage } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'

/** `seq`: positive safe integer, one order per key across both kinds; `timestamp`: non-negative safe integer. */
type BroadcastTransport = {
  send(key: string, payload: string): { seq: number; timestamp: number } | Promise<{ seq: number; timestamp: number }>
  listen(key: string, onMessage: (payload: string, info: { seq: number; timestamp: number }) => void): () => void
  sendBinary(
    key: string,
    payload: Uint8Array,
  ): { seq: number; timestamp: number } | Promise<{ seq: number; timestamp: number }>
  listenBinary(
    key: string,
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): () => void
}

type TransportBroadcastDriverOptions = { transport: BroadcastTransport }

/** Broadcast on the app's `config.broadcast.transport`. */
class TransportBroadcastDriver implements BroadcastDriver {
  private readonly transport: BroadcastTransport

  constructor({ transport }: TransportBroadcastDriverOptions) {
    this.transport = transport
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): PublishResult | Promise<PublishResult> {
    const result =
      typeof payload === 'string'
        ? this.transport.send(route.key, payload)
        : this.transport.sendBinary(route.key, payload)
    return isPromise(result) ? result.then(checkOrdering) : checkOrdering(result)
  }

  subscribe(route: BroadcastRoute, receiver: BackendReceiver): DriverAttempt {
    const stop =
      route.kind === 'text'
        ? this.transport.listen(route.key, (payload, info) => receiver.deliver(payload, checkOrdering(info)))
        : this.transport.listenBinary(route.key, (payload, info) => receiver.deliver(payload, checkOrdering(info)))
    return new ListenAttempt({ stop })
  }
}

type ListenAttemptOptions = { stop: () => void }

/** Ready at once: a user transport's listen() has no establishment to wait for. */
class ListenAttempt extends DriverAttempt {
  private readonly stop: () => void

  constructor({ stop }: ListenAttemptOptions) {
    super()
    this.stop = stop
    this.transition('ready')
  }

  async unsubscribe(): Promise<void> {
    this.transition('closed')
    this.stop()
  }
}

function checkOrdering<Ordering extends { seq: number; timestamp: number }>(ordering: Ordering): Ordering {
  assertUsage(
    isOrdering(ordering),
    `config.broadcast.transport returned { seq: ${ordering.seq}, timestamp: ${ordering.timestamp} }: seq must be a ` +
      `positive safe integer and timestamp a non-negative safe integer.`,
  )
  return ordering
}
