export { TransportBroadcastDriver }
export type { BroadcastTransport, BroadcastUnsubscribe }

import { isOrdering } from '../driverOrdering.js'
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
  listen(
    key: string,
    onMessage: (payload: string, info: { seq: number; timestamp: number }) => void,
  ): BroadcastUnsubscribe
  sendBinary(
    key: string,
    payload: Uint8Array,
  ): { seq: number; timestamp: number } | Promise<{ seq: number; timestamp: number }>
  listenBinary(
    key: string,
    onMessage: (payload: Uint8Array, info: { seq: number; timestamp: number }) => void,
  ): BroadcastUnsubscribe
}

/** What stops a listener: the function, where the listener receives what any server publishes from the moment `listen()`
 *  returns; with `ready`, where the broker confirms the listener later: it resolves once the listener receives what any
 *  server publishes to the key, and rejects where the broker refuses it. Telefunc calls `unsubscribe()` once for every
 *  `listen()` that returned, also where its `ready` rejected; a `listen()` that throws returns nothing to call, so it
 *  releases what it took before it throws. */
type BroadcastUnsubscribe = (() => void) | { unsubscribe: () => void; ready: Promise<void> }

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
    let listening: BroadcastUnsubscribe
    try {
      listening =
        route.kind === 'text'
          ? this.transport.listen(route.key, (payload, info) => receiver.deliver(payload, checkOrdering(info)))
          : this.transport.listenBinary(route.key, (payload, info) => receiver.deliver(payload, checkOrdering(info)))
    } catch (err) {
      throw refusal(err)
    }
    return new ListenAttempt(listening)
  }
}

/** Ready once the transport's listener is: at once where `listen()` returned the function that stops it, else once its
 *  `ready` resolves; closed with the broker's refusal where that rejects, and the listener stopped. */
class ListenAttempt extends DriverAttempt {
  private readonly stop: () => void

  constructor(listening: BroadcastUnsubscribe) {
    super()
    if (typeof listening === 'function') {
      this.stop = listening
      this.transition('ready')
      return
    }
    this.stop = listening.unsubscribe
    void this.whenListening(listening.ready)
  }

  private async whenListening(ready: Promise<void>): Promise<void> {
    try {
      await ready
    } catch (err) {
      // Unsubscribed first: that stopped it.
      if (this.state() === 'closed') return
      this.transition('closed', refusal(err))
      return this.stopRefused()
    }
    this.transition('ready')
  }

  /** What its stop throws is logged, as on an unsubscribe: the refusal already ended the attempt. */
  private stopRefused(): void {
    try {
      this.stop()
    } catch (error) {
      console.error(error)
    }
  }

  async unsubscribe(): Promise<void> {
    this.transition('closed')
    this.stop()
  }
}

/** What the broker refused a listener with: a close without a reason is a backend's on purpose, which isn't reported,
 *  while a refusal always is. */
function refusal(err: unknown): unknown {
  return err ?? new Error('config.broadcast.transport refused the listener with no error')
}

function checkOrdering<Ordering extends { seq: number; timestamp: number }>(ordering: Ordering): Ordering {
  assertUsage(
    isOrdering(ordering),
    `config.broadcast.transport returned { seq: ${ordering.seq}, timestamp: ${ordering.timestamp} }: seq must be a ` +
      `positive safe integer and timestamp a non-negative safe integer.`,
  )
  return ordering
}
