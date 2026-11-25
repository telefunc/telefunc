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
import { stopLogged } from '../stopLogged.js'

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
 *  server publishes to the key, and rejects where the broker refuses it. Once the listener receives, the transport keeps it
 *  receiving across its reconnects: it has no way to end it. Telefunc calls `unsubscribe()` once for every `listen()` that
 *  returned, also where its `ready` rejected. It waits on nothing `unsubscribe()` returns and logs what it throws or what a
 *  promise it returns rejects with; it may call `listen()` for the key again meanwhile. With `telefunc/cloudflare` it is
 *  called on the Durable Object that holds the subscription: a cleanup that must go on after that object's handler
 *  returns passes its promise to `waitUntil()` from `cloudflare:workers`. A `listen()` that throws returns nothing to
 *  call, so it releases what it took before it throws. */
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

  // One order per source, delivered once to each receiver: a holder asks nothing more of it.
  subscribe(route: BroadcastRoute, receiver: BackendReceiver, _holder: string): DriverAttempt {
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
  private readonly listening: BroadcastUnsubscribe
  /** What the reactions on the broker's `ready` settle, until it is unsubscribed: a broker may keep `ready` pending for
   *  good, and the reactions then hold none of the attempt. */
  private readonly confirming: Confirming = { attempt: null }

  constructor(listening: BroadcastUnsubscribe) {
    super()
    this.listening = listening
    if (typeof listening === 'function') {
      this.transition('ready')
      return
    }
    const confirming = this.confirming
    confirming.attempt = this
    void listening.ready.then(
      () => confirming.attempt?.confirmed(),
      (err: unknown) => confirming.attempt?.refused(err),
    )
  }

  unsubscribe(): Promise<void> {
    this.confirming.attempt = null
    this.transition('closed')
    this.stop()
    return Promise.resolve()
  }

  private confirmed(): void {
    this.transition('ready')
  }

  private refused(err: unknown): void {
    this.confirming.attempt = null
    this.transition('closed', refusal(err))
    this.stop()
  }

  /** As the method it is, where `listen()` returned an object. */
  private stop(): void {
    const listening = this.listening
    stopLogged(typeof listening === 'function' ? listening : () => listening.unsubscribe())
  }
}

type Confirming = { attempt: ListenAttempt | null }

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
