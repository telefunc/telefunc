export { createBroadcastTransportDriver }
export type { BroadcastTransport }

import { isOrderingPosition } from '../../ordering-frame.js'
import type { BroadcastDriver, BroadcastLane, PublishResult } from './contract.js'
import type { BackendReceiver, SubscriptionAttempt, SubscriptionBinding } from '../subscription.js'
import { assertUsage } from '../../../utils/assert.js'
import { isPromise } from '../../../utils/isPromise.js'
import { DriverAttempt } from '../attempt.js'

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

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

function createBroadcastTransportDriver(transport: BroadcastTransport): BroadcastDriver {
  return {
    publish: (lane, payload) => publish(transport, lane, payload),
    subscriptions: {
      bind: (lane) => bind(transport, lane),
    },
  }
}

function publish(
  transport: BroadcastTransport,
  lane: BroadcastLane,
  payload: Uint8Array,
): PublishResult | Promise<PublishResult> {
  const result =
    lane.kind === 'text'
      ? transport.send(lane.key, textDecoder.decode(payload))
      : transport.sendBinary(lane.key, payload)
  return isPromise(result) ? result.then(checkMark) : checkMark(result)
}

function checkMark<Mark extends { seq: number; timestamp: number }>(mark: Mark): Mark {
  assertUsage(
    isOrderingPosition(mark),
    `config.broadcast.transport returned { seq: ${mark.seq}, timestamp: ${mark.timestamp} }: seq must be a positive safe integer and timestamp a non-negative safe integer.`,
  )
  return mark
}

function bind(transport: BroadcastTransport, lane: BroadcastLane): SubscriptionBinding {
  return {
    partition: lane.kind,
    valid: () => true,
    open: (receiver) => open(transport, lane, receiver),
  }
}

function open(transport: BroadcastTransport, lane: BroadcastLane, receiver: BackendReceiver): SubscriptionAttempt {
  const stop =
    lane.kind === 'text'
      ? transport.listen(lane.key, (payload, info) => receiver(textEncoder.encode(payload), checkMark(info)))
      : transport.listenBinary(lane.key, (payload, info) => receiver(payload, checkMark(info)))
  return new TransportAttempt(stop)
}

/** Ready at once: a user transport's listen() has no establishment to wait for. */
class TransportAttempt extends DriverAttempt {
  readonly #stop: () => void

  constructor(stop: () => void) {
    super()
    this.#stop = stop
    this.transition('ready')
  }

  async unsubscribe(): Promise<void> {
    if (this.ended) return
    this.#stop()
    this.transition('closed')
  }
}
