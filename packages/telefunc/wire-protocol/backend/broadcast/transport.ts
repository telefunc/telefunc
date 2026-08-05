export { createBroadcastTransportDriver }
export type { BroadcastTransport }

import type { BroadcastDriver, BroadcastLane, PublishResult } from './contract.js'
import type { BackendReceiver, SubscriptionAttempt, SubscriptionBinding } from '../subscription.js'

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
  return lane.kind === 'text'
    ? transport.send(lane.key, textDecoder.decode(payload))
    : transport.sendBinary(lane.key, payload)
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
      ? transport.listen(lane.key, (payload, info) => receiver(textEncoder.encode(payload), info))
      : transport.listenBinary(lane.key, receiver)
  let closed = false
  const listeners = new Set<(state: 'ready' | 'closed') => void>()
  return {
    ready: Promise.resolve(),
    state: () => (closed ? 'closed' : 'ready'),
    onStateChange: (listener) => {
      listeners.add(listener)
      return () => listeners.delete(listener)
    },
    unsubscribe: async () => {
      if (closed) return
      closed = true
      stop()
      for (const listener of listeners) listener('closed')
      listeners.clear()
    },
  }
}
