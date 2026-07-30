export { Broadcast, BroadcastChannel, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  BroadcastBinaryListener,
  BroadcastListener,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { makePublishInfo } from '../channel.js'
import { ServerChannel } from './channel.js'
import { getBackend } from '../backend/install.js'
import type { BackendSpi, BackendSubscription, BroadcastLane, PublishResult } from '../backend/spi.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage } from '../../utils/assert.js'
import { isPromise } from '../../utils/isPromise.js'
import { ChannelClosedError } from '../channel-errors.js'
import { ACK_STATUS, encodePublishText, encodePublishBinary, TAG } from '../shared-ws.js'
import type { ChannelCtrlFrame, ChannelDataFrame, WirePublishInfo } from '../shared-ws.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
type BroadcastUnsubscribe = () => void
type TrackedSubscription = {
  subscription: BackendSubscription
  removed: Promise<void>
  markRemoved: () => void
}

const subscriptionsByBackend = new WeakMap<BackendSpi, Map<string, Set<TrackedSubscription>>>()

function laneKey(lane: BroadcastLane): string {
  return `${lane.kind}\0${lane.key}`
}

function trackSubscription(backend: BackendSpi, lane: BroadcastLane, subscription: BackendSubscription): () => void {
  let byLane = subscriptionsByBackend.get(backend)
  if (!byLane) subscriptionsByBackend.set(backend, (byLane = new Map()))
  const key = laneKey(lane)
  let subscriptions = byLane.get(key)
  if (!subscriptions) byLane.set(key, (subscriptions = new Set()))
  let markRemoved!: () => void
  const tracked: TrackedSubscription = {
    subscription,
    removed: new Promise<void>((resolve) => {
      markRemoved = resolve
    }),
    markRemoved: () => markRemoved(),
  }
  subscriptions.add(tracked)
  let active = true
  let unobserve = () => {}
  const remove = () => {
    if (!active) return
    active = false
    unobserve()
    tracked.markRemoved()
    subscriptions.delete(tracked)
    if (subscriptions.size === 0) byLane.delete(key)
  }
  unobserve = subscription.onStateChange((state) => {
    if (state === 'closed') remove()
  })
  if (!active) unobserve()
  else if (subscription.state() === 'closed') remove()
  return remove
}

function publishWhenSubscriptionsReady<T>(
  backend: BackendSpi,
  lane: BroadcastLane,
  publish: () => T | Promise<T>,
): T | Promise<T> {
  const subscriptions = subscriptionsByBackend.get(backend)?.get(laneKey(lane))
  if (!subscriptions) return publish()
  const pending = [...subscriptions]
    .filter(({ subscription }) => subscription.state() !== 'ready')
    .map(({ subscription, removed }) => Promise.race([subscription.ready, removed]))
  if (pending.length === 0) return publish()
  return Promise.all(pending).then(() => publishWhenSubscriptionsReady(backend, lane, publish))
}

class ServerBroadcast<T = unknown> extends ServerChannel {
  readonly [SERVER_BROADCAST_BRAND] = true
  /** @see ChannelShield in ../channel.ts — broadcast only validates incoming
   *  publishes from clients (the `data` direction). The `ack` slot is unused: publish
   *  receipts are server-generated, not client-supplied. */
  declare readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  readonly key: string

  private _broadcastListeners: Array<BroadcastListener<T>> = []
  private _broadcastBinaryListeners: Array<BroadcastBinaryListener> = []
  private _backend: BackendSpi | null = null
  private _unsubBroadcast: BackendSubscription | null = null
  private _unsubBinaryBroadcast: BackendSubscription | null = null
  private _untrackBroadcast: (() => void) | null = null
  private _untrackBinaryBroadcast: (() => void) | null = null
  private _peerSubscribedText = false
  private _peerSubscribedBinary = false

  constructor(opts: { key: string }) {
    super()
    this.key = opts.key
  }

  static isServerBroadcast(value: unknown): value is ServerBroadcast {
    return value !== null && typeof value === 'object' && SERVER_BROADCAST_BRAND in value
  }

  // Channel methods that don't apply to broadcast: throw at runtime.
  override send(): never {
    assertUsage(false, '`send()` is not available on a `BroadcastChannel` — use `publish()`.')
  }
  override sendBinary(): never {
    assertUsage(false, '`sendBinary()` is not available on a `BroadcastChannel` — use `publishBinary()`.')
  }
  override listen(): never {
    assertUsage(false, '`listen()` is not available on a `BroadcastChannel` — use `subscribe()`.')
  }
  override listenBinary(): never {
    assertUsage(false, '`listenBinary()` is not available on a `BroadcastChannel` — use `subscribeBinary()`.')
  }

  publish(data: ChannelData<T>): Promise<ChannelPublishAck> {
    this._ensureBroadcast()
    if (!this._backend) throw new ChannelClosedError()
    const serialized = stringify(data)
    const ret = this._trackAck(Promise.resolve(this._publishBroadcast(serialized)))
    ret.catch(() => {})
    return ret
  }

  subscribe(callback: BroadcastListener<T>): () => void {
    this._ensureBroadcast()
    this._subscribeBroadcast()
    this._broadcastListeners.push(callback)
    return () => {
      const index = this._broadcastListeners.indexOf(callback)
      if (index >= 0) this._broadcastListeners.splice(index, 1)
    }
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this._ensureBroadcast()
    if (!this._backend) throw new ChannelClosedError()
    const ret = this._trackAck(Promise.resolve(this._publishBinaryBroadcast(data)))
    ret.catch(() => {})
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener): () => void {
    this._ensureBroadcast()
    this._subscribeBinaryBroadcast()
    this._broadcastBinaryListeners.push(callback)
    return () => {
      const index = this._broadcastBinaryListeners.indexOf(callback)
      if (index >= 0) this._broadcastBinaryListeners.splice(index, 1)
    }
  }

  // --- Transport callbacks ---

  protected override _dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.PUBLISH_ACK_REQ) {
      void this._onPeerPublishAckReqMessage(frame.text, frame.seq)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY_ACK_REQ) {
      void this._onPeerPublishBinaryAckReqMessage(frame.data, frame.seq)
      return
    }
    super._dispatchDataFrame(frame)
  }

  override _dispatchCtrl(frame: ChannelCtrlFrame): void {
    if (frame.tag === TAG.BROADCAST_SUB) {
      this._onPeerBroadcastSubscribe(frame.binary)
      return
    }
    if (frame.tag === TAG.BROADCAST_UNSUB) {
      this._onPeerBroadcastUnsubscribe(frame.binary)
      return
    }
    super._dispatchCtrl(frame)
  }

  _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishAckReq(text, seq))
  }

  _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this._trackAck(this._dispatchPublishBinaryAckReq(data, seq))
  }

  _deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this._broadcastListeners) {
      try {
        cb(data, info)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    if (!this._peerSubscribedText) return
    const wireText = encodePublishText(serialized, rawInfo)
    if (this._peer) {
      this._peer.sendPublish(wireText)
      return
    }
    this._prePeerBuffer.pushPublish(wireText)
  }

  _deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const cb of this._broadcastBinaryListeners) {
      try {
        cb(data, info)
      } catch (err) {
        if (this._handleCallbackError(err)) return
      }
    }
    if (!this._peerSubscribedBinary) return
    const wireData = encodePublishBinary(data, rawInfo)
    if (this._peer) {
      this._peer.sendPublishBinary(wireData)
      return
    }
    this._prePeerBuffer.pushPublishBinary(wireData)
  }

  _onPeerBroadcastSubscribe(binary: boolean): void {
    this._ensureBroadcast()
    if (binary) {
      this._peerSubscribedBinary = true
      this._subscribeBinaryBroadcast()
    } else {
      this._peerSubscribedText = true
      this._subscribeBroadcast()
    }
  }

  _onPeerBroadcastUnsubscribe(binary: boolean): void {
    if (binary) {
      this._peerSubscribedBinary = false
      this._clearBinarySubscription()
    } else {
      this._peerSubscribedText = false
      this._clearTextSubscription()
    }
  }

  protected override _shutdown(err?: Error): void {
    this._clearTextSubscription()
    this._clearBinarySubscription()
    super._shutdown(err)
  }

  // --- Internal broadcast helpers ---

  private _ensureBroadcast(): void {
    if (this._backend) return
    this._backend = getBackend()
  }

  private _subscribeBroadcast(): void {
    if (this._unsubBroadcast) return
    assert(this._backend)
    const lane = { key: this.key, kind: 'text' } as const
    this._unsubBroadcast = this._backend.subscribe(lane, (payload, rawInfo) =>
      this._deliverBroadcastMessage(textDecoder.decode(payload), rawInfo),
    )
    this._untrackBroadcast = trackSubscription(this._backend, lane, this._unsubBroadcast)
  }

  private _subscribeBinaryBroadcast(): void {
    if (this._unsubBinaryBroadcast) return
    assert(this._backend)
    const lane = { key: this.key, kind: 'binary' } as const
    this._unsubBinaryBroadcast = this._backend.subscribe(lane, (data, rawInfo) =>
      this._deliverBroadcastBinaryMessage(data, rawInfo),
    )
    this._untrackBinaryBroadcast = trackSubscription(this._backend, lane, this._unsubBinaryBroadcast)
  }

  private _clearTextSubscription(): void {
    this._untrackBroadcast?.()
    this._untrackBroadcast = null
    if (this._unsubBroadcast) void this._unsubBroadcast.unsubscribe()
    this._unsubBroadcast = null
  }

  private _clearBinarySubscription(): void {
    this._untrackBinaryBroadcast?.()
    this._untrackBinaryBroadcast = null
    if (this._unsubBinaryBroadcast) void this._unsubBinaryBroadcast.unsubscribe()
    this._unsubBinaryBroadcast = null
  }

  private _publishBroadcast(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._backend)
    const backend = this._backend
    const toAck = (r: PublishResult): ChannelPublishAck =>
      Object.assign(makePublishInfo(this.key, r.seq, r.timestamp), {
        meta: r.meta,
        ...(r.receivers === undefined ? {} : { receivers: r.receivers }),
      })
    const lane = { key: this.key, kind: 'text' } as const
    const result = publishWhenSubscriptionsReady(backend, lane, () =>
      backend.publish(lane, textEncoder.encode(serialized)),
    )
    if (isPromise(result)) return result.then(toAck)
    return toAck(result)
  }

  private _publishBinaryBroadcast(data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this._backend)
    const backend = this._backend
    const toAck = (r: PublishResult): ChannelPublishAck =>
      Object.assign(makePublishInfo(this.key, r.seq, r.timestamp), {
        meta: r.meta,
        ...(r.receivers === undefined ? {} : { receivers: r.receivers }),
      })
    const lane = { key: this.key, kind: 'binary' } as const
    const result = publishWhenSubscriptionsReady(backend, lane, () => backend.publish(lane, data))
    if (isPromise(result)) return result.then(toAck)
    return toAck(result)
  }

  private async _dispatchPublishAckReq(serialized: string, seq: number): Promise<void> {
    try {
      this._ensureBroadcast()
      const validateData = this._validators.get('data')
      if (validateData) {
        const data = parse(serialized) as ChannelData<T>
        const result = validateData(data)
        // `shield-error` status lets the client reject its `publish()` promise with a branded
        // ShieldValidationError — same identity every other shield-fail surface produces.
        if (result !== true) {
          this._sendAckRes(seq, result, ACK_STATUS.SHIELD_ERROR)
          return
        }
      }
      const result = await this._publishBroadcast(serialized)
      this._sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }

  private async _dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      this._ensureBroadcast()
      const result = await this._publishBinaryBroadcast(data)
      this._sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this._handleCallbackError(err)) return
      this._sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }
}

/** Public surface of a `BroadcastChannel` instance — same shape `Channel` uses to hide internal
 *  `_methods` from autocomplete on user-facing `chat.` etc. The underlying class is `ServerBroadcast`. */
type BroadcastChannel<T = unknown> = {
  readonly key: string
  readonly id: string
  readonly isClosed: boolean
  readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  publish(data: ChannelData<T>): Promise<ChannelPublishAck>
  subscribe(callback: BroadcastListener<T>): () => void
  publishBinary(data: Uint8Array): Promise<ChannelPublishAck>
  subscribeBinary(callback: BroadcastBinaryListener): () => void
  onClose(callback: ChannelCloseCallback): void
  onOpen(callback: () => void): void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
  abort(): void
  abort(abortValue: unknown, message?: string): void
}

const BroadcastChannel = ServerBroadcast as {
  new <T = unknown>(opts: { key: string }): BroadcastChannel<T>
}

const Broadcast = {
  publish<U = unknown>(key: string, data: ChannelData<U>): PublishResult | Promise<PublishResult> {
    const backend = getBackend()
    const serialized = stringify(data)
    const lane = { key, kind: 'text' } as const
    return publishWhenSubscriptionsReady(backend, lane, () => backend.publish(lane, textEncoder.encode(serialized)))
  },
  subscribe<U = unknown>(key: string, callback: BroadcastListener<U>): BroadcastUnsubscribe {
    const backend = getBackend()
    const lane = { key, kind: 'text' } as const
    const subscription = backend.subscribe(lane, (payload, info) => {
      const data = parse(textDecoder.decode(payload)) as ChannelData<U>
      callback(data, { key, seq: info.seq, timestamp: info.timestamp })
    })
    const untrack = trackSubscription(backend, lane, subscription)
    return () => {
      untrack()
      void subscription.unsubscribe()
    }
  },
  publishBinary(key: string, data: Uint8Array): PublishResult | Promise<PublishResult> {
    const backend = getBackend()
    const lane = { key, kind: 'binary' } as const
    return publishWhenSubscriptionsReady(backend, lane, () => backend.publish(lane, data))
  },
  subscribeBinary(key: string, callback: BroadcastBinaryListener): BroadcastUnsubscribe {
    const backend = getBackend()
    const lane = { key, kind: 'binary' } as const
    const subscription = backend.subscribe(lane, (data, info) => {
      callback(data, { key, seq: info.seq, timestamp: info.timestamp })
    })
    const untrack = trackSubscription(backend, lane, subscription)
    return () => {
      untrack()
      void subscription.unsubscribe()
    }
  },
}
