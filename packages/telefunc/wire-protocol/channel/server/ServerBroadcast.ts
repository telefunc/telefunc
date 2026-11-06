export { Broadcast, BroadcastChannel, ServerBroadcast }

import type {
  ChannelData,
  ChannelPublishAck,
  ChannelPublishInfo,
  BroadcastBinaryListener,
  BroadcastListener,
  ChannelCloseCallback,
  ChannelCloseOptions,
  ChannelCloseResult,
} from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'
import { makePublishInfo } from '../channel.js'
import { broadcastMessages } from '../channelMessages.js'
import { ServerChannel } from './ServerChannel.js'
import { handleCallbackBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { getBroadcastAdapter } from '../../broadcast/broadcastAdapter.js'
import type {
  BroadcastPublishResult,
  BroadcastAdapter,
  BroadcastUnsubscribe,
} from '../../broadcast/broadcastAdapter.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assert, assertUsage, assertWarning } from '../../../utils/assert.js'
import { admitPublish, type MemoryHolder } from './channelMemory.js'
import { isPromise } from '../../../utils/isPromise.js'
import { Listeners } from '../../../utils/Listeners.js'
import { ChannelClosedError, ChannelOverflowError } from '../channelErrors.js'
import { ACK_STATUS, ERROR_REASON, encode } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { ChannelDataFrame } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { PublishBody } from './PublishBody.js'
import { CHANNEL_HOLD_MAX_BYTES, CHANNEL_RECONNECT_TIMEOUT_MS, CREDIT_WINDOW_MAX_BYTES } from '../../constants.js'
import type { IndexedPeer } from './IndexedPeer.js'
import type { AttachState } from '../../wire/payloads.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../../shared/constants.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { isAbort } from '../../../node/server/Abort.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')

class ServerBroadcast<T = unknown> extends ServerChannel implements MemoryHolder {
  readonly [SERVER_BROADCAST_BRAND] = true
  /** @see ChannelShield in ../channel/channel.ts — broadcast only validates incoming
   *  publishes from clients (the `data` direction). The `ack` slot is unused: publish
   *  receipts are server-generated, not client-supplied. */
  declare readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  readonly key: string

  private readonly broadcastListeners = new Listeners<BroadcastListener<T>>()
  private readonly broadcastBinaryListeners = new Listeners<BroadcastBinaryListener>()
  private adapter: BroadcastAdapter | null = null
  private unsubBroadcast: BroadcastUnsubscribe | null = null
  private unsubBinaryBroadcast: BroadcastUnsubscribe | null = null
  private peerSubscribedText = false
  private peerSubscribedBinary = false

  constructor(opts: { key: string }) {
    super()
    this.key = opts.key
  }

  /** Its page is told what it missed by `info.seq`, and the server's log by this once. */
  private missedWarned = false
  /** The page's channel on the wire it last attached on: a publish while the page is away joins the replay through it,
   *  as one a silently dead wire lost does. */
  private lastPeer: IndexedPeer | null = null

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
    this.ensureBroadcast()
    if (!this.adapter) throw new ChannelClosedError()
    const serialized = stringify(data)
    const ret = this.lifecycle.trackAck(Promise.resolve(this.publishBroadcast(serialized)))
    ret.catch(() => {})
    return ret
  }

  override messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options)
  }

  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): () => void {
    if (options.signal?.aborted) return () => {}
    this.ensureBroadcast()
    const remove = this.broadcastListeners.add(callback, {
      ...options,
      onRemove: () => this.unsubscribeUnusedBroadcasts(),
    })
    this.subscribeBroadcast()
    this.unsubscribeUnusedBroadcasts()
    return remove
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this.ensureBroadcast()
    if (!this.adapter) throw new ChannelClosedError()
    const ret = this.lifecycle.trackAck(Promise.resolve(this.publishBinaryBroadcast(data)))
    ret.catch(() => {})
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): () => void {
    if (options.signal?.aborted) return () => {}
    this.ensureBroadcast()
    const remove = this.broadcastBinaryListeners.add(callback, {
      ...options,
      onRemove: () => this.unsubscribeUnusedBroadcasts(),
    })
    this.subscribeBinaryBroadcast()
    this.unsubscribeUnusedBroadcasts()
    return remove
  }

  // --- Transport callbacks ---

  protected override dispatchDataFrame(frame: ChannelDataFrame): void {
    if (frame.tag === TAG.PUBLISH_ACK_REQ) {
      void this._onPeerPublishAckReqMessage(frame.text, frame.seq)
      return
    }
    if (frame.tag === TAG.PUBLISH_BINARY_ACK_REQ) {
      void this._onPeerPublishBinaryAckReqMessage(frame.data, frame.seq)
      return
    }
    super.dispatchDataFrame(frame)
  }

  _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.dispatchPublishAckReq(text, seq))
  }

  _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this.lifecycle.trackAck(this.dispatchPublishBinaryAckReq(data, seq))
  }

  private deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this.broadcastListeners.list()) {
      try {
        const result = cb(data, info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    if (!this.peerSubscribedText) return
    this.forwardPublish(bodyOf(encode.publish, serialized, rawInfo))
  }

  private deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const cb of this.broadcastBinaryListeners.list()) {
      try {
        // A copy of its own, which a Node.js Buffer's slice() isn't: a listener may write into it or transfer its buffer.
        const result = cb(new Uint8Array(data), info)
        if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
      } catch (err) {
        if (this.lifecycle.handleCallbackError(err)) return
      }
    }
    if (!this.peerSubscribedBinary) return
    this.forwardPublish(bodyOf(encode.publishBinary, data, rawInfo))
  }

  /** A publish to the page. A subscription is the page's, so it has attached before. */
  private forwardPublish(body: PublishBody): void {
    this.lastPeer!.sendPublish(body)
    const share = this.share()
    if (this.heldBytes > share) this.shed(share)
  }

  /** What it may hold: 60 s of what its page takes, within what any channel may. */
  private share(): number {
    const perMs = this.peer?.sender.queue.drainRate() ?? 0
    if (perMs === 0) return CHANNEL_HOLD_MAX_BYTES
    return Math.min(CHANNEL_HOLD_MAX_BYTES, Math.max(CREDIT_WINDOW_MAX_BYTES, perMs * CHANNEL_RECONNECT_TIMEOUT_MS))
  }

  /** Its newest publish, where no wire has that yet. */
  get newestUnhanded(): PublishBody | undefined {
    if (this._phase === 'ended' || !this._replayBuffer) return undefined
    const peer = this.peer
    if (peer && peer.sender.queue.waitingPublishes(peer.index) === 0) return undefined
    return this._replayBuffer.newestPublish()
  }

  /** `MemoryHolder`: its oldest publishes no wire has go, down to `toBytes`; its page sees the jump in `info.seq`. Above 0
   *  the newest stays, and at 0, where not even that fits, the channel ends: no later message would show the page. */
  shed(toBytes: number): void {
    const replay = this._replayBuffer
    if (this._phase === 'ended' || !replay) return
    const peer = this.peer
    const dropped = replay.dropOldestPublishes(
      toBytes,
      peer ? peer.sender.queue.waitingPublishes(peer.index) : Number.POSITIVE_INFINITY,
      toBytes === 0,
    )
    if (peer && dropped > 0) peer.sender.queue.dropWaitingPublishes(peer.index, dropped)
    if (dropped > 0) this.warnMissed()
    if (toBytes === 0) {
      this.endWithError(
        ERROR_REASON.OVERFLOW,
        new ChannelOverflowError('Broadcast closed: the server could not hold even the newest message for its client'),
      )
    }
  }

  private warnMissed(): void {
    if (this.missedWarned) return
    this.missedWarned = true
    assertWarning(
      false,
      `A \`BroadcastChannel\` on key "${this.key}" fell behind what the server holds for its client, which misses its oldest messages: \`info.seq\` shows the gap on the client.`,
      { onlyOnce: false },
    )
  }

  protected override onPeerSubscription({ binary, subscribed }: { binary: boolean; subscribed: boolean }): void {
    if (binary) this.peerSubscribedBinary = subscribed
    else this.peerSubscribedText = subscribed
    if (subscribed) {
      this.ensureBroadcast()
      if (binary) this.subscribeBinaryBroadcast()
      else this.subscribeBroadcast()
      return
    }
    this.unsubscribeUnusedBroadcasts()
  }

  private unsubscribeUnusedBroadcasts(): void {
    if (!this.peerSubscribedBinary && this.broadcastBinaryListeners.size === 0) {
      this.unsubBinaryBroadcast?.()
      this.unsubBinaryBroadcast = null
    }
    if (!this.peerSubscribedText && this.broadcastListeners.size === 0) {
      this.unsubBroadcast?.()
      this.unsubBroadcast = null
    }
  }

  override _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    this.lastPeer = peer
    super._attachPeer(peer, state)
  }

  protected override shutdown(err?: Error, options?: { pageGone?: boolean }): void {
    this.lastPeer = null
    this.unsubBroadcast?.()
    this.unsubBroadcast = null
    this.unsubBinaryBroadcast?.()
    this.unsubBinaryBroadcast = null
    super.shutdown(err, options)
    this.broadcastListeners.clear()
    this.broadcastBinaryListeners.clear()
  }

  // --- Internal broadcast helpers ---

  private ensureBroadcast(): void {
    if (this.adapter) return
    this.adapter = getBroadcastAdapter()
  }

  private subscribeBroadcast(): void {
    if (this.unsubBroadcast) return
    assert(this.adapter)
    this.unsubBroadcast = this.adapter.subscribe(
      this.key,
      (serialized, rawInfo) => this.deliverBroadcastMessage(serialized, rawInfo),
      this.wireRequest,
    )
  }

  private subscribeBinaryBroadcast(): void {
    if (this.unsubBinaryBroadcast) return
    assert(this.adapter)
    this.unsubBinaryBroadcast = this.adapter.subscribeBinary(
      this.key,
      (data, rawInfo) => this.deliverBroadcastBinaryMessage(data, rawInfo),
      this.wireRequest,
    )
  }

  private publishBroadcast(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this.adapter)
    return receiptOf(this.key, this.adapter.publish(this.key, serialized))
  }

  private publishBinaryBroadcast(data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    assert(this.adapter)
    return receiptOf(this.key, this.adapter.publishBinary(this.key, data))
  }

  private async dispatchPublishAckReq(serialized: string, seq: number): Promise<void> {
    try {
      this.ensureBroadcast()
      const validateData = this._validators.get('data')
      if (validateData) {
        const data = parse(serialized) as ChannelData<T>
        const result = validateData(data)
        // `shield-error` status lets the client reject its `publish()` promise with a branded
        // ShieldValidationError — same identity every other shield-fail surface produces.
        if (result !== true) {
          this.sendAckRes(seq, result, ACK_STATUS.SHIELD_ERROR)
          return
        }
      }
      const result = await this.publishBroadcast(serialized)
      this.sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this.lifecycle.handleCallbackError(err)) return
      this.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }

  private async dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      this.ensureBroadcast()
      const result = await this.publishBinaryBroadcast(data)
      this.sendAckRes(seq, stringify(result))
    } catch (err) {
      if (this.lifecycle.handleCallbackError(err)) return
      this.sendAckRes(seq, `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`, ACK_STATUS.ERROR)
    }
  }
}

/** Public surface of a `BroadcastChannel` instance — same shape `Channel` uses to hide internal
 *  `_methods` from autocomplete on user-facing `chat.` etc. The underlying class is `ServerBroadcast`. */
type BroadcastChannel<T = unknown> = {
  readonly key: string
  readonly id: string
  readonly isClosed: boolean
  readonly opened: Promise<void>
  readonly closed: Promise<void>
  messages(options?: { signal?: AbortSignal }): AsyncIterableIterator<
    ({ type: 'text'; data: ChannelData<T> } | { type: 'binary'; data: Uint8Array }) & { info: ChannelPublishInfo }
  >
  readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  publish(data: ChannelData<T>): Promise<ChannelPublishAck>
  subscribe(callback: BroadcastListener<T>, options?: { signal?: AbortSignal }): () => void
  publishBinary(data: Uint8Array): Promise<ChannelPublishAck>
  subscribeBinary(callback: BroadcastBinaryListener, options?: { signal?: AbortSignal }): () => void
  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): void
  onOpen(callback: () => void, options?: { signal?: AbortSignal }): void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
  abort(): void
  abort(abortValue: unknown, message?: string): void
}

const BroadcastChannel = ServerBroadcast as {
  new <T = unknown>(opts: { key: string }): BroadcastChannel<T>
}

/** One delivery hands every subscriber on this server the same `info`, so they share the body the first one makes. */
const publishBodies = new WeakMap<WirePublishInfo, PublishBody>()

function bodyOf<T>(
  encodeBody: (data: T, info: WirePublishInfo) => Uint8Array<ArrayBuffer>,
  data: T,
  info: WirePublishInfo,
): PublishBody {
  let body = publishBodies.get(info)
  if (!body) {
    body = new PublishBody(encodeBody(data, info))
    admitPublish(body)
    publishBodies.set(info, body)
  }
  return body
}

/** The adapter's receipt as the public one, carrying its key like a subscriber's `info`. */
function receiptOf(
  key: string,
  result: BroadcastPublishResult | Promise<BroadcastPublishResult>,
): ChannelPublishAck | Promise<ChannelPublishAck> {
  const toAck = (r: BroadcastPublishResult): ChannelPublishAck => ({
    key,
    seq: r.seq,
    timestamp: r.timestamp,
    meta: r.meta,
  })
  if (!isPromise(result)) return toAck(result)
  const receipt = result.then(toAck)
  receipt.catch(() => {})
  return receipt
}

const Broadcast = {
  publish<U = unknown>(key: string, data: ChannelData<U>): ChannelPublishAck | Promise<ChannelPublishAck> {
    const adapter = getBroadcastAdapter()
    const serialized = stringify(data)
    return receiptOf(key, adapter.publish(key, serialized))
  },
  subscribe<U = unknown>(
    key: string,
    callback: BroadcastListener<U>,
    options?: { signal?: AbortSignal },
  ): BroadcastUnsubscribe {
    const adapter = getBroadcastAdapter()
    return subscribeWithSignal(
      () =>
        adapter.subscribe(key, (serialized, info) => {
          const data = parse(serialized) as ChannelData<U>
          callStaticListener(callback, data, makePublishInfo(key, info.seq, info.timestamp))
        }),
      options,
    )
  },
  publishBinary(key: string, data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    const adapter = getBroadcastAdapter()
    return receiptOf(key, adapter.publishBinary(key, data))
  },
  subscribeBinary(
    key: string,
    callback: BroadcastBinaryListener,
    options?: { signal?: AbortSignal },
  ): BroadcastUnsubscribe {
    const adapter = getBroadcastAdapter()
    return subscribeWithSignal(
      () =>
        adapter.subscribeBinary(key, (data, info) => {
          // A copy of its own, as a `BroadcastChannel` listener gets.
          callStaticListener(callback, new Uint8Array(data), makePublishInfo(key, info.seq, info.timestamp))
        }),
      options,
    )
  },
}

/** Reports a listener's error as a channel listener's is, but an Abort: there is no channel to close. */
function callStaticListener<D>(
  callback: (data: D, info: ChannelPublishInfo) => unknown,
  data: D,
  info: ChannelPublishInfo,
): void {
  try {
    const pending = callback(data, info)
    if (isPromise(pending)) pending.catch(reportStaticListenerError)
  } catch (err) {
    reportStaticListenerError(err)
  }
}

function reportStaticListenerError(err: unknown): void {
  if (!isAbort(err)) handleCallbackBug(err)
}

function subscribeWithSignal(subscribe: () => () => void, { signal }: { signal?: AbortSignal } = {}): () => void {
  if (signal?.aborted) return () => {}
  let unsubscribe: (() => void) | undefined = subscribe()
  const remove = () => {
    const current = unsubscribe
    unsubscribe = undefined
    signal?.removeEventListener('abort', remove)
    current?.()
  }
  signal?.addEventListener('abort', remove, { once: true })
  if (signal?.aborted) remove()
  return remove
}
