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
import { assertUsage, assertWarning } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { admitPublish, maxPendingBytesOf, queueMemory, type MemoryHolder } from './channelMemory.js'
import { isPromise } from '../../../utils/isPromise.js'
import { Listeners } from '../../../utils/Listeners.js'
import { ChannelOverflowError } from '../channelErrors.js'
import { ACK_STATUS, ERROR_REASON, encode, type AckResultStatus } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { ChannelDataFrame } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { PublishBody } from './PublishBody.js'
import { CHANNEL_RECONNECT_TIMEOUT_MS, CREDIT_WINDOW_MAX_BYTES } from '../../constants.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { LossyDelivery } from './LossyDelivery.js'
import type { AttachState } from '../../wire/payloads.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { isAbort } from '../../../node/server/Abort.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')
const NOT_EVEN_NEWEST = 'the server could not hold even the newest message for its client'
const FELL_BEHIND = 'its client fell behind what the server holds for it; `lossy: true` lets it skip to the newest'

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

  /** Where its page's path carries less than the wire's lossy streams offer, a newer publish replaces those waiting, on
   *  the way to its page, and on the page's way up once the page is behind. */
  readonly lossy: boolean
  /** A lossy page's publishes on their way to it. */
  private readonly delivery: LossyDelivery | null

  constructor(opts: BroadcastChannelOptions) {
    super()
    const { key, lossy = false } = opts
    assertUsage(typeof lossy === 'boolean', "`new BroadcastChannel()`'s `lossy` should be a boolean")
    this.key = key
    this.lossy = lossy
    this.delivery = lossy ? new LossyDelivery({ holder: this, onSkip: () => this.warnMissed() }) : null
  }

  /** Its page is told what it missed by `info.seq`, and the server's log by this once. */
  private missedWarned = false
  /** Why `shed` ended it, which its page has yet to learn: see `shed`. */
  private overflowed: string | null = null
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
    const serialized = stringify(data)
    const ret = this.lifecycle.trackAck(Promise.resolve(this.publishBroadcast(serialized)))
    ret.catch(() => {})
    return ret
  }

  override messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options, queueMemory())
  }

  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): () => void {
    if (options.signal?.aborted) return () => {}
    const remove = this.broadcastListeners.add(callback, {
      ...options,
      onRemove: () => this.unsubscribeUnusedBroadcasts(),
    })
    this.subscribeBroadcast()
    this.unsubscribeUnusedBroadcasts()
    return remove
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    const ret = this.lifecycle.trackAck(Promise.resolve(this.publishBinaryBroadcast(data)))
    ret.catch(() => {})
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): () => void {
    if (options.signal?.aborted) return () => {}
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

  // What the publish carries is the broadcast's to hold, so its request holds only the room for its answer.
  _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this.answerHeld(seq, 0, () => this.dispatchPublishAckReq(text, seq))
  }

  _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    return this.answerHeld(seq, 0, () => this.dispatchPublishBinaryAckReq(data, seq))
  }

  private deliverBroadcastMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    const data = parse(serialized) as ChannelData<T>
    for (const cb of this.broadcastListeners.list()) {
      if (this.callListener(cb, data, info)) return
    }
    if (!this.peerSubscribedText) return
    const body = bodyOf(encode.publish, serialized, rawInfo, this.owner, (holder) => this.replaceIn(holder, false))
    this.forwardPublish(body)
  }

  private deliverBroadcastBinaryMessage(data: Uint8Array, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const cb of this.broadcastBinaryListeners.list()) {
      // A copy of its own, which a Node.js Buffer's slice() isn't: a listener may write into it or transfer its buffer.
      if (this.callListener(cb, new Uint8Array(data), info)) return
    }
    if (!this.peerSubscribedBinary) return
    const body = bodyOf(encode.publishBinary, data, rawInfo, this.owner, (holder) => this.replaceIn(holder, true))
    this.forwardPublish(body)
  }

  /** Calls `listener` with a publish; true where it threw `Abort`, which aborts the channel, so the rest get none. */
  private callListener<D>(
    listener: (data: D, info: ChannelPublishInfo) => unknown,
    data: D,
    info: ChannelPublishInfo,
  ): boolean {
    try {
      const result = listener(data, info)
      if (isPromise(result)) result.catch((err: unknown) => this.lifecycle.handleCallbackError(err))
    } catch (err) {
      return this.lifecycle.handleCallbackError(err)
    }
    return false
  }

  /** A publish to the page. A subscription is the page's, so it has attached before, but it may have ended since: this
   *  delivery's own listeners or admitting its body can end the channel, and the subscribers after it still get theirs. */
  private forwardPublish(body: PublishBody): void {
    this.endOverflowed()
    if (this._phase === 'ended') return
    if (this.delivery) {
      // A lossy page back from a lost wire gets the newest publish, not those the lost wire carried.
      if (this.peer === null && (this._replayBuffer?.dropPublishes() ?? 0) > 0) this.warnMissed()
      return this.delivery.publish(body)
    }
    this.lastPeer!.sendPublish(body)
    const share = this.share()
    if (this.heldBytes > share) this.shed(share, this.owner)
  }

  /** A graceful close goes behind every publish its page is due; an abort may drop them. */
  override close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this._phase === 'open') this.delivery?.flush()
    return super.close(opts)
  }

  /** Where `holder` is a lossy page this publish reaches (its key, on its object), it lets go of what the publish
   *  replaces. */
  private replaceIn(holder: MemoryHolder, binary: boolean): void {
    if (!(holder instanceof ServerBroadcast) || !holder.lossy || holder.key !== this.key) return
    if (holder.owner !== this.owner) return
    if (binary ? holder.peerSubscribedBinary : holder.peerSubscribedText) holder.delivery?.skipWaiting()
  }

  /** What it may hold: 60 s of what its page takes, at least 64 MiB, within what any channel may; that most before its
   *  page's rate is measured. */
  private share(): number {
    const perMs = this.peer?.sender.queue.drainRate() ?? 0
    const most = maxPendingBytesOf()
    if (perMs === 0) return most
    return Math.min(most, Math.max(CREDIT_WINDOW_MAX_BYTES, perMs * CHANNEL_RECONNECT_TIMEOUT_MS))
  }

  /** How many of its newest publishes no wire has: all of them while it has no wire. */
  private unhanded(): number {
    const { peer } = this
    return peer ? peer.sender.queue.waitingPublishes(peer.index) : Number.POSITIVE_INFINITY
  }

  /** Its newest publish, where no wire has that yet. */
  get newestUnhanded(): PublishBody | undefined {
    if (this._phase === 'ended' || !this._replayBuffer) return undefined
    if (this.delivery) return this.delivery.newestWaiting
    return this.unhanded() === 0 ? undefined : this._replayBuffer.newestPublish()
  }

  private get owner(): string | undefined {
    return this.adapter?.ownerOf?.(this.wireRequest)
  }

  /** `MemoryHolder`: its page gets every publish, so where it would miss one no wire has, the channel ends: from that gap
   *  on everything goes, receipts too, and its page reads on up to the gap and no further. A lossy one ends only at 0,
   *  where not even its newest fits: above that, a newer publish replaces what waits (`replaceIn`). The bytes go at
   *  once, but only its own object can write to its page, so from `owner`'s it ends when its own next runs. */
  shed(toBytes: number, owner?: string): void {
    const replay = this._replayBuffer
    if (this._phase === 'ended' || !replay || (this.lossy && toBytes > 0)) return
    if (toBytes === 0) this.delivery?.drop()
    const gap = replay.dropFromGap(toBytes, this.unhanded()) > 0
    if (gap) {
      this.peer?.sender.queue.dropWaitingFromPublish(this.peer.index)
      this.dropPendingAnswers()
    }
    if (toBytes === 0) this.overflow(NOT_EVEN_NEWEST, owner)
    else if (gap) this.overflow(FELL_BEHIND, owner)
  }

  /** An answer past the gap would cross it: the channel ends first, and the publish it answers settles with that. */
  protected override sendAckRes(ackedSeq: number, result: string, status?: AckResultStatus): void {
    if (this.overflowed !== null) return this.endOverflowed()
    super.sendAckRes(ackedSeq, result, status)
  }

  private overflow(why: string, owner: string | undefined): void {
    this.overflowed ??= why
    if (owner === this.owner) this.endOverflowed()
  }

  private endOverflowed(): void {
    if (this.overflowed === null) return
    this.endWithError(ERROR_REASON.OVERFLOW, new ChannelOverflowError(`Broadcast closed: ${this.overflowed}`))
  }

  private warnMissed(): void {
    if (this.missedWarned) return
    this.missedWarned = true
    assertWarning(
      false,
      `A lossy \`BroadcastChannel\` on key "${this.key}" skipped messages its client hadn't taken yet: \`info.seq\` shows the gap on the client.`,
      { onlyOnce: false },
    )
  }

  protected override onPeerSubscription({ binary, subscribed }: { binary: boolean; subscribed: boolean }): void {
    if (binary) this.peerSubscribedBinary = subscribed
    else this.peerSubscribedText = subscribed
    if (!subscribed) return this.unsubscribeUnusedBroadcasts()
    if (binary) this.subscribeBinaryBroadcast()
    else this.subscribeBroadcast()
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
    if (this._phase !== 'ended') this.delivery?.attach(peer)
    this.endOverflowed()
  }

  override _onPeerDisconnect(peer: IndexedPeer, reconnectTimeout: number): void {
    super._onPeerDisconnect(peer, reconnectTimeout)
    if (this.peer === null) this.delivery?.detach()
  }

  override _acknowledge(): void {
    super._acknowledge()
    this.endOverflowed()
  }

  protected override shutdown(err?: Error, options?: { pageGone?: boolean }): void {
    this.delivery?.drop()
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

  /** The installed adapter, which the channel keeps from its first use. */
  private broadcastAdapter(): BroadcastAdapter {
    this.adapter ??= getBroadcastAdapter()
    return this.adapter
  }

  private subscribeBroadcast(): void {
    if (this.unsubBroadcast) return
    this.unsubBroadcast = this.broadcastAdapter().subscribe(
      this.key,
      (serialized, rawInfo) => this.deliverBroadcastMessage(serialized, rawInfo),
      this.wireRequest,
    )
  }

  private subscribeBinaryBroadcast(): void {
    if (this.unsubBinaryBroadcast) return
    this.unsubBinaryBroadcast = this.broadcastAdapter().subscribeBinary(
      this.key,
      (data, rawInfo) => this.deliverBroadcastBinaryMessage(data, rawInfo),
      this.wireRequest,
    )
  }

  private publishBroadcast(serialized: string): ChannelPublishAck | Promise<ChannelPublishAck> {
    return receiptOf(this.key, this.broadcastAdapter().publish(this.key, serialized))
  }

  private publishBinaryBroadcast(data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    return receiptOf(this.key, this.broadcastAdapter().publishBinary(this.key, data))
  }

  private async dispatchPublishAckReq(serialized: string, seq: number): Promise<void> {
    try {
      const validateData = this._validators.get('data')
      const verdict = validateData ? validateData(parse(serialized) as ChannelData<T>) : true
      // `shield-error` status lets the client reject its `publish()` promise with a branded
      // ShieldValidationError — same identity every other shield-fail surface produces.
      if (verdict !== true) return this.sendAckRes(seq, verdict, ACK_STATUS.SHIELD_ERROR)
      const receipt = await this.publishBroadcast(serialized)
      this.sendAckRes(seq, stringify(receipt))
    } catch (err) {
      this.answerFailed(seq, err)
    }
  }

  private async dispatchPublishBinaryAckReq(data: Uint8Array, seq: number): Promise<void> {
    try {
      const result = await this.publishBinaryBroadcast(data)
      this.sendAckRes(seq, stringify(result))
    } catch (err) {
      this.answerFailed(seq, err)
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

type BroadcastChannelOptions = {
  key: string
  /**
   * A client that keeps up gets every message. One that can't skips to the newest: a newer message replaces those
   * waiting, and `info.seq` jumps over them. Without it, the client gets every message, or the channel closes with
   * `ChannelOverflowError` past what the server holds for it.
   *
   * https://telefunc.com/channel#skip-to-the-newest
   *
   * @default false
   */
  lossy?: boolean
}

const BroadcastChannel = ServerBroadcast as {
  new <T = unknown>(opts: BroadcastChannelOptions): BroadcastChannel<T>
}

const globalObject = getGlobalObject('wire-protocol/channel/server/ServerBroadcast.ts', {
  /** One delivery hands every subscriber on this server the same `info`, so they share the body the first one makes. */
  publishBodies: new WeakMap<WirePublishInfo, PublishBody>(),
})

function bodyOf<T>(
  encodeBody: (data: T, info: WirePublishInfo) => Uint8Array<ArrayBuffer>,
  data: T,
  info: WirePublishInfo,
  owner: string | undefined,
  replace: (holder: MemoryHolder) => void,
): PublishBody {
  let body = globalObject.publishBodies.get(info)
  if (!body) {
    body = new PublishBody(encodeBody(data, info))
    admitPublish(body, owner, replace)
    globalObject.publishBodies.set(info, body)
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
    if (options?.signal?.aborted) return () => {}
    const unsubscribe = adapter.subscribe(key, (serialized, info) => {
      const data = parse(serialized) as ChannelData<U>
      callStaticListener(callback, data, makePublishInfo(key, info.seq, info.timestamp))
    })
    return unsubscribeOnAbort(unsubscribe, options?.signal)
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
    if (options?.signal?.aborted) return () => {}
    const unsubscribe = adapter.subscribeBinary(key, (data, info) => {
      // A copy of its own, as a `BroadcastChannel` listener gets.
      callStaticListener(callback, new Uint8Array(data), makePublishInfo(key, info.seq, info.timestamp))
    })
    return unsubscribeOnAbort(unsubscribe, options?.signal)
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

/** Unsubscribes once `signal` aborts, at once if it aborted while subscribing, or when the returned function runs. */
function unsubscribeOnAbort(unsubscribe: () => void, signal: AbortSignal | undefined): () => void {
  let current: (() => void) | undefined = unsubscribe
  const remove = () => {
    const pending = current
    current = undefined
    signal?.removeEventListener('abort', remove)
    pending?.()
  }
  signal?.addEventListener('abort', remove, { once: true })
  if (signal?.aborted) remove()
  return remove
}
