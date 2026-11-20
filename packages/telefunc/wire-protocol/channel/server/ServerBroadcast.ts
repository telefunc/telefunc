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
import { Outcome, makePublishInfo, neverOpened, stringifyValue, unsubscribeOf, type Unsubscribe } from '../channel.js'
import { broadcastMessages } from '../channelMessages.js'
import { ServerChannel } from './ServerChannel.js'
import { handleCallbackBug } from '../../../node/server/runTelefunc/validateTelefunctionError.js'
import { getBroadcastAdapter } from '../../broadcast/broadcastAdapter.js'
import type { BroadcastPublishResult, BroadcastAdapter, Listening } from '../../broadcast/broadcastAdapter.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { parse } from '@brillout/json-serializer/parse'
import { assertUsage } from '../../../utils/assert.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import {
  admitPublish,
  fitsPublish,
  hold,
  letGo,
  maxPendingBytesOf,
  queueMemory,
  type MemoryHolder,
} from './channelMemory.js'
import { isPromise } from '../../../utils/isPromise.js'
import { Listeners, type Registration } from '../../../utils/Listeners.js'
import { ChannelClosedError, ChannelOverflowError } from '../channelErrors.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { ACK_STATUS, ERROR_REASON, encode, type AckResultStatus } from '../../wire/frame.js'
import { TAG } from '../../wire/tags.js'
import type { ChannelDataFrame, ErrorReason } from '../../wire/frame.js'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import { PublishBody } from './PublishBody.js'
import { LossyDelivery } from './LossyDelivery.js'
import type { IndexedPeer } from './IndexedPeer.js'
import type { AttachState, Want } from '../../wire/payloads.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { isAbort } from '../../../node/server/Abort.js'
import type { EndingFrame } from '../ChannelEnding.js'
assertIsNotBrowser()

const SERVER_BROADCAST_BRAND: unique symbol = Symbol.for('ServerBroadcast')
const NOT_EVEN_NEWEST = 'the server could not hold even the newest message for its client'
const FELL_BEHIND = 'its client fell behind what the server holds for it; `lossy: true` lets it skip to the newest'
const NOT_IN_EFFECT =
  'Broadcast closed: its client subscribed, then made a telefunction call that ended before the subscription took effect'
const NOT_IN_EFFECT_IN_TIME =
  'Broadcast closed: its subscription did not take effect within `connectTtl` of a telefunction call its client made after subscribing'

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
  /** Its subscription to the key's publishes on its backend, while its page or a subscriber of it wants them. */
  private textListening: Listening | null = null
  private binaryListening: Listening | null = null
  /** Its subscribers waiting for their kind's subscription to take effect. */
  private readonly textWaiting = new Set<Outcome>()
  private readonly binaryWaiting = new Set<Outcome>()
  /** The page's want it applied last: which of the key's publishes go to the page. */
  private pageWant: Readonly<Want> = { version: 0, text: false, binary: false }

  /** Past what its wire takes to confirm, a newer publish replaces those waiting, on the way to its page and on the page's
   *  way up. */
  readonly lossy: boolean
  private readonly lossyPublishes: LossyDelivery<PublishBody> | null

  constructor(opts: BroadcastChannelOptions) {
    super()
    const { key, lossy = false } = opts
    assertUsage(typeof lossy === 'boolean', "`new BroadcastChannel()`'s `lossy` should be a boolean")
    this.key = key
    this.lossy = lossy
    this.lossyPublishes = lossy
      ? new LossyDelivery({
          peer: () => this.peer,
          replay: () => this._replayBuffer,
          hand: (body) => this.lastPeer!.sendPublish(body),
          hold: (body) => hold(this, body),
          letGo: (body) => letGo(this, body),
          dropReplaceable: () => this.dropUnhanded(),
          // A broadcast's publishes count against no credit.
          outOfCredit: () => false,
        })
      : null
  }

  /** Why `shed` ended it, which its page has yet to learn: see `shed`. */
  private overflowed: string | null = null
  /** Its abort, owed to its page: an overflow takes its place where its page would miss a message (`shed`). */
  private owesAbort = false
  /** While it owes its page that abort, the page's channel on the wire it reads, which the overflow then goes on. */
  private endedOn: IndexedPeer | null = null
  /** The page's channel on the wire it last attached on: a publish while the page is away joins the replay through it,
   *  as one a silently dead wire lost does. */
  private lastPeer: IndexedPeer | null = null
  /** The publishes its page wanted before its first attach (a call of the page's brought the want): that attach sends
   *  those its page still wants, ahead of the close where the broadcast closed meanwhile, as a plain channel does. */
  private beforeAttach: PublishBody[] = []
  /** The calls waiting for its page's want to take effect, which go on once it does or the broadcast ends. */
  private readonly callWaits = new Set<() => void>()

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
    const serialized = stringifyValue(data, 'publish')
    const ret = this.lifecycle.trackAck(Promise.resolve(this.publishBroadcast(serialized)))
    ret.catch(() => {})
    return ret
  }

  override messages(options?: { signal?: AbortSignal }) {
    return broadcastMessages<ChannelData<T>>(this, options, queueMemory())
  }

  /** In effect once its backend delivers what any server publishes to the key. */
  subscribe(callback: BroadcastListener<T>, options: { signal?: AbortSignal } = {}): Unsubscribe {
    return this.addSubscriber(this.broadcastListeners, false, callback, options)
  }

  publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    const ret = this.lifecycle.trackAck(Promise.resolve(this.publishBinaryBroadcast(data)))
    ret.catch(() => {})
    return ret
  }

  subscribeBinary(callback: BroadcastBinaryListener, options: { signal?: AbortSignal } = {}): Unsubscribe {
    return this.addSubscriber(this.broadcastBinaryListeners, true, callback, options)
  }

  /** Its `opened` settles as a page's does: once the backend delivers to it what any server publishes to the key, or once
   *  it is removed, with the end as known then where the close callbacks were called; one still registered rejects
   *  where the broadcast ends first. One added to an ended broadcast never takes effect. */
  private addSubscriber<L>(
    listeners: Listeners<L>,
    binary: boolean,
    callback: L,
    options: { signal?: AbortSignal },
  ): Unsubscribe {
    if (options.signal?.aborted) return unsubscribeOf(() => {})
    if (this._phase === 'ended') return neverOpened(this.endError())
    const waiting = binary ? this.binaryWaiting : this.textWaiting
    const listening = binary ? this.binaryListening : this.textListening
    const outcome = new Outcome()
    // Both before the backend starts: it may deliver a message before its subscribe returns, to which a listener may end
    // the broadcast, which settles what waits with that end.
    if (listening && !listening.opened) outcome.settle(true)
    else waiting.add(outcome)
    const remove = listeners.add(callback, {
      ...options,
      onRemove: () => {
        // Removed, it can no longer take effect: once its close callbacks were called, that is the end as known now.
        waiting.delete(outcome)
        outcome.settle(this.lifecycle.closeFired ? this.endError() : true)
        this.unsubscribeUnusedBroadcasts()
      },
    })
    if (binary) this.subscribeBinaryBroadcast()
    else this.subscribeBroadcast()
    return unsubscribeOf(remove, outcome)
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

  /** `listeners`: the ones a publish made now reaches, also one added while it's delivered, as a server subscriber is. */
  private deliverBroadcastMessage(
    serialized: string,
    rawInfo: WirePublishInfo,
    listeners: Iterable<Registration<BroadcastListener<T>>>,
  ): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    const data = parse(serialized) as ChannelData<T>
    for (const { listener } of listeners) {
      if (!listener) continue
      if (this.callListener(listener, data, info)) return
    }
    if (!this.takesPublish(this.pageWant.text)) return this.endOverflowed()
    const body = bodyOf(encode.publish, serialized, rawInfo, this.owner, (holder) => this.replaceIn(holder, false))
    this.forwardPublish(body)
  }

  private deliverBroadcastBinaryMessage(
    data: Uint8Array,
    rawInfo: WirePublishInfo,
    listeners: Iterable<Registration<BroadcastBinaryListener>>,
  ): void {
    const info = makePublishInfo(this.key, rawInfo.seq, rawInfo.timestamp)
    for (const { listener } of listeners) {
      if (!listener) continue
      // A copy of its own, which a Node.js Buffer's slice() isn't: a listener may write into it or transfer its buffer.
      if (this.callListener(listener, new Uint8Array(data), info)) return
    }
    if (!this.takesPublish(this.pageWant.binary)) return this.endOverflowed()
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

  /** Whether a publish of the kind its page subscribed to (`subscribed`) is the page's. One made after its close began
   *  is not, and costs it nothing: this is asked before the body is made, which admits it against the budget. */
  private takesPublish(subscribed: boolean): boolean {
    return subscribed && this._phase === 'open'
  }

  /** A publish to a page that takes it (`takesPublish`). Where admitting this body or one before it cut the channel, it
   *  ends now and sends nothing, and the subscribers after it still get theirs. Where the server can't hold it for the
   *  page, the page ends as one that missed it. */
  private forwardPublish(body: PublishBody): void {
    if (this.overflowed !== null) return this.endOverflowed()
    if (!fitsPublish(body)) return this.overflow(this.lossy ? NOT_EVEN_NEWEST : FELL_BEHIND, this.owner)
    if (this.lastPeer === null) return this.holdBeforeAttach(body)
    if (this.lossyPublishes) return this.lossyPublishes.send(body)
    this.lastPeer!.sendPublish(body)
    const most = maxPendingBytesOf()
    if (this.heldBytes > most) this.shed(most, this.owner)
  }

  /** Holds a publish for the page's first attach: a lossy one only the newest; past `maxPendingBytes`, none, and the
   *  page ends as one that missed them. */
  private holdBeforeAttach(body: PublishBody): void {
    if (this.lossy) this.dropBeforeAttach()
    this.beforeAttach.push(body)
    hold(this, body)
    if (this.heldBytes <= maxPendingBytesOf()) return
    this.dropBeforeAttach()
    this.overflow(this.lossy ? NOT_EVEN_NEWEST : FELL_BEHIND, this.owner)
  }

  /** As its replay's `dropFromGap`, for what it holds for its first attach: all of it, where it holds more than
   *  `toBytes` and more than one waits (any at 0). True where any went. */
  private dropBeforeAttachFromGap(toBytes: number): boolean {
    const mayGo = toBytes === 0 ? this.beforeAttach.length : this.beforeAttach.length - 1
    if (mayGo <= 0 || this.heldBytes <= toBytes) return false
    this.dropBeforeAttach()
    return true
  }

  private dropBeforeAttach(): void {
    for (const body of this.beforeAttach) letGo(this, body)
    this.beforeAttach = []
  }

  /** A graceful close goes behind what waits for its page, so a lossy one first replaces that with the newest. An abort
   *  drops what waits for a lossy one's page, and goes behind it for a reliable one (`onAbortOwed`). */
  override close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    this.endOverflowed()
    if (this._phase === 'open') this.sendNewestOnly()
    return super.close(opts)
  }

  override abort(abortValue?: unknown, message?: string): void {
    this.endOverflowed()
    super.abort(abortValue, message)
  }

  /** Leaves a lossy page only the newest publish to wait for: what waits for its wire goes, and the newest is sent again. */
  private sendNewestOnly(): void {
    const publishes = this.lossyPublishes
    if (publishes === null) return
    if (this.peer !== null && publishes.held === null && this.unhanded() > 1) {
      const newest = this._replayBuffer!.newestPublish()!
      publishes.letWaitingGo()
      this.peer.sendPublish(newest)
    }
    publishes.flush()
  }

  protected override onPageHas(lastSeq: number): void {
    super.onPageHas(lastSeq)
    this.lossyPublishes?.mayGo()
  }

  /** A reliable broadcast's publishes go ahead of its ABORT, as a channel's reliable sends do; a lossy one's go, with
   *  what it held for its first attach. */
  protected override onAbortOwed(): void {
    this.owesAbort = true
    this.endedOn = this.peer
    if (!this.lossy) return
    this.dropBeforeAttach()
    super.onAbortOwed()
  }

  /** Its publishes that no wire has taken go, unless it ended: its replay is then what its page reads. */
  protected override dropUnhanded(): void {
    if (this._phase === 'ended') return
    this.dropBeforeAttach()
    if (this.peer) this.peer.dropUnhanded()
    else this._replayBuffer?.dropNewestPublishes(this.unhanded())
  }

  /** Where `holder` is a lossy page this publish reaches (its key, on its object), it lets go of what the publish
   *  replaces. */
  private replaceIn(holder: MemoryHolder, binary: boolean): void {
    if (!ServerBroadcast.isServerBroadcast(holder) || !holder.lossyPublishes || holder.key !== this.key) return
    if (holder.owner !== this.owner) return
    if (holder.takesPublish(binary ? holder.pageWant.binary : holder.pageWant.text)) {
      holder.lossyPublishes.letWaitingGo()
    }
  }

  /** How many of its newest publishes no wire has: all of them while it has no wire. Ended, those of the wire its page
   *  reads its abort on. */
  private unhanded(): number {
    return (this.peer ?? this.endedOn)?.unhanded() ?? Number.POSITIVE_INFINITY
  }

  /** Its newest publish, where no wire has that yet: before its first attach, the newest it holds for that. */
  get newestUnhanded(): PublishBody | undefined {
    if (this._phase === 'ended' || !this._replayBuffer) return undefined
    return (
      this.lossyPublishes?.held ??
      this.beforeAttach.at(-1) ??
      (this.unhanded() === 0 ? undefined : this._replayBuffer.newestPublish())
    )
  }

  /** The request whose server its page is on: its wire's, else, till its page attaches, the call's that carried it. */
  private get ownerRequest(): Request | undefined {
    return this.wireRequest ?? this._callRequest
  }

  private get owner(): string | undefined {
    return this.adapter?.ownerOf?.(this.ownerRequest)
  }

  /** `MemoryHolder`: its page gets every publish, so where it would miss one no wire has, the channel ends: from that gap
   *  on everything goes, receipts too, and its page reads on up to the gap and no further. A lossy one holds no publish
   *  waiting but its newest, so it ends only at 0, where not even that fits. The bytes go at once and the channel ends
   *  after the call that cut it (`overflow`). Ended by its abort, it is cut the same way, and the overflow takes the
   *  abort's place; the abort alone needs no memory, so without a gap it ends as it was going to. */
  shed(toBytes: number, owner?: string): void {
    const replay = this._replayBuffer
    const ended = this._phase === 'ended'
    if (!replay || (ended && !this.owesAbort) || (this.lossy && toBytes > 0)) return
    if (toBytes === 0) this.lossyPublishes?.dispose()
    const gap = this.dropBeforeAttachFromGap(toBytes) || replay.dropFromGap(toBytes, this.unhanded()) > 0
    const peer = this.peer ?? this.endedOn
    if (gap) {
      peer?.sender.queue.dropWaitingFrom(peer.index, 'publish')
      this.dropPendingFrames()
    }
    if (toBytes === 0 && !ended) this.overflow(NOT_EVEN_NEWEST, owner)
    else if (gap) this.overflow(FELL_BEHIND, owner)
  }

  /** What it held for its first attach goes with the frames it held for that. */
  protected override dropPendingFrames(): void {
    this.dropBeforeAttach()
    super.dropPendingFrames()
  }

  /** An answer past the gap would cross it: the channel ends first, and the publish it answers settles with that. */
  protected override sendAckRes(ackedSeq: number, result: string, status?: AckResultStatus): void {
    if (this.overflowed !== null) return this.endOverflowed()
    super.sendAckRes(ackedSeq, result, status)
  }

  /** It ends after the call that cut it, so no close callback runs inside that call: at its next event (a delivery of its
   *  key, its heartbeat, its attach, its answer, its close running out, the app's `close()` or `abort()`), a microtask
   *  later at the latest. Only its own object can write to its page, so one cut for another `owner`'s publish waits for
   *  that event. */
  private overflow(why: string, owner: string | undefined): void {
    this.overflowed ??= why
    if (owner === this.owner) queueMicrotask(() => this.endOverflowed())
  }

  private endOverflowed(): void {
    if (this.overflowed === null) return
    if (this._phase !== 'ended') {
      this.endWithError(ERROR_REASON.OVERFLOW, new ChannelOverflowError(`Broadcast closed: ${this.overflowed}`))
      return
    }
    if (!this.owesAbort) return
    this.owesAbort = false
    this.replaceEnd(ERROR_REASON.OVERFLOW, this.endedOn)
    this.endedOn = null
  }

  /** Applies a want newer than the one it has: the wire and a call can each bring the page's. A closing one takes none,
   *  as it delivers nothing more, and an ended one would listen to its backend with nothing left to stop it. */
  protected override onPeerWant(want: Want): void {
    if (this._phase === 'open' && want.version > this.pageWant.version) this.applyPageWant(want)
  }

  private applyPageWant(want: Want): void {
    this.pageWant = want
    if (want.text) this.subscribeBroadcast(true)
    if (want.binary) this.subscribeBinaryBroadcast(true)
    // A page that reads none of its publishes any more would only drop those still waiting for its wire.
    if (!want.text && !want.binary) this.dropUnhanded()
    this.unsubscribeUnusedBroadcasts()
  }

  /** Tells the page its want is in effect, and the calls waiting for it, where its backend delivers what any server
   *  publishes to the key already; else that waits for the backend (`whenListening`). A page that never asked for one has
   *  nothing to confirm. */
  override _confirmWant(): void {
    if (this.pageWant.version === 0 || !this.pageWantInEffect()) return
    this.peer?.sendWanted(this.pageWant.version)
    for (const goOn of this.callWaits) goOn()
    this.callWaits.clear()
  }

  private pageWantInEffect(): boolean {
    const { text, binary } = this.pageWant
    return !(text && this.textListening?.opened) && !(binary && this.binaryListening?.opened)
  }

  /** The call waits until the want it carries is in effect, or the broadcast ended. Where the call's request goes first
   *  (its page left, or aborted the call), or the call waited `waitAtMost` (`connectTtl`), the broadcast ends now with an
   *  error its page reads: its telefunction runs without the subscription. */
  override _applyCallWant(want: Want, callAborted: AbortSignal, waitAtMost: number): Promise<void> | undefined {
    this.onPeerWant(want)
    this._confirmWant()
    // A want a closing broadcast didn't take never takes effect: the call runs once the broadcast ended.
    if (want.version > this.pageWant.version) return this.closed.then(() => {})
    return this.pageWantInEffect() ? undefined : this.inEffectOrEnded(callAborted, waitAtMost)
  }

  /** Every way the wait ends comes past its `await`, which drops its timer and abort listener. */
  private async inEffectOrEnded(callAborted: AbortSignal, waitAtMost: number): Promise<void> {
    const inEffect = createDeferred()
    this.callWaits.add(inEffect.resolve)
    const end = (message: string) => this.endWithError(ERROR_REASON.NOT_IN_EFFECT, new ChannelClosedError(message))
    const endAborted = () => end(NOT_IN_EFFECT)
    if (callAborted.aborted) endAborted()
    else callAborted.addEventListener('abort', endAborted, { once: true })
    const timer = unrefTimer(setTimeout(() => end(NOT_IN_EFFECT_IN_TIME), waitAtMost))
    await inEffect.promise
    clearTimeout(timer)
    callAborted.removeEventListener('abort', endAborted)
  }

  private unsubscribeUnusedBroadcasts(): void {
    if (!this.pageWant.binary && this.broadcastBinaryListeners.size === 0) {
      this.binaryListening?.unsubscribe()
      this.binaryListening = null
    }
    if (!this.pageWant.text && this.broadcastListeners.size === 0) {
      this.textListening?.unsubscribe()
      this.textListening = null
    }
  }

  /** What it held for its first attach goes first, of the kinds the attach's want keeps. */
  protected override sendHeldForAttach(peer: IndexedPeer): void {
    if (this._phase !== 'ended') this.lastPeer = peer
    const held = this.beforeAttach
    this.dropBeforeAttach()
    for (const body of held)
      if (this.pageWant[body.frame[0] === TAG.PUBLISH ? 'text' : 'binary']) peer.sendPublish(body)
  }

  override _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    if (this.owesAbort && this._phase === 'ended') this.endedOn = peer
    // Attached again, what its want brings goes to its new wire at once; on a first attach, behind what was held.
    if (this.lastPeer !== null && this._phase !== 'ended') this.lastPeer = peer
    super._attachPeer(peer, state)
    this.endOverflowed()
  }

  override _acknowledge(): void {
    super._acknowledge()
    this.endOverflowed()
  }

  /** @internal Its page needs nothing more of it: no overflow can take the abort's place, and it lets go of the wire. */
  override _release(): void {
    this.owesAbort = false
    this.endedOn = null
    super._release()
  }

  /** A wire that closed has none of its publishes: once ended, its page away is cut as an open one's (`unhanded`). */
  override _onPeerDisconnect(peer: IndexedPeer, reconnectTimeout: number): void {
    if (this.endedOn?.sender === peer.sender) this.endedOn = null
    super._onPeerDisconnect(peer, reconnectTimeout)
  }

  /** Where its close ran out, one that was cut ends with why, which its page reads; a close its page asked for or
   *  answered ends gracefully. */
  override _end(err?: Error): void {
    if (err !== undefined && this.overflowed !== null) return this.endOverflowed()
    super._end(err)
  }

  /** What it held for its first attach goes with the rest it buffered: an error ends it without them. */
  protected override endWithError(reason: ErrorReason, err: Error): void {
    if (this._phase !== 'ended') this.dropBeforeAttach()
    super.endWithError(reason, err)
  }

  /** Its close running out is an event that ends one that was cut with why, in the timeout's place. */
  override _sendEnding(frame: EndingFrame): void {
    if (frame.kind === 'timedOut' && this.overflowed !== null) return this.endOverflowed()
    super._sendEnding(frame)
  }

  override _finalize(err: Error | undefined): void {
    if (this.endsPageGone) this.dropBeforeAttach()
    this.lossyPublishes?.dispose()
    this.lastPeer = null
    this.textListening?.unsubscribe()
    this.textListening = null
    this.binaryListening?.unsubscribe()
    this.binaryListening = null
    super._finalize(err)
    // What waits gets the end: one its close callbacks removed or added too.
    const endError = this.endError()
    settleAll(this.textWaiting, endError)
    settleAll(this.binaryWaiting, endError)
    for (const goOn of this.callWaits) goOn()
    this.callWaits.clear()
    this.broadcastListeners.clear()
    this.broadcastBinaryListeners.clear()
  }

  // --- Internal broadcast helpers ---

  /** The installed adapter, which the channel keeps from its first use. */
  private broadcastAdapter(): BroadcastAdapter {
    this.adapter ??= getBroadcastAdapter()
    return this.adapter
  }

  /** `forPage`: a subscription its backend refuses at once fails as one it gives up later does (`whenListening`),
   *  where a server subscriber's `subscribe()` throws the refusal instead. */
  private subscribeBroadcast(forPage = false): void {
    if (this.textListening) return
    this.textListening = starting()
    this.startListening(this.textListening, this.broadcastListeners, this.textWaiting, forPage, () =>
      this.broadcastAdapter().subscribe(
        this.key,
        Object.assign(
          (serialized: string, rawInfo: WirePublishInfo) =>
            this.deliverBroadcastMessage(serialized, rawInfo, this.broadcastListeners.live()),
          // A publish that waits goes to the listeners there are when it's made.
          {
            taken: () => {
              const listeners = this.broadcastListeners.list()
              return (serialized: string, rawInfo: WirePublishInfo) =>
                this.deliverBroadcastMessage(serialized, rawInfo, listeners)
            },
          },
        ),
        this.ownerRequest,
      ),
    )
  }

  private subscribeBinaryBroadcast(forPage = false): void {
    if (this.binaryListening) return
    this.binaryListening = starting()
    this.startListening(this.binaryListening, this.broadcastBinaryListeners, this.binaryWaiting, forPage, () =>
      this.broadcastAdapter().subscribeBinary(
        this.key,
        Object.assign(
          (data: Uint8Array, rawInfo: WirePublishInfo) =>
            this.deliverBroadcastBinaryMessage(data, rawInfo, this.broadcastBinaryListeners.live()),
          {
            taken: () => {
              const listeners = this.broadcastBinaryListeners.list()
              return (data: Uint8Array, rawInfo: WirePublishInfo) =>
                this.deliverBroadcastBinaryMessage(data, rawInfo, listeners)
            },
          },
        ),
        this.ownerRequest,
      ),
    )
  }

  /** Starts `listening`, which holds its kind's place while its backend starts: a message the backend delivers meanwhile
   *  may add a subscriber of the kind, which waits in `waiting` with the others, or end the broadcast, or remove the
   *  kind's last subscriber, either of which stops it once started. Where a server subscriber's start throws, the place
   *  is given back, and the kind's subscribers, which all came for a start of the key that failed, get the error and are
   *  removed. */
  private startListening<L>(
    listening: Listening,
    listeners: Listeners<L>,
    waiting: Set<Outcome>,
    forPage: boolean,
    subscribe: () => Listening,
  ): void {
    let started: Listening
    try {
      started = listeningOrFailure(forPage, subscribe)
    } catch (err) {
      if (this.textListening === listening) this.textListening = null
      if (this.binaryListening === listening) this.binaryListening = null
      settleAll(waiting, err instanceof Error ? err : new Error(String(err)))
      listeners.clear()
      throw err
    }
    listening.unsubscribe = started.unsubscribe
    if (!this.isListening(listening)) return listening.unsubscribe()
    listening.opened = started.opened
    if (started.opened) void this.whenListening(listening, started.opened, waiting)
    else settleAll(waiting, true)
  }

  private isListening(listening: Listening): boolean {
    return this.textListening === listening || this.binaryListening === listening
  }

  /** Once its backend delivers to `listening` what any server publishes to the key, the subscribers in `waiting` and
   *  the page's want are in effect. A subscription its backend failed stays failed (Redis doesn't try it again), so the
   *  broadcast ends: with the backend's error on the server, reported as a server error, and on its page as a
   *  subscription that didn't take effect; every `opened` waiting rejects with that end. One removed meanwhile left `waiting` at its removal. */
  private async whenListening(listening: Listening, opened: Outcome, waiting: Set<Outcome>): Promise<void> {
    try {
      await opened.promise
    } catch (err) {
      if (!this.isListening(listening)) return
      handleCallbackBug(err)
      return this.endWithError(ERROR_REASON.NOT_IN_EFFECT, err instanceof Error ? err : new Error(String(err)))
    }
    if (!this.isListening(listening)) return
    listening.opened = undefined
    settleAll(waiting, true)
    this._confirmWant()
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
  readonly closed: Promise<ChannelCloseResult>
  messages(options?: { signal?: AbortSignal }): AsyncIterableIterator<
    ({ type: 'text'; data: ChannelData<T> } | { type: 'binary'; data: Uint8Array }) & { info: ChannelPublishInfo }
  >
  readonly [TELEFUNC_SHIELDS]: {
    data: ChannelData<T>
    ack: unknown
  }
  /** Serializes `data` as JSON: bytes go through `publishBinary()`. */
  publish(data: ChannelData<T>): Promise<ChannelPublishAck>
  /** Receive `(data, info)` for each publish to the key. The subscription is in effect once it gets what any server
   *  publishes to the key (`opened`): while the broadcast backend is unreachable, that waits, and where the backend gives
   *  it up, the broadcast ends: with its error on the server, and on its page with a `ChannelClosedError` that says its
   *  subscription didn't take effect. A telefunction the page calls after `subscribe()` runs once the
   *  subscription is in effect, or once the broadcast ended.
   *
   *  https://telefunc.com/channel#chat-example */
  subscribe(callback: BroadcastListener<T>, options?: { signal?: AbortSignal }): Unsubscribe
  publishBinary(data: Uint8Array): Promise<ChannelPublishAck>
  /** As `subscribe()`, for binary publishes. */
  subscribeBinary(callback: BroadcastBinaryListener, options?: { signal?: AbortSignal }): Unsubscribe
  onClose(callback: ChannelCloseCallback, options?: { signal?: AbortSignal }): () => void
  onOpen(callback: () => void, options?: { signal?: AbortSignal }): () => void
  close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult>
  abort(): void
  abort(abortValue: unknown, message?: string): void
}

type BroadcastChannelOptions = {
  key: string
  /**
   * Where only the newest message matters: a client that keeps up gets every message, and one that falls behind skips
   * to the newest, with `info.seq` jumping over what it skipped. This channel's client skips whoever publishes, while a
   * client of the same key without it gets every message, or its channel closes with `ChannelOverflowError` past what
   * the server holds for it.
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
  /** Serializes `data` as JSON: bytes go through `publishBinary()`. */
  publish<U = unknown>(key: string, data: ChannelData<U>): ChannelPublishAck | Promise<ChannelPublishAck> {
    const adapter = getBroadcastAdapter()
    const serialized = stringifyValue(data, 'publish')
    return receiptOf(key, adapter.publish(key, serialized))
  },
  /** Receive `(data, info)` for each publish to `key`, in effect once it gets what any server publishes to `key`. */
  subscribe<U = unknown>(key: string, callback: BroadcastListener<U>, options?: { signal?: AbortSignal }): Unsubscribe {
    const adapter = getBroadcastAdapter()
    if (options?.signal?.aborted) return unsubscribeOf(() => {})
    const listening = adapter.subscribe(key, (serialized, info) => {
      const data = parse(serialized) as ChannelData<U>
      callStaticListener(callback, data, makePublishInfo(key, info.seq, info.timestamp))
    })
    return removedOnAbort(listening, options?.signal)
  },
  publishBinary(key: string, data: Uint8Array): ChannelPublishAck | Promise<ChannelPublishAck> {
    const adapter = getBroadcastAdapter()
    return receiptOf(key, adapter.publishBinary(key, data))
  },
  /** As `subscribe()`, for binary publishes. */
  subscribeBinary(key: string, callback: BroadcastBinaryListener, options?: { signal?: AbortSignal }): Unsubscribe {
    const adapter = getBroadcastAdapter()
    if (options?.signal?.aborted) return unsubscribeOf(() => {})
    const listening = adapter.subscribeBinary(key, (data, info) => {
      // A copy of its own, as a `BroadcastChannel` listener gets.
      callStaticListener(callback, new Uint8Array(data), makePublishInfo(key, info.seq, info.timestamp))
    })
    return removedOnAbort(listening, options?.signal)
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

/** The subscription `subscribe` starts; where it throws and `orFailure`, one whose `opened` rejects with that. */
function listeningOrFailure(orFailure: boolean, subscribe: () => Listening): Listening {
  try {
    return subscribe()
  } catch (err) {
    if (!orFailure) throw err
    // The broadcast it fails reports it (`whenListening`).
    return { unsubscribe: () => {}, opened: Outcome.failed(err instanceof Error ? err : new Error(String(err))) }
  }
}

/** A subscription its backend is starting: not in effect yet, with nothing to stop. */
function starting(): Listening {
  return { unsubscribe: () => {}, opened: new Outcome() }
}

function settleAll(waiting: Set<Outcome>, outcome: true | Error): void {
  for (const opened of waiting) opened.settle(outcome)
  waiting.clear()
}

/** Unsubscribes once `signal` aborts, at once if it aborted while subscribing, or when the returned function runs; its
 *  `opened` resolves once the subscription is in effect or removed, and rejects where its backend failed it first. */
function removedOnAbort({ unsubscribe, opened, removed }: Listening, signal: AbortSignal | undefined): Unsubscribe {
  const outcome = opened && new Outcome(opened)
  let current: (() => void) | undefined = unsubscribe
  const remove = () => {
    const pending = current
    current = undefined
    signal?.removeEventListener('abort', remove)
    // Ahead of the transport's remover, which may fail the wait it abandons.
    outcome?.settle(opened?.failure ?? true)
    pending?.()
  }
  // Off `signal` once the adapter removed the subscription itself, as when the app removes it.
  signal?.addEventListener('abort', remove, { once: true, signal: removed })
  if (signal?.aborted) remove()
  return unsubscribeOf(remove, outcome)
}
