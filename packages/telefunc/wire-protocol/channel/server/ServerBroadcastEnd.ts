export { ServerBroadcastEnd }
export type { BroadcastEndOwner }

import { ServerChannel } from './ServerChannel.js'
import { getChannelMux, type ChannelMux } from './ChannelMux.js'
import { assert } from '../../../utils/assert.js'
import type { IndexedPeer } from './IndexedPeer.js'
import { maxPendingBytesOf, type MemoryHolder } from './channelMemory.js'
import { LossyDelivery } from './LossyDelivery.js'
import type { PublishBody } from './PublishBody.js'
import { CHANNEL_RECONNECT_TIMEOUT_MS, CREDIT_WINDOW_MAX_BYTES, MAX_DOMAINS_PER_CHANNEL } from '../../constants.js'
import { ChannelOverflowError, isChannelOverflowError } from '../channelErrors.js'
import { isShieldValidationError } from '../../../shared/ShieldValidationError.js'
import { ACK_STATUS, ERROR_REASON, encode, type AckResultStatus } from '../../wire/frame.js'
import {
  parseDomainRequest,
  type BroadcastKind,
  type AttachState,
  type DomainAnnouncement,
} from '../../wire/payloads.js'
import type { ChannelCloseOptions, ChannelCloseResult } from '../channel.js'
import { assertProtocol } from '../../wire/protocolViolation.js'

const NOT_EVEN_NEWEST = 'the server could not hold even the newest message for its client'
const FELL_BEHIND = 'its client fell behind what the server holds for it; `lossy: true` lets it skip to the newest'

/** What a page's publishes, subscriptions and attaches go to. */
type BroadcastEndOwner = {
  /** What it returns answers the page's `publish()`. */
  _onPagePublish(serialized: string): unknown
  _onPagePublishBinary(data: Uint8Array): unknown
  /** The page subscribes or unsubscribes `kind`, and declares both at each attach. */
  _onPageSubscription?(kind: BroadcastKind, on: boolean): void
  /** Each attach of the page while the channel is open, after the page's subscriptions applied. */
  _onPageAttach?(): void
  /** A newer publish of a lossy stream took the place of one its page hadn't taken. */
  _onPublishesMissed?(): void
}

/** The server end of a page's `ClientBroadcast`: a channel to that page, which also takes the page's publishes and
 *  subscriptions, and delivers publishes to it. */
class ServerBroadcastEnd extends ServerChannel implements MemoryHolder {
  private owner: BroadcastEndOwner | null = null
  /** The page's channel on the wire it last attached on: a publish while the page is away joins the replay through it,
   *  as one a silently dead wire lost does. */
  private lastPeer: IndexedPeer | null = null
  /** Why it ends, where `shed` couldn't tell its page yet: see `shed`. */
  private overflowed: string | null = null
  /** Its streams' domains (see `TAG.DOMAIN`), by stream: each a channel to its page that carries the stream's
   *  publishes, so that no other stream's loss delays them. */
  private readonly domains = new Map<string, ServerBroadcastEnd>()
  /** The end whose stream it carries, where it is a domain: it lives, overflows and takes page publishes as that end. */
  private parent: ServerBroadcastEnd | null = null
  /** Its lossy streams' publishes on their way to its page. */
  private readonly lossy = new LossyDelivery({ holder: this, onSkip: () => this.owner?._onPublishesMissed?.() })

  setOwner(owner: BroadcastEndOwner): void {
    this.owner = owner
  }

  get isAttached(): boolean {
    return this.peer !== null
  }

  /** Ends its page's wire, as a frame that breaks the protocol does. */
  terminateWire(): void {
    this.peer?.sender.terminate()
  }

  /** @internal Its close waits for `promise`, as for an ack's answer. */
  trackAck<T>(promise: Promise<T>): Promise<T> {
    return this.lifecycle.trackAck(promise)
  }

  /** @internal A callback's `Abort` aborts the channel: true where `err` is one. */
  _handleCallbackError(err: unknown): boolean {
    return this.lifecycle.handleCallbackError(err)
  }

  /** A publish to its page, never refused, and held while the page is away. Its listeners or admitting its body can have
   *  ended the channel by now. */
  deliver(body: PublishBody): void {
    this.endOverflowed()
    if (this._phase === 'ended') return
    // A domain carries its stream's bodies itself.
    const domain =
      body.domain === null || this.parent !== null ? undefined : this.domainOf(body.domain, body.lossyStream !== null)
    if (domain) return domain.deliver(body)
    if (body.lossyStream !== null) return this.deliverLossy(body, body.lossyStream)
    this.forward(body)
    const share = this.share()
    if (this.heldBytes > share) this.shed(share)
  }

  /** A page back from a lost wire gets its stream's newest publish, not those the lost wire carried, or with `delta`
   *  those of its group too. */
  private deliverLossy(body: PublishBody, stream: string): void {
    if (this.peer === null && !body.delta && (this._replayBuffer?.dropPublishesOf(stream) ?? 0) > 0) {
      this.owner?._onPublishesMissed?.()
    }
    this.lossy.publish(body)
  }

  private forward(body: PublishBody): void {
    if (this.lastPeer) this.lastPeer.sendPublish(body)
    else this.offlineBuffer.pushPublish(body)
  }

  /** `stream`'s domain, opened and announced to its page the first time; undefined past `MAX_DOMAINS_PER_CHANNEL`, where
   *  the stream stays on this channel. */
  private domainOf(stream: string, lossy: boolean): ServerBroadcastEnd | undefined {
    const open = this.domains.get(stream)
    if (open !== undefined || this.domains.size >= MAX_DOMAINS_PER_CHANNEL) {
      return open
    }
    const owner = this.owner
    assert(owner)
    const domain = new ServerBroadcastEnd()
    domain.parent = this
    domain.setOwner({
      _onPagePublish: (serialized) => owner._onPagePublish(serialized),
      _onPagePublishBinary: (data) => owner._onPagePublishBinary(data),
      _onPublishesMissed: () => owner._onPublishesMissed?.(),
    })
    // One opened before this end registered registers with it.
    if (this.didRegister) this.registerDomain(domain)
    this.domains.set(stream, domain)
    this.announce({ stream, channelId: domain.id, lossy })
    return domain
  }

  /** On the mux, so the page that learns of it attaches it there. */
  private registerDomain(domain: ServerBroadcastEnd): void {
    if (this.mux) this.mux.registerChannel(domain)
    else domain._registerChannel()
  }

  /** The page asks for the domain of a stream it publishes on. An open one was announced as it opened, before or after
   *  the page asked; past the cap, an announcement of none answers. */
  protected override onPeerDomainRequest(text: string): void {
    // Else a page could open domains of domains, past every cap.
    assertProtocol(this.parent === null, 'DOMAIN on a domain')
    const { stream, lossy } = parseDomainRequest(text)
    if (this.domainOf(stream, lossy) === undefined) this.announce({ stream, channelId: null, lossy })
  }

  /** Sequenced on its own channel, so its page learns of a domain in order with what this channel carries. */
  private announce(announcement: DomainAnnouncement): void {
    const text = JSON.stringify(announcement)
    if (this.lastPeer) this.lastPeer.sendSequenced(encode.domain, text)
    else this.offlineBuffer.pushDomain(text)
  }

  /** A graceful close goes behind every publish its page is due; an abort may drop them. */
  override close(opts?: ChannelCloseOptions): Promise<ChannelCloseResult> {
    if (this._phase === 'open') this.lossy.flush()
    return super.close(opts)
  }

  /** `MemoryHolder`: on its object, a newer publish of `stream` takes the place of those waiting, as what another's
   *  holds waits for that object's own delivery. */
  replaceWaiting(stream: string): void {
    if (this._phase !== 'ended' && getChannelMux() === this.mux) this.lossy.skipWaiting(stream)
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
    return this.lossy.newestWaiting ?? (this.unhanded() === 0 ? undefined : this._replayBuffer.newestPublish())
  }

  /** `MemoryHolder`: its page gets every reliable publish, so where it would miss one no wire has, the channel ends: from
   *  that gap on everything goes, receipts too, and its page reads on up to the gap and no further. A lossy stream's
   *  newer publish replaces what of it waits (`replaceWaiting`), so a page only on those ends at 0, where not even that
   *  fits. The bytes go at once,
   *  but only its own object can write to its page, so from another's it ends when its own next runs. */
  shed(toBytes: number): void {
    const replay = this._replayBuffer
    if (this._phase === 'ended' || !replay) return
    if (toBytes === 0) this.lossy.drop()
    const gap = replay.dropFromGap(toBytes, this.unhanded()) > 0
    if (gap) {
      this.peer?.sender.queue.dropWaitingFromPublish(this.peer.index)
      this.dropPendingAnswers()
    }
    if (toBytes === 0) this.overflow(NOT_EVEN_NEWEST)
    else if (gap) this.overflow(FELL_BEHIND)
  }

  /** An answer past the gap would cross it: the channel ends first, and the publish it answers settles with that. */
  protected override sendAckRes(ackedSeq: number, result: string, status?: AckResultStatus): void {
    if (this.overflowed !== null) return this.endOverflowed()
    super.sendAckRes(ackedSeq, result, status)
  }

  private overflow(why: string): void {
    if (this.parent) return this.parent.overflow(why)
    this.overflowed ??= why
    if (getChannelMux() === this.mux) this.endOverflowed()
  }

  private endOverflowed(): void {
    if (this.overflowed === null) return
    this.endWithError(ERROR_REASON.OVERFLOW, new ChannelOverflowError(`Broadcast closed: ${this.overflowed}`))
  }

  /** @internal Its domains that opened before it registered register with it. */
  override _registerChannel(mux?: ChannelMux): void {
    super._registerChannel(mux)
    for (const domain of this.domains.values()) this.registerDomain(domain)
  }

  override _attachPeer(peer: IndexedPeer, state?: AttachState): void {
    this.lastPeer = peer
    super._attachPeer(peer, state)
    if (this._phase !== 'ended') this.lossy.attach(peer)
    this.endOverflowed()
    if (this.peer !== peer || this.isClosed) return
    assert(this.owner)
    this.owner._onPageAttach?.()
  }

  override _onPeerDisconnect(peer: IndexedPeer, reconnectTimeout: number): void {
    super._onPeerDisconnect(peer, reconnectTimeout)
    if (this.peer === null) this.lossy.detach()
  }

  override _acknowledge(): void {
    super._acknowledge()
    this.endOverflowed()
  }

  protected override shutdown(err?: Error, options?: { pageGone?: boolean }): void {
    for (const domain of this.domains.values()) domain._end(err)
    this.domains.clear()
    this.lossy.drop()
    this.lastPeer = null
    super.shutdown(err, options)
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    const owner = this.owner
    assert(owner)
    // What the publish carries is its owner's to hold, so its request holds only the room for its answer.
    return this.answerHeld(seq, 0, () => this.answerRequest(seq, [(t: string) => owner._onPagePublish(t)], text))
  }

  override _onPeerPublishBinaryAckReqMessage(data: Uint8Array, seq: number): Promise<void> {
    const owner = this.owner
    assert(owner)
    return this.answerHeld(seq, 0, () =>
      this.answerRequest(seq, [(d: Uint8Array) => owner._onPagePublishBinary(d)], data),
    )
  }

  /** A full buffer or a shield failure answers with the status its page's `publish()` rejects with. */
  protected override answerFailed(seq: number, err: unknown): void {
    if (isChannelOverflowError(err)) return this.sendAckRes(seq, err.message, ACK_STATUS.OVERFLOW)
    if (isShieldValidationError(err)) return this.sendAckRes(seq, err.message, ACK_STATUS.SHIELD_ERROR)
    super.answerFailed(seq, err)
  }

  protected override onPeerSubscription(kind: BroadcastKind, on: boolean): void {
    assert(this.owner)
    this.owner._onPageSubscription?.(kind, on)
  }
}
