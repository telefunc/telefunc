export { ClientRoom, ClientRoomParticipant, ClientStandaloneParticipant }

import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isObject } from '../../utils/isObject.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../channel.js'
import type { ClientBroadcast, ClientChannel } from '../client/channel.js'
import {
  leaveCauseFromWire,
  ParticipantBase,
  RoomState,
  frameWithMemberId,
  hasRoomTag,
  normalizeJoinOptions,
  sizeFromWire,
  unframeMemberId,
  type MemberWants,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type ReqJoinAck,
  type ReqOkAck,
  type ReqPublishAck,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomRosterEvent,
  type RoomSnapshotMetadata,
  type RoomStubRequest,
} from './shared.js'
import type {
  BinaryPublishOptions,
  RoomBinaryListener,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  RemoteParticipant,
  Room,
  RoomMeta,
  Sender,
} from './types.js'

// ---------------------------------------------------------------------------
// ClientRoom
// ---------------------------------------------------------------------------

/**
 * Client-side `Room`, revived from a serialized `ServerRoom`.
 *
 * Composes over a plain `ClientBroadcast` stub: room events & data arrive as its broadcast
 * messages, requests (join/leave/set-meta) ride its channel messages. Membership starts from
 * the serialized snapshot; the relayed event stream keeps it fresh from there.
 */
class ClientRoom implements Room {
  private readonly _stub: ClientBroadcast
  private readonly _state: RoomState
  private readonly _localParticipants = new Map<string, ClientRoomParticipant>()
  private _lastBinaryWantsSent = ''
  private _lastTextWantsSent = ''
  /** DMs relayed before their participant's join ack resolved (a reactive send racing the
   *  join round-trip) — held bounded FIFO, flushed into the participant on registration. */
  private _pendingDms: Array<{
    to: string
    from: string
    fromMeta: ParticipantMeta | null
    fromIdentity: string | null
    data: unknown
  }> | null = null
  private _rosterArrived!: () => void
  /** Settled by the first streamed roster (or wire death) — gates `getParticipants()`. */
  private readonly _rosterReady = new Promise<void>((resolve) => (this._rosterArrived = resolve))

  constructor(stub: ClientBroadcast, snapshot: RoomSnapshotMetadata) {
    this._stub = stub
    this._state = new RoomState({
      roomId: snapshot.roomId,
      meta: snapshot.meta,
      size: sizeFromWire(snapshot.size),
      seed: { count: snapshot.count }, // the roster itself streams right behind the response
      updateStamp: snapshot.stamp,
      closed: snapshot.closed,
      onListenersChanged: () => this._syncWants(),
      onCallbackError: reportRoomError,
    })
    this._state._owner = this
    if (snapshot.closed) this._rosterArrived()

    // Delivery handlers are local-only — what the server relays is driven by the declared
    // wants: control always arrives, text while subscribed, binary per `sub-binary`.
    stub._subscribeLocal((envelope, info) => this._onEnvelope(envelope, info))
    stub._subscribeBinaryLocal((framed, info) => this._onBinaryFrame(framed, info))
    // Wire death — the network gave up or the stub was GC'd. (A server `Room.close()` arrives
    // as the `closed` ctrl event before the stub shuts down, so it takes the 'closed' path.)
    stub.onClose(() => this._applyClosed('disconnected'))
  }

  // ── Room API ──

  get id(): string {
    return this._state.roomId
  }
  get meta(): RoomMeta {
    return this._state.meta
  }
  get size(): number {
    return this._state.size
  }
  get count(): number {
    return this._state.count
  }
  get isEmpty(): boolean {
    return this._state.count === 0
  }
  get isFull(): boolean {
    return this._state.isFull
  }
  get isClosed(): boolean {
    return this._state.closed
  }

  async join(meta: ParticipantMeta = {}, options?: JoinOptions): Promise<LocalParticipant> {
    if (options?.identity !== undefined) {
      throw new Error(
        'join() options.identity is server-assigned: identity is trusted, so set it where trust lives — in the granting telefunction (server-side join()), not on the client.',
      )
    }
    const selfDelivery = normalizeJoinOptions(meta, options)
    const ack = (await this._request({ __r: 'req-join', meta, selfDelivery })) as ReqJoinAck
    if (!ack.ok) throw new Error(ack.err)
    const participant = new ClientRoomParticipant(this, ack.id, meta, selfDelivery)
    this._localParticipants.set(ack.id, participant)
    this._state.applyJoin(ack.id, meta, ack.joinedAt) // the relayed event is absorbed
    this._flushPendingDms(ack.id, participant)
    return participant
  }

  async getParticipants(): Promise<RemoteParticipant[]> {
    if (!this._state.rosterKnown) await this._rosterReady
    return this._state.listRemotes() // kept fresh by the event stream from there on
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    if (!this._state.rosterKnown) await this._rosterReady
    return this._state.getRemote(id)
  }

  /** @internal — sync view read for sender resolution (delivery must not wait on I/O). */
  _getRemote(id: string): RemoteParticipant | null {
    return this._state.getRemote(id)
  }

  /** DMs held for this participant while its join ack was in flight — deliver in order. */
  private _flushPendingDms(id: string, participant: ClientRoomParticipant): void {
    if (!this._pendingDms) return
    const held = this._pendingDms.filter((msg) => msg.to === id)
    if (held.length === this._pendingDms.length) this._pendingDms = null
    else if (held.length > 0) this._pendingDms = this._pendingDms.filter((msg) => msg.to !== id)
    for (const msg of held) participant._deliverMessage(msg.from, msg.fromMeta, msg.fromIdentity, msg.data)
  }

  /** @internal — revival of a serialized `RemoteParticipant` (see `roomRemoteReviver`). */
  _reviveRemote(snap: { id: string; meta: ParticipantMeta; joinedAt: number; metaSeq: number }): RemoteParticipant {
    return this._state.ensureRemoteFromSnapshot(snap)
  }

  subscribe(callback: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._state.subscribe(callback)
  }
  subscribeBinary(callback: RoomBinaryListener, options?: { track?: string }): () => void {
    return this._state.subscribeBinary(callback, options)
  }
  onJoin(callback: (member: RemoteParticipant) => void): () => void {
    return this._state.onJoin(callback)
  }
  onLeave(callback: (member: RemoteParticipant, cause?: LeaveCause) => void): () => void {
    return this._state.onLeave(callback)
  }
  onUpdate(callback: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this._state.onUpdate(callback)
  }
  onEmpty(callback: () => void): () => void {
    return this._state.onEmpty(callback)
  }
  onFull(callback: () => void): () => void {
    return this._state.onFull(callback)
  }
  onClose(callback: () => void): () => void {
    return this._state.onClose(callback)
  }
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._state.onAnnounce(callback)
  }

  // ── Requests & publishes (used by ClientRoomParticipant) ──

  /** @internal */
  async _request(req: RoomStubRequest): Promise<ReqJoinAck | ReqOkAck> {
    const ack = await this._stub.send(req, { ack: true })
    assert(isObject(ack) && typeof ack.ok === 'boolean')
    return ack as ReqJoinAck | ReqOkAck
  }

  /** @internal — the envelope sent upward is a claim: the server validates `from` against this
   *  stub's members and stamps the verified `fromMeta` itself before anything reaches the room. */
  async _publishData(from: string, data: unknown): Promise<ChannelPublishAck> {
    return await this._stub.publish({ __r: 'data', from, data } satisfies RoomDataPublish)
  }

  /** @internal */
  async _publishBinaryData(framed: Uint8Array): Promise<ChannelPublishAck> {
    return await this._stub.publishBinary(framed)
  }

  /** @internal — a local participant completed its voluntary leave. */
  _dropParticipant(id: string): void {
    this._localParticipants.delete(id)
    this._state.applyLeave(id) // the relayed event is absorbed
  }

  // ── Event stream (relayed broadcast messages) ──

  private _onEnvelope(envelope: unknown, rawInfo: ChannelPublishInfo): void {
    if (!hasRoomTag(envelope)) return
    const event = envelope as RoomEnvelope | RoomDmEnvelope | RoomRosterEvent
    switch (event.__r) {
      case 'roster':
        // The authoritative member list, positioned in the relay stream: everything relayed
        // before it is reflected in it, later events apply incrementally on top.
        this._state.reconcile(event.members)
        this._syncWants() // per-member binary wants may reference the members just learned
        this._rosterArrived()
        return
      case 'data':
        this._state.applyData(
          event.from,
          event.fromMeta,
          event.fromIdentity ?? null,
          event.data,
          makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp),
          isSuppressed(this.id, event.from),
        )
        return
      case 'join':
        this._state.applyJoin(event.id, event.meta, event.joinedAt, event.identity ?? null)
        return
      case 'leave': {
        const cause = leaveCauseFromWire(event)
        this._state.applyLeave(event.id, cause)
        const local = this._localParticipants.get(event.id)
        if (local) {
          this._localParticipants.delete(event.id)
          local._onLeft(cause) // kicked (with the kick's reason), or left through another handle
        }
        return
      }
      case 'p-meta': {
        this._state.applyParticipantMeta(event.id, event.meta, event.prev, event.seq)
        const local = this._localParticipants.get(event.id)
        if (local) local._meta = event.meta
        return
      }
      case 'update':
        this._state.applyRoomUpdate(event.meta, event.prev, sizeFromWire(event.size), event.at, event.by)
        return
      case 'closed':
        this._applyClosed('closed')
        return
      case 'announce':
        this._state.applyAnnounce(event.data, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
        return
      case 'dm': {
        // Relayed from this member's private inbox — only its own stub ever receives it.
        const local = this._localParticipants.get(event.to)
        if (local) {
          local._deliverMessage(event.from, event.fromMeta, event.fromIdentity ?? null, event.data)
          return
        }
        // The DM beat its target's join ack (same connection, different request) — hold it.
        if (this._state.closed) return
        const pending = (this._pendingDms ??= [])
        pending.push({
          to: event.to,
          from: event.from,
          fromMeta: event.fromMeta,
          fromIdentity: event.fromIdentity ?? null,
          data: event.data,
        })
        if (pending.length > 64) pending.shift()
        return
      }
    }
  }

  private _onBinaryFrame(framed: Uint8Array, rawInfo: ChannelPublishInfo): void {
    const unframed = unframeMemberId(framed)
    if (!unframed) return
    this._state.applyBinary(
      unframed.from,
      unframed.payload,
      unframed.track,
      unframed.keyFrame,
      makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp),
      isSuppressed(this.id, unframed.from),
    )
  }

  private _applyClosed(causeType: 'closed' | 'disconnected'): void {
    if (this._state.closed) return
    this._pendingDms = null
    const cause: LeaveCause = { type: causeType }
    this._state.applyClosed(cause)
    this._rosterArrived() // unblock any getParticipants() waiting on a wire that just died
    // After onClose, like on the server: the room-level signal fires before per-handle cleanup.
    for (const local of this._localParticipants.values()) local._onLeft(cause)
    this._localParticipants.clear()
  }

  /** Declare this holder's wants to the server. Both data lanes are member-selective:
   *  room-level text listeners ride the standard broadcast subscription — declared
   *  synchronously, like `subscribe()`, so same-connection FIFO guarantees a publish right
   *  after subscribing gets its own frame back — while participant-scoped text listeners
   *  declare a `sub-text` member set and binary listeners a `sub-binary` one, each re-sent
   *  only when it changes. */
  private _syncWants(): void {
    const state = this._state
    const text: MemberWants = state.closed ? { all: false, members: [] } : state.textWants()
    this._stub._setWireTextSubscribed(text.all)

    if (state.closed) return // stub is dead — nothing to declare
    const textEncoded = text.all ? '' : [...text.members].sort().join(',')
    if (textEncoded !== this._lastTextWantsSent) {
      this._lastTextWantsSent = textEncoded
      // A room-level subscription supersedes the member set — clear it server-side.
      void this._stub.send({ __r: 'sub-text', members: text.all ? [] : text.members }, { ack: false }).catch(() => {})
    }

    const wants: MemberWants = state.binaryWants()
    const encoded = wants.all ? 'all' : [...wants.members].sort().join(',')
    if (encoded === this._lastBinaryWantsSent) return
    this._lastBinaryWantsSent = encoded
    void this._stub.send({ __r: 'sub-binary', all: wants.all, members: wants.members }, { ack: false }).catch(() => {})
  }
}

// ---------------------------------------------------------------------------
// Local participants
// ---------------------------------------------------------------------------

/** Client-side half of both `LocalParticipant` flavors: links `selfDelivery` suppression
 *  to the sibling room via the registry below. */
abstract class ClientParticipantBase extends ParticipantBase {
  protected readonly _roomId: string

  constructor(roomId: string, id: string, meta: ParticipantMeta, selfDelivery: boolean, identity: string | null) {
    super(id, meta, selfDelivery, identity)
    this._roomId = roomId
    if (!selfDelivery) setSuppressed(roomId, id, true)
  }

  override _onLeft(cause: LeaveCause): void {
    setSuppressed(this._roomId, this.id, false)
    super._onLeft(cause)
  }

  protected _reportError(err: unknown): void {
    reportRoomError(err)
  }
}

/** `LocalParticipant` returned by `ClientRoom.join()` — operates through the room's stub. */
class ClientRoomParticipant extends ClientParticipantBase {
  private readonly _room: ClientRoom

  constructor(clientRoom: ClientRoom, id: string, meta: ParticipantMeta, selfDelivery: boolean) {
    // Client-side joins carry no identity — it's server-assigned (see JoinOptions.identity).
    super(clientRoom.id, id, meta, selfDelivery, null)
    this._room = clientRoom
  }

  protected override _resolveSender(id: string): Sender | null {
    return this._room._getRemote(id)
  }

  async publish(data: unknown): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishData(this.id, data)
  }

  async publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishBinaryData(frameWithMemberId(this.id, data, options))
  }

  async send(to: string | Sender, data: unknown): Promise<void> {
    this._assertActive()
    const toId = typeof to === 'string' ? to : to.id
    unwrapOkAck(await this._room._request({ __r: 'req-dm', id: this.id, to: toId, data }))
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    unwrapOkAck(await this._room._request({ __r: 'req-set-meta', id: this.id, meta }))
    this._meta = meta
  }

  async leave(): Promise<void> {
    if (this._left) return
    this._left = true
    try {
      unwrapOkAck(await this._room._request({ __r: 'req-leave', id: this.id }))
    } finally {
      // Local cleanup even when the wire is gone — the server reaps the member on stub death.
      this._room._dropParticipant(this.id)
      this._onLeft({ type: 'left' })
    }
  }
}

/** `LocalParticipant` revived from a serialized `ServerLocalParticipant` — owns its stub channel. */
class ClientStandaloneParticipant extends ClientParticipantBase {
  private readonly _channel: ClientChannel

  constructor(channel: ClientChannel, metadata: ParticipantStubMetadata) {
    super(metadata.roomId, metadata.id, metadata.meta, metadata.selfDelivery, metadata.identity ?? null)
    this._channel = channel

    channel.listen((notice: unknown) => {
      if (!hasRoomTag(notice)) return
      const msg = notice as ParticipantStubNotice
      if (msg.__r === 'p-meta') this._meta = msg.meta
      else if (msg.__r === 'dm') this._deliverMessage(msg.from, msg.fromMeta, msg.fromIdentity ?? null, msg.data)
      else if (msg.__r === 'left') this._onLeft(standaloneLeftCause(msg))
    })
    channel.onClose(() => this._onLeft({ type: 'disconnected' }))
  }

  async publish(data: unknown): Promise<ChannelPublishAck> {
    this._assertActive()
    return unwrapPublishAck(await this._request({ __r: 'req-publish', data }))
  }

  async publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    this._assertActive()
    return unwrapPublishAck(await this._channel.sendBinary(frameWithMemberId(this.id, data, options), { ack: true }))
  }

  async send(to: string | Sender, data: unknown): Promise<void> {
    this._assertActive()
    unwrapOkAck(await this._request({ __r: 'req-dm', to: typeof to === 'string' ? to : to.id, data }))
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    unwrapOkAck(await this._request({ __r: 'req-set-meta', meta }))
    this._meta = meta
  }

  async leave(): Promise<void> {
    if (this._left) return
    this._left = true
    try {
      unwrapOkAck(await this._request({ __r: 'req-leave' }))
    } finally {
      // Local cleanup even when the wire is gone — the server reaps the member on stub death.
      this._onLeft({ type: 'left' })
      void this._channel.close().catch(() => {})
    }
  }

  private _request(req: ParticipantStubRequest): Promise<unknown> {
    return this._channel.send(req, { ack: true })
  }
}

// ---------------------------------------------------------------------------
// selfDelivery registry — links participants to separately revived rooms
// ---------------------------------------------------------------------------

// A telefunction typically returns `{ room, participant }` — two independent wire values, often
// from *different responses*. When the participant sets `selfDelivery = false`, the sibling
// `ClientRoom` (same page, different wire object) must suppress that member's echoed messages.
//
// A module-global map looks avoidable; it is not — this page is the only place the link can
// exist. The server can't suppress it: room stub and participant stub may ride different
// responses, and "same browser across responses" is not a server-side concept (the stub-member
// relay skip covers only members joined through the *room's own* stub). Nor can the objects
// find each other at revival: they revive independently, so any rendezvous keyed by room ID is
// this registry under another name. Member IDs are UUIDs (no cross-room collisions) and
// entries are removed on leave — the registry cannot leak or misfire.
//
// (Storing `selfDelivery` in the member's KV record wouldn't help: every view would know every
// member's flag, but a room still couldn't tell which members are *its own page's* — locality
// is exactly the information only this registry has.)
const globalObject = getGlobalObject('wire-protocol/room/client.ts', {
  suppressed: new Map<string, Set<string>>(), // roomId → members with selfDelivery off
})

function setSuppressed(roomId: string, memberId: string, suppressed: boolean): void {
  const members = globalObject.suppressed.get(roomId)
  if (suppressed) {
    if (members) members.add(memberId)
    else globalObject.suppressed.set(roomId, new Set([memberId]))
  } else if (members) {
    members.delete(memberId)
    if (members.size === 0) globalObject.suppressed.delete(roomId)
  }
}

function isSuppressed(roomId: string, memberId: string): boolean {
  return globalObject.suppressed.get(roomId)?.has(memberId) ?? false
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function unwrapOkAck(ack: unknown): void {
  assert(isObject(ack) && typeof ack.ok === 'boolean')
  const res = ack as ReqOkAck
  if (!res.ok) throw new Error(res.err)
}

function unwrapPublishAck(ack: unknown): ChannelPublishAck {
  assert(isObject(ack) && typeof ack.ok === 'boolean')
  const res = ack as ReqPublishAck
  if (!res.ok) throw new Error(res.err)
  return res.ack
}

/** A standalone participant's `left` notice carries the server-side cause verbatim. */
function standaloneLeftCause(msg: { cause?: 'removed' | 'disconnected' | 'closed'; reason?: unknown }): LeaveCause {
  const type = msg.cause ?? 'left'
  return msg.reason === undefined ? { type } : { type, reason: msg.reason }
}

function reportRoomError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
