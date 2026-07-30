export { ClientRoom, ClientRoomParticipant, ClientStandaloneParticipant, RoomClientBroadcast }

import { assertUsage } from '../../utils/assert.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import {
  makePublishInfo,
  type BroadcastBinaryListener,
  type BroadcastListener,
  type ChannelData,
  type ChannelPublishAck,
  type ChannelPublishInfo,
} from '../channel.js'
import { ClientBroadcast } from '../client/channel.js'
import type { ClientChannel } from '../client/channel.js'
import type { WirePublishInfo } from '../shared-ws.js'
import { parse } from '@brillout/json-serializer/parse'
import {
  leaveCauseFromWire,
  frameWithMemberId,
  hasRoomTag,
  mergeAttributes,
  normalizeJoinOptions,
  unframeMemberId,
  type BinaryWants,
  type MemberWants,
  type MemberSnapshot,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type RoomDemandEvent,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomRosterEvent,
  type RoomSnapshotMetadata,
  type RoomStubRequest,
} from './protocol.js'
import { RoomState } from './state.js'
import { ParticipantBase, type InboxMessage } from './participant.js'
import type {
  BinaryPublishOptions,
  RoomBinaryListener,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  Room,
  RoomMeta,
  RoomSendReceipt,
  RoomSnapshotView,
  Sender,
} from './types.js'

/** One awaiter of a conflated publish — resolved with the winning send's receipt (see `_drainCoalesce`). */
type CoalesceWaiter = { resolve: (ack: ChannelPublishAck) => void; reject: (err: unknown) => void }

/**
 * Room's broadcast stub owns two concerns a generic Broadcast doesn't have: local delivery is
 * always installed while wire text demand is selective, and reconnect must re-declare that demand.
 * Keeping the behavior in this subclass prevents Room policy from leaking into ClientBroadcast.
 */
class RoomClientBroadcast<T = unknown> extends ClientBroadcast<T> {
  private readonly _roomListeners: Array<BroadcastListener<T>> = []
  private readonly _roomBinaryListeners: BroadcastBinaryListener[] = []
  private readonly _roomReconnectCallbacks: Array<() => void> = []
  private _roomWireTextSubscribed = false
  private _roomDidOpen = false

  _subscribeLocal(callback: BroadcastListener<T>): () => void {
    this._roomListeners.push(callback)
    return () => {
      const index = this._roomListeners.indexOf(callback)
      if (index >= 0) this._roomListeners.splice(index, 1)
    }
  }

  _subscribeBinaryLocal(callback: BroadcastBinaryListener): () => void {
    this._roomBinaryListeners.push(callback)
    return () => {
      const index = this._roomBinaryListeners.indexOf(callback)
      if (index >= 0) this._roomBinaryListeners.splice(index, 1)
    }
  }

  _setWireTextSubscribed(on: boolean, reconcile = false): void {
    if ((!reconcile && on === this._roomWireTextSubscribed) || this._isClosed) return
    this._roomWireTextSubscribed = on
    if (on) this._connection.sendBroadcastSubscribe(this, false)
    else this._connection.sendBroadcastUnsubscribe(this, false)
  }

  _onReconnect(callback: () => void): void {
    this._roomReconnectCallbacks.push(callback)
  }

  override _onTransportOpen(batched: boolean): void {
    const reopened = this._roomDidOpen
    super._onTransportOpen(batched)
    if (this._isClosed) return
    if (!reopened) {
      this._roomDidOpen = true
      return
    }
    for (const callback of this._roomReconnectCallbacks) {
      try {
        callback()
      } catch (error) {
        if (this._handleCallbackError(error)) return
      }
    }
  }

  override _onTransportPublish(data: string, wireInfo: WirePublishInfo): void {
    super._onTransportPublish(data, wireInfo)
    const parsed = parse(data) as ChannelData<T>
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    for (const callback of this._roomListeners) {
      try {
        callback(parsed, info)
      } catch (error) {
        if (this._handleCallbackError(error)) return
      }
    }
  }

  override _onTransportPublishBinary(data: Uint8Array, wireInfo: WirePublishInfo): void {
    super._onTransportPublishBinary(data, wireInfo)
    const info = makePublishInfo(this.key!, wireInfo.seq, wireInfo.timestamp)
    for (const callback of this._roomBinaryListeners) {
      try {
        callback(data, info)
      } catch (error) {
        if (this._handleCallbackError(error)) return
      }
    }
  }
}

// ---------------------------------------------------------------------------
// ClientRoom
// ---------------------------------------------------------------------------

/**
 * Client-side `Room`, revived from a serialized `ServerRoom`.
 *
 * Composes over a Room-owned broadcast stub: room events & data arrive as its broadcast
 * messages, requests (join/leave/set-meta) ride its channel messages. Membership starts from
 * the serialized snapshot; the relayed event stream keeps it fresh from there.
 */
class ClientRoom implements Room {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  private readonly _stub: RoomClientBroadcast
  private readonly _state: RoomState
  private readonly _localParticipants = new Map<string, ClientRoomParticipant>()
  /** Wants already declared to the server, by lane. Every lane's declaration is a full
   *  replace, re-sent only when its canonical encoding changes — `''` encodes "nothing
   *  wanted" on every lane, which is also the nothing-declared-yet initial state. */
  private readonly _declaredWants = new Map<string, string>()
  /** DMs relayed before their participant's join ack resolved (a reactive send racing the
   *  join round-trip) — held bounded (count-capped, drop-oldest), flushed on registration. */
  private _pendingDms: Array<InboxMessage & { to: string }> | null = null
  private _rosterArrived!: () => void
  private _rosterFailed!: (error: unknown) => void
  /** Settled by the replayable initial roster response (or wire death) — gates `getParticipants()`. */
  private readonly _rosterReady = new Promise<void>((resolve, reject) => {
    this._rosterArrived = resolve
    this._rosterFailed = reject
  })

  constructor(stub: RoomClientBroadcast, snapshot: RoomSnapshotMetadata) {
    this._stub = stub
    this._state = new RoomState({
      roomId: snapshot.roomId,
      meta: snapshot.meta,
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
    // Reconnect first reconciles the existing holder and releases bounded sequenced replay. This is
    // the final declaration layer: forget keyed wants we assumed the holder knew, and explicitly
    // reconcile the unsequenced room-wide text control from current intent.
    stub._onReconnect(() => {
      this._declaredWants.clear()
      this._syncWants(true)
    })
    // A backend rejection can arrive before the application asks for the roster. Mark it handled
    // here while preserving the original rejection for each later getter.
    void this._rosterReady.catch(() => {})
  }

  // ── Room API ──

  get id(): string {
    return this._state.roomId
  }
  get meta(): RoomMeta {
    return this._state.meta
  }
  get count(): number {
    return this._state.count
  }
  get isEmpty(): boolean {
    return this._state.count === 0
  }
  get isClosed(): boolean {
    return this._state.closed
  }

  async join(options?: JoinOptions): Promise<LocalParticipant> {
    assertUsage(
      options?.identity === undefined,
      'join() options.identity is server-assigned: identity is trusted, so set it where trust lives — in the granting telefunction (server-side join()), not on the client.',
    )
    assertUsage(
      options?.hidden === undefined,
      'join() options.hidden is server-side only: a hidden participant is created by the granting telefunction (server-side join({ hidden: true })), not by a client.',
    )
    const { meta, selfDelivery } = normalizeJoinOptions(options)
    // A rejected join (guard `Abort`, or a `RoomError` like a closed room) rejects this request
    // natively via the channel ack — no envelope to unwrap.
    const { id, joinedAt } = (await this._request({ __r: 'req-join', meta, selfDelivery })) as {
      id: string
      joinedAt: number
    }
    const participant = new ClientRoomParticipant(this, id, meta, selfDelivery)
    this._localParticipants.set(id, participant)
    this._state.applyJoin(id, meta, joinedAt) // the relayed event is absorbed
    this._flushPendingDms(id, participant)
    return participant
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    if (!this._state.rosterKnown) await this._rosterReady
    // kept fresh by the event stream from there on
    return options?.hidden ? this._state.listHidden() : this._state.listRemotes()
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
    if (held.length === 0) return
    const rest = this._pendingDms.filter((msg) => msg.to !== id)
    this._pendingDms = rest.length > 0 ? rest : null
    for (const msg of held) this._deliverDm(participant, msg)
  }

  /** Deliver a DM to a locally-held member. A plain DM just fires its listeners; an ack DM
   *  (`send(…, { ack: true })`) routes the handler's reply back up the stub as a `dm-reply`, which
   *  the server turns into the sender's `dm-ack` — so a client recipient replies just like a
   *  server-side one. */
  private _deliverDm(participant: ClientRoomParticipant, msg: InboxMessage): void {
    if (msg.ackId === undefined) {
      participant._deliverMessage(msg)
      return
    }
    const ackId = msg.ackId
    void participant._deliverMessageAck(msg).then((reply) => {
      void this._stub.send({ __r: 'dm-reply', id: participant.id, ackId, ...reply }, { ack: false }).catch(() => {})
    })
  }

  /** @internal — revival of a serialized `RemoteParticipant` (see `roomRemoteReviver`). */
  _reviveRemote(snap: {
    id: string
    meta: ParticipantMeta
    joinedAt: number
    metaSeq: number
    identity: string | null
    hidden?: boolean
  }): RemoteParticipant {
    return this._state.ensureRemoteFromSnapshot(snap)
  }

  subscribe(callback: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._state.subscribe(callback)
  }
  subscribeBinary(callback: RoomBinaryListener, options?: { track?: string | null }): () => void {
    return this._state.subscribeBinary(callback, options)
  }
  onJoin(callback: (member: RemoteParticipant) => void): () => void {
    return this._state.onJoin(callback)
  }
  onLeave(callback: (member: RemoteParticipant, cause?: LeaveCause) => void): () => void {
    return this._state.onLeave(callback)
  }
  onParticipantUpdate(
    callback: (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void,
  ): () => void {
    return this._state.onParticipantUpdate(callback)
  }
  onUpdate(callback: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this._state.onUpdate(callback)
  }
  onEmpty(callback: () => void): () => void {
    return this._state.onEmpty(callback)
  }
  onClose(callback: () => void): () => void {
    return this._state.onClose(callback)
  }
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._state.onAnnounce(callback)
  }

  // Arrow-valued, not prototype methods: the documented `useSyncExternalStore(room.onChange, room.snapshot)`
  // passes both detached, so React calls them with no receiver. Bound properties survive that; a plain method
  // would deref `this === undefined` and throw on the first render (see the twin on `ServerRoom`).
  onChange = (callback: () => void): (() => void) => this._state.onChange(callback)

  // The roster streams in right behind the response — its arrival is an onChange.
  snapshot = (): RoomSnapshotView => this._state.snapshot()

  // ── Requests & publishes (used by ClientRoomParticipant) ──

  /** @internal — an ack-bearing stub request. Resolves with the handler's raw return, or rejects
   *  natively (the channel rebuilds an `AbortError`/`Error` from the ack status) — no envelope. */
  _request(req: RoomStubRequest): Promise<unknown> {
    return this._stub.send(req, { ack: true })
  }

  /** @internal — the envelope sent upward is a claim: the server validates `from` against this
   *  stub's members and stamps the verified `fromMeta` itself before anything reaches the room. */
  async _publishText(from: string, data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    return await this._stub.publish({
      __r: 'data',
      from,
      data,
      ...(retain ? { retain: true } : {}),
    } satisfies RoomDataPublish)
  }

  /** @internal */
  async _publishBinaryFramed(framed: Uint8Array): Promise<ChannelPublishAck> {
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
    const event = envelope as RoomEnvelope | RoomDmEnvelope | RoomRosterEvent | RoomDemandEvent
    switch (event.__r) {
      case 'roster':
        // The authoritative member list, positioned in the relay stream: everything relayed
        // before it is reflected in it, later events apply incrementally on top. The client's
        // roster carries only presence members, so reconcile must not reap directly-granted hidden
        // handles (they aren't roster-managed) — see `RoomState.reconcile`.
        this._applyRoster(event.members)
        return
      case 'roster-error':
        this._rosterFailed(new Error('Failed to load room participants'))
        return
      case 'data':
        // Tail mode holds server-side (see `RoomStubChannel._tailPending`): text reaches this client
        // only once it subscribes, already selected and ordered, so nothing is buffered here.
        this._state.applyData(
          event.from,
          event.fromMeta,
          event.fromIdentity ?? null,
          event.data,
          makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp),
        )
        return
      case 'join':
        this._state.applyJoin(event.id, event.meta, event.joinedAt, event.identity ?? null, event.hidden)
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
        this._state.applyRoomUpdate(event.meta, event.at, event.by)
        return
      case 'closed':
        this._applyClosed('closed')
        return
      case 'announce':
        this._state.applyAnnounce(event.data, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
        return
      case 'demand':
        // Whether anyone wants one of our own members' tracks flipped (onDemand).
        this._localParticipants.get(event.member)?._onDemand(event.track, event.wanted)
        return
      case 'dm': {
        // Relayed from this member's private inbox — only its own stub ever receives it.
        const msg: InboxMessage = {
          from: event.from,
          fromMeta: event.fromMeta,
          fromIdentity: event.fromIdentity ?? null,
          data: event.data,
          ...(event.ackId ? { ackId: event.ackId } : {}),
        }
        const local = this._localParticipants.get(event.to)
        if (local) {
          this._deliverDm(local, msg)
          return
        }
        // The DM beat its target's join ack (same connection, different request) — hold it.
        if (this._state.closed) return
        const pending = (this._pendingDms ??= [])
        pending.push({ ...msg, to: event.to })
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
      unframed.meta,
      makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp),
    )
  }

  private _applyRoster(members: MemberSnapshot[]): void {
    this._state.reconcile(members, true)
    this._syncWants() // per-member binary wants may reference the members just learned
    this._rosterArrived()
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

  /** Declare this holder's wants to the server, one declaration per lane. The room-level text
   *  want rides the standard broadcast subscription — declared synchronously, like
   *  `subscribe()`, so same-connection FIFO guarantees a publish right after subscribing gets
   *  its own frame back; everything else is a keyed `_declareWant`. */
  private _syncWants(reconcileText = false): void {
    const state = this._state
    const text: MemberWants = state.closed ? { all: false, members: [] } : state.textWants()
    this._stub._setWireTextSubscribed(text.all, reconcileText)
    if (state.closed) return // stub is dead — nothing to declare

    // A room-level text subscription supersedes the member set — clear it server-side.
    this._declareWant('text', text.all ? '' : [...text.members].sort().join(','), {
      __r: 'sub-text',
      members: text.all ? [] : text.members,
    })
    const binary = state.binaryWants()
    this._declareWant('binary', encodeBinaryWants(binary), { __r: 'sub-binary', wants: binary })
  }

  /** Send one lane's declaration iff its canonical encoding changed since last declared. */
  private _declareWant(lane: string, encoded: string, request: RoomStubRequest): void {
    if ((this._declaredWants.get(lane) ?? '') === encoded) return
    this._declaredWants.set(lane, encoded)
    void this._stub.send(request, { ack: false }).catch(() => {})
  }
}

// ---------------------------------------------------------------------------
// Local participants
// ---------------------------------------------------------------------------

/** Client-side half of both `LocalParticipant` flavors. `selfDelivery` needs nothing here: the
 *  server never relays a self-suppressed member's echo to this client's room stub (see server
 *  `_onTextData`), so the flag is carried purely as a public read-only property. */
abstract class ClientParticipantBase extends ParticipantBase {
  /** Per-key conflation state for `publish(data, { coalesce })` — at most one in-flight send per
   *  key; while it's in flight the newest value waits in `pending` and supersedes any earlier one. */
  private readonly _coalescers = new Map<
    string,
    { sending: boolean; pending: { data: unknown; retain?: boolean; waiters: CoalesceWaiter[] } | null }
  >()

  constructor(id: string, meta: ParticipantMeta, selfDelivery: boolean, identity: string | null) {
    super(id, meta, selfDelivery, identity)
  }

  /** The actual wire publish — each flavor supplies it; `publish()` wraps it with conflation. */
  protected abstract _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck>

  publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    const key = options?.coalesce
    if (key === undefined) return this._sendPublish(data, options?.retain)
    return new Promise<ChannelPublishAck>((resolve, reject) => {
      let slot = this._coalescers.get(key)
      if (!slot) {
        slot = { sending: false, pending: null }
        this._coalescers.set(key, slot)
      }
      // Supersede any queued value; its waiters ride along and all resolve with the winning send.
      slot.pending = { data, retain: options?.retain, waiters: [...(slot.pending?.waiters ?? []), { resolve, reject }] }
      this._drainCoalesce(key)
    })
  }

  private _drainCoalesce(key: string): void {
    const slot = this._coalescers.get(key)
    if (!slot || slot.sending || !slot.pending) return
    const { data, retain, waiters } = slot.pending
    slot.pending = null
    slot.sending = true
    this._sendPublish(data, retain)
      .then(
        (ack) => waiters.forEach((w) => w.resolve(ack)),
        (err) => waiters.forEach((w) => w.reject(err)),
      )
      .finally(() => {
        slot.sending = false
        if (slot.pending) this._drainCoalesce(key)
        else this._coalescers.delete(key)
      })
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
    super(id, meta, selfDelivery, null)
    this._room = clientRoom
  }

  protected override _resolveSender(id: string): Sender | null {
    return this._room._getRemote(id)
  }

  protected async _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishText(this.id, data, retain)
  }

  async publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishBinaryFramed(frameWithMemberId(this.id, data, options))
  }

  // Impl of the overloaded `LocalParticipant.send`; callers get precise result types via the interface.
  // A rejected send rejects the request natively via the channel ack (guard/recipient `Abort` or an
  // operational `RoomError`) — no envelope to unwrap.
  async send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    this._assertActive()
    const toId = typeof to === 'string' ? to : to.id
    return await this._room._request({
      __r: 'req-dm',
      id: this.id,
      to: toId,
      data,
      ...(options?.ack ? { ack: true } : {}),
    })
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._request({ __r: 'req-set-meta', id: this.id, meta })
    this._meta = meta
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._request({ __r: 'req-set-attrs', id: this.id, attrs })
    this._meta = mergeAttributes(this._meta, attrs)
  }

  async leave(): Promise<void> {
    if (this._left) return
    this._left = true
    try {
      await this._room._request({ __r: 'req-leave', id: this.id })
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
    super(metadata.id, metadata.meta, metadata.selfDelivery, metadata.identity ?? null)
    this._channel = channel

    channel.listen((notice: unknown) => {
      if (!hasRoomTag(notice)) return
      const msg = notice as ParticipantStubNotice
      if (msg.__r === 'p-meta') this._meta = msg.meta
      else if (msg.__r === 'demand') this._onDemand(msg.track, msg.wanted)
      else if (msg.__r === 'dm') {
        const inbox: InboxMessage = {
          from: msg.from,
          fromMeta: msg.fromMeta,
          fromIdentity: msg.fromIdentity ?? null,
          data: msg.data,
          ...(msg.ackId ? { ackId: msg.ackId } : {}),
        }
        // An ack DM replies through the channel's own ack — the handler's return rides it home.
        if (msg.ackId) return this._deliverMessageAck(inbox)
        this._deliverMessage(inbox)
      } else if (msg.__r === 'left') this._onLeft(standaloneLeftCause(msg))
    })
    channel.onClose(() => this._onLeft({ type: 'disconnected' }))
  }

  protected async _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return (await this._request({ __r: 'req-publish', data, ...(retain ? { retain: true } : {}) })) as ChannelPublishAck
  }

  async publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    this._assertActive()
    return (await this._channel.sendBinary(frameWithMemberId(this.id, data, options), {
      ack: true,
    })) as ChannelPublishAck
  }

  // Impl of the overloaded `LocalParticipant.send`; callers get precise result types via the interface.
  // A rejected send (guard `Abort`, recipient's `Abort`, or an operational `RoomError`) rejects the
  // request natively via the channel ack — no envelope to unwrap.
  async send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    this._assertActive()
    const toId = typeof to === 'string' ? to : to.id
    return await this._request({ __r: 'req-dm', to: toId, data, ...(options?.ack ? { ack: true } : {}) })
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._request({ __r: 'req-set-meta', meta })
    this._meta = meta
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._request({ __r: 'req-set-attrs', attrs })
    this._meta = mergeAttributes(this._meta, attrs)
  }

  async leave(): Promise<void> {
    if (this._left) return
    this._left = true
    try {
      await this._request({ __r: 'req-leave' })
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
// Helpers
// ---------------------------------------------------------------------------

/** A standalone participant's `left` notice carries the server-side cause verbatim. */
function standaloneLeftCause(msg: { cause?: 'removed' | 'disconnected' | 'closed'; reason?: unknown }): LeaveCause {
  const type = msg.cause ?? 'left'
  return msg.reason === undefined ? { type } : { type, reason: msg.reason }
}

/** Canonical (order-independent) form of a binary want — the dedupe key for `sub-binary`.
 *  The empty want encodes to `''`, matching the nothing-sent-yet initial state. */
function encodeBinaryWants(wants: BinaryWants): string {
  const encodeTracks = (w: { all: boolean; tracks: string[] }) => (w.all ? '*' : [...w.tracks].sort().join(','))
  const members = Object.entries(wants.members)
    .map(([id, w]) => `${id}=${encodeTracks(w)}`)
    .sort()
  return [encodeTracks(wants.everyMember), ...members].join(';')
}

function reportRoomError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
