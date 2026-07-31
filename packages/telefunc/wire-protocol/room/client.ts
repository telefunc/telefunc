export { ClientRoom, ClientStandaloneParticipant }

import { assertUsage } from '../../utils/assert.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../channel.js'
import { ClientBroadcast } from '../client/channel.js'
import type { ClientChannel } from '../client/channel.js'
import {
  DM_PARTICIPANT_LEFT,
  leaveCauseFromWire,
  frameWithMemberId,
  hasRoomTag,
  mergeAttributes,
  normalizeJoinOptions,
  ownMetadata,
  unframeMemberId,
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
import { RoomState, RoomStateView } from './state.js'
import { ParticipantBase, type InboxMessage } from './participant.js'
import { adoptSubordinateOf } from '../wrapProxy.js'
import type {
  BinaryPublishOptions,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  Room,
  RoomSnapshotView,
  Sender,
} from './types.js'

/** One awaiter of a conflated publish — resolved with the winning send's receipt (see `_drainCoalesce`). */
type CoalesceWaiter = { resolve: (ack: ChannelPublishAck) => void; reject: (err: unknown) => void }
type ParticipantMutationRequest = Extract<ParticipantStubRequest, { __r: 'req-dm' | 'req-set-meta' | 'req-set-attrs' }>
type PendingJoinEvent =
  | { kind: 'demand'; track: string | null; wanted: boolean }
  | { kind: 'dm'; message: InboxMessage }
  | { kind: 'leave'; cause: LeaveCause }

// ClientRoom

/**
 * Client-side `Room`, revived from a serialized `ServerRoom`.
 *
 * Composes over a broadcast stub: room events & data arrive as its broadcast
 * messages, requests (join/leave/set-meta) ride its channel messages. Membership starts from
 * the serialized snapshot; the relayed event stream keeps it fresh from there.
 */
class ClientRoom extends RoomStateView implements Room {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  private readonly _stub: ClientBroadcast
  protected readonly _state: RoomState
  /** Routing never owns public handles: a live handle owns this Room, not the reverse. */
  private readonly _localParticipants = new Map<string, WeakRef<ClientRoomParticipant>>()
  private _announcedDemand = false
  /** Member-targeted events that beat an in-flight join ack, keyed once their wire ID is known. */
  private _inFlightJoins = 0
  private readonly _pendingJoinEvents = new Map<string, PendingJoinEvent[]>()
  private _closedCause: LeaveCause | null = null
  private _rosterArrived!: () => void
  private _rosterFailed!: (error: unknown) => void
  /** Settled by the replayable initial roster response (or wire death) — gates `getParticipants()`. */
  private readonly _rosterReady = new Promise<void>((resolve, reject) => {
    this._rosterArrived = resolve
    this._rosterFailed = reject
  })

  constructor(stub: ClientBroadcast, snapshot: RoomSnapshotMetadata) {
    super()
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
    if (snapshot.closed) {
      this._closedCause = { type: 'closed' }
      this._rosterArrived()
    }

    // Delivery handlers are local-only — what the server relays is driven by the declared wants: control always arrives, text while subscribed, binary per `sub-binary`.
    stub._subscribeLocal((envelope, info) => this._onEnvelope(envelope, info))
    stub._subscribeBinaryLocal((framed, info) => this._onBinaryFrame(framed, info))
    // Wire death — the network gave up or the stub was GC'd. (A server `Room.close()` arrives as the `closed` ctrl event before the stub shuts down, so it takes the 'closed' path.)
    stub.onClose(() => this._applyClosed('disconnected'))
    // Reconnect reconciles the existing holder and redeclares current intent.
    stub._onReconnect(() => this._syncWants(true))
    // A backend rejection can arrive before the application asks for the roster. Mark it handled here while preserving the original rejection for each later getter.
    void this._rosterReady.catch(() => {})
  }

  // ── Room API ──

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
    this._inFlightJoins++
    try {
      // A rejected join (guard `Abort`, or a `RoomError` like a closed room) rejects this request natively via the channel ack — no envelope to unwrap.
      const { id, joinedAt } = (await this._request({ __r: 'req-join', meta, selfDelivery })) as {
        id: string
        joinedAt: number
      }
      const participant = new ClientRoomParticipant(this, id, meta, selfDelivery)
      adoptSubordinateOf(this, participant)
      if (this._closedCause) {
        participant._onLeft(this._closedCause)
        return participant
      }
      this._localParticipants.set(id, new WeakRef(participant))
      this._state.applyJoin(id, meta, joinedAt)
      for (const event of this._pendingJoinEvents.get(id) ?? []) {
        if (event.kind === 'demand') participant._onDemand(event.track, event.wanted)
        else if (event.kind === 'dm') this._deliverDm(participant, event.message)
        else {
          this._state.applyLeave(id, event.cause)
          this._localParticipants.delete(id)
          participant._onLeft(event.cause)
        }
      }
      this._pendingJoinEvents.delete(id)
      return participant
    } finally {
      this._inFlightJoins--
      if (this._inFlightJoins === 0) this._pendingJoinEvents.clear()
    }
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    assertUsage(!options?.hidden, 'Hidden participants can only be enumerated on the server')
    if (!this._state.rosterKnown) await this._rosterReady
    // kept fresh by the event stream from there on
    return this._state.listRemotes()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    if (!this._state.rosterKnown) await this._rosterReady
    return this._state.getRemote(id)
  }

  /** @internal — sync view read for sender resolution (delivery must not wait on I/O). */
  _getRemote(id: string): RemoteParticipant | null {
    return this._state.getRemote(id)
  }

  private _holdPendingJoinEvent(id: string, event: PendingJoinEvent): boolean {
    if (this._inFlightJoins === 0) return false
    const events = this._pendingJoinEvents.get(id) ?? []
    if (events.length === 0) this._pendingJoinEvents.set(id, events)
    events.push(event)
    return true
  }

  /** Deliver a DM to a locally-held member. A plain DM just fires its listeners; an ack DM (`send(…, { ack: true })`) routes the handler's reply back up the stub as a `dm-reply`, which the server
   * turns into the sender's `dm-ack` — so a client recipient replies just like a server-side one.
   */
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

  // The roster streams in right behind the response — its arrival is an onChange.
  snapshot = (): RoomSnapshotView => this._state.snapshot()

  // ── Requests & publishes (used by ClientRoomParticipant) ──

  /** @internal — an ack-bearing stub request. Resolves with the handler's raw return, or rejects natively (the channel rebuilds an `AbortError`/`Error` from the ack status) — no envelope. */
  _request(req: RoomStubRequest): Promise<unknown> {
    return this._stub.send(req, { ack: true })
  }

  /** @internal — the envelope sent upward is a claim: the server validates `from` against this stub's members and stamps the verified `fromMeta` itself before anything reaches the room. */
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
        // The authoritative member list, positioned in the relay stream: everything relayed before it is reflected in it, later events apply incrementally on top. The client's roster carries only
        // presence members, so reconcile must not reap directly-granted hidden handles (they aren't roster-managed) — see `RoomState.reconcile`.
        this._applyRoster(event.members)
        return
      case 'roster-error':
        this._rosterFailed(new Error('Failed to load room participants'))
        return
      case 'data':
        // Tail mode holds server-side (see `RoomStubChannel._tailPending`): text reaches this client only once it subscribes, already selected and ordered, so nothing is buffered here.
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
        const local = this._localParticipant(event.id)
        if (local) {
          this._localParticipants.delete(event.id)
          local._onLeft(cause) // kicked (with the kick's reason), or left through another handle
        } else this._holdPendingJoinEvent(event.id, { kind: 'leave', cause })
        return
      }
      case 'p-meta': {
        this._state.applyParticipantMeta(event.id, event.meta, event.seq)
        const local = this._localParticipant(event.id)
        if (local) local._meta = ownMetadata(event.meta)
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
        {
          const local = this._localParticipant(event.member)
          if (local) local._onDemand(event.track, event.wanted)
          else
            this._holdPendingJoinEvent(event.member, {
              kind: 'demand',
              track: event.track,
              wanted: event.wanted,
            })
        }
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
        const local = this._localParticipant(event.to)
        if (local) {
          this._deliverDm(local, msg)
          return
        }
        if (this._holdPendingJoinEvent(event.to, { kind: 'dm', message: msg })) return
        if (event.ackId)
          void this._stub
            .send({ __r: 'dm-reply', id: event.to, ackId: event.ackId, ...DM_PARTICIPANT_LEFT }, { ack: false })
            .catch(() => {})
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
    const cause: LeaveCause = { type: causeType }
    this._closedCause = cause
    this._pendingJoinEvents.clear()
    this._state.applyClosed(cause)
    this._rosterArrived() // unblock any getParticipants() waiting on a wire that just died
    // After onClose, like on the server: the room-level signal fires before per-handle cleanup.
    for (const ref of this._localParticipants.values()) ref.deref()?._onLeft(cause)
    this._localParticipants.clear()
  }

  private _localParticipant(id: string): ClientRoomParticipant | null {
    const participant = this._localParticipants.get(id)?.deref() ?? null
    if (!participant) this._localParticipants.delete(id)
    return participant
  }

  /** Declare this holder's wants to the server. The room-level text want rides the standard broadcast subscription — declared synchronously, like `subscribe()`, so same-connection FIFO guarantees a
   * publish right after subscribing gets its own frame back.
   */
  private _syncWants(reconcileText = false): void {
    const state = this._state
    const text: MemberWants = state.closed ? { all: false, members: [] } : state.textWants()
    this._stub._setWireTextSubscribed(text.all, reconcileText)
    if (state.closed) return // stub is dead — nothing to declare

    // A room-level text subscription supersedes the member set — clear it server-side.
    const announce = state.wantsAnnounce
    const declaration = { __r: 'sub-text', members: text.all ? [] : text.members, announce } as const
    const fence = reconcileText || announce !== this._announcedDemand
    this._announcedDemand = announce
    if (fence) this._stub._addTelefunctionCallBarrier(this._stub.send(declaration, { ack: true }))
    else void this._stub.send(declaration, { ack: false }).catch(() => {})
    const binary = state.binaryWants()
    void this._stub.send({ __r: 'sub-binary', wants: binary }, { ack: false }).catch(() => {})
  }
}

// Local participants

/** Client-side half of both `LocalParticipant` flavors. `selfDelivery` needs nothing here: the
 *  server never relays a self-suppressed member's echo to this client's room stub (see server
 *  `_onTextData`), so the flag is carried purely as a public read-only property. */
abstract class ClientParticipantBase extends ParticipantBase {
  /** Per-key conflation state for `publish(data, { coalesce })` — at most one in-flight send per key; while it's in flight the newest value waits in `pending` and supersedes any earlier one. */
  private readonly _coalescers = new Map<
    string,
    { sending: boolean; pending: { data: unknown; retain?: boolean; waiters: CoalesceWaiter[] } | null }
  >()

  constructor(
    id: string,
    meta: ParticipantMeta,
    selfDelivery: boolean,
    identity: string | null,
    private readonly _requestParticipant: (request: ParticipantMutationRequest) => Promise<unknown>,
  ) {
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

  // Implementation of the overloaded `LocalParticipant.send`; the interface supplies precise returns.
  async send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    this._assertActive()
    const toId = typeof to === 'string' ? to : to.id
    return await this._requestParticipant({
      __r: 'req-dm',
      to: toId,
      data,
      ...(options?.ack ? { ack: true } : {}),
    })
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetadata(meta)
    await this._requestParticipant({ __r: 'req-set-meta', meta: owned })
    this._meta = owned
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetadata(attrs)
    await this._requestParticipant({ __r: 'req-set-attrs', attrs: owned })
    this._meta = mergeAttributes(this._meta, owned)
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
    super(id, meta, selfDelivery, null, (request) => clientRoom._request({ ...request, id } as RoomStubRequest))
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

  async leave(): Promise<void> {
    if (this._left) return
    await this._room._request({ __r: 'req-leave', id: this.id })
    this._room._dropParticipant(this.id)
    this._onLeft({ type: 'left' })
  }
}

/** `LocalParticipant` revived from a serialized `ServerLocalParticipant` — owns its stub channel. */
class ClientStandaloneParticipant extends ClientParticipantBase {
  private readonly _channel: ClientChannel

  constructor(channel: ClientChannel, metadata: ParticipantStubMetadata) {
    super(metadata.id, metadata.meta, metadata.selfDelivery, metadata.identity ?? null, (request) =>
      channel.send(request, { ack: true }),
    )
    this._channel = channel

    channel.listen((notice: unknown) => {
      if (!hasRoomTag(notice)) return
      const msg = notice as ParticipantStubNotice
      if (msg.__r === 'p-meta') this._meta = ownMetadata(msg.meta)
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
      } else if (msg.__r === 'left') this._onLeft(leaveCauseFromWire(msg))
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

  async leave(): Promise<void> {
    if (this._left) return
    await this._request({ __r: 'req-leave' })
    this._onLeft({ type: 'left' })
    void this._channel.close().catch(() => {})
  }

  private _request(req: ParticipantStubRequest): Promise<unknown> {
    return this._channel.send(req, { ack: true })
  }
}

function reportRoomError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
