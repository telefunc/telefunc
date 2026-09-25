export { ClientRoom, ClientStandaloneParticipant }

import { createDeferred } from '../../utils/createDeferred.js'
import { assert, assertUsage } from '../../utils/assert.js'
import { markHandled } from '../../utils/markHandled.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../channel.js'
import { ClientBroadcast } from '../client/channel.js'
import type { ClientChannel } from '../client/channel.js'
import { DM_FAILURE } from './errors.js'
import { decodeBinaryFrame, emptyBinaryWants, encodeBinaryFrame } from './binary.js'
import { assertKnownOptions, leaveCauseFromWire, normalizeJoinOptions, ownMetaArgument, recipientId } from './model.js'
import {
  hasRoomTag,
  inboxMessageFromWire,
  joinedMember,
  type DmReply,
  type InboxMessage,
  type MemberSnapshot,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type AcceptedMeta,
  type RoomDemandEvent,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomRosterEvent,
  type RoomSnapshotMetadata,
  type RoomStubRequest,
} from './protocol.js'
import { RoomState, RoomStateView } from './state.js'
import { ParticipantBase } from './participant.js'
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

/** The member an event concerns while that member's join may still be settling. */
function heldMemberOf(event: RoomEnvelope | RoomDmEnvelope | RoomRosterEvent | RoomDemandEvent): string | null {
  switch (event.__r) {
    case 'dm':
      return event.to
    case 'demand':
      return event.member
    case 'leave':
      return event.id
    default:
      return null
  }
}

/** One awaiter of a conflated publish, resolved with the winning send's receipt (see `_drainCoalesce`). */
type CoalesceWaiter = { resolve: (ack: ChannelPublishAck) => void; reject: (err: unknown) => void }
type ParticipantMutationRequest = Extract<ParticipantStubRequest, { __r: 'req-dm' | 'req-set-meta' | 'req-set-attrs' }>
type WantsDeclaration = Extract<RoomStubRequest, { __r: 'sub-text' | 'sub-binary' }>

/**
 * Client Room composes delivery and requests over one Broadcast stub.
 * Broadcast frames carry Room events/data; acked channel messages carry mutations.
 * Its serialized membership seed stays fresh through the positioned event stream.
 */
class ClientRoom extends RoomStateView implements Room {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  private readonly _stub: ClientBroadcast
  protected readonly _state: RoomState
  private readonly _localParticipants = new Map<string, ClientRoomParticipant>()
  private _closedCause: LeaveCause | null = null
  /** Joins awaiting their ack. The server relays to a member from its admission, before the ack registers it here, so events naming an unknown member wait for the joins to settle. */
  private _pendingJoins = 0
  private _heldForJoins: Array<{ envelope: unknown; rawInfo: ChannelPublishInfo }> = []
  /** The wants the server stub holds, as last declared; a fresh stub holds none. */
  private readonly _declared: Record<WantsDeclaration['__r'], string> = {
    'sub-text': JSON.stringify({ __r: 'sub-text', members: [], announce: false }),
    'sub-binary': JSON.stringify({ __r: 'sub-binary', wants: emptyBinaryWants() }),
  }
  /** Settled by the replayable initial roster response (or wire death). Gates `getParticipants()`. */
  private readonly _roster = createDeferred()

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
      onCallbackError: reportClientCallbackError,
      onLeave: (id, cause) => this._onLeave(id, cause),
    })
    this._state._owner = this
    if (snapshot.closed) {
      this._closedCause = { type: 'closed' }
      this._roster.resolve()
    }

    // Delivery handlers are local-only. What the server relays is driven by the declared wants: control always arrives, text while subscribed, binary per `sub-binary`.
    stub._subscribeLocal('text', (envelope, info) => this._onEnvelope(envelope, info))
    stub._subscribeLocal('binary', (framed, info) => this._onBinaryFrame(framed, info))
    // Wire death: the network gave up or the stub was GC'd. (A server `Room.close()` arrives as the `closed` ctrl event before the stub shuts down, so it takes the 'closed' path.)
    stub.onClose(() => this._applyClosed('disconnected'))
    // A backend rejection can arrive before the application asks for the roster. Mark it handled here while preserving the original rejection for each later getter.
    void this._roster.promise.catch(() => {})
  }

  async join(options?: JoinOptions): Promise<LocalParticipant> {
    assertUsage(
      options?.identity === undefined,
      'join() options.identity is server-assigned: identity is trusted, so set it where trust lives, in the granting telefunction (server-side join()), not on the client.',
    )
    assertUsage(
      options?.hidden === undefined,
      'join() options.hidden is server-side only: a hidden participant is created by the granting telefunction (server-side join({ hidden: true })), not by a client.',
    )
    const { meta, selfDelivery } = normalizeJoinOptions(options)
    this._pendingJoins++
    try {
      // A rejected join (guard `Abort`, or a `RoomError` like a closed room) rejects this request natively via the channel ack. No envelope to unwrap.
      const { id, joinedAt } = (await this._request({ __r: 'req-join', meta, selfDelivery })) as {
        id: string
        joinedAt: number
      }
      const participant = new ClientRoomParticipant(this, id, meta, selfDelivery)
      if (this._closedCause) {
        participant._onLeft(this._closedCause)
        return participant
      }
      this._localParticipants.set(id, participant)
      this._state.applyJoin({ id, meta, joinedAt, metaSeq: 0, identity: null })
      return participant
    } finally {
      this._pendingJoins--
      const held = this._heldForJoins
      this._heldForJoins = []
      for (const { envelope, rawInfo } of held) this._onEnvelope(envelope, rawInfo)
    }
  }

  private _holdsForJoin(memberId: string): boolean {
    return this._pendingJoins > 0 && !this._localParticipants.has(memberId)
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    assertUsage(!options?.hidden, 'Hidden participants can only be enumerated on the server')
    await this._awaitRoster()
    return this._state.listVisible()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this._awaitRoster()
    return this._state.getRemote(id)
  }

  private async _awaitRoster(): Promise<void> {
    if (!this._state.rosterKnown) await this._roster.promise
  }

  /** @internal Sync view read for sender resolution (delivery must not wait on I/O). */
  _getRemote(id: string): RemoteParticipant | null {
    return this._state.getRemote(id)
  }

  /** Plain DM fires local listeners; ack DM returns their reply
   * through the symmetric `dm-reply`/`dm-ack` path. */
  private _deliverDm(participant: ClientRoomParticipant, msg: InboxMessage): void {
    if (msg.ackId === undefined) {
      participant._deliverMessage(msg)
      return
    }
    const ackId = msg.ackId
    void participant._deliverMessageAck(msg).then((reply) => this._replyDm(participant.id, ackId, reply))
  }

  private _replyDm(id: string, ackId: string, reply: DmReply): void {
    // A closed stub can't carry the reply; the sender's ack times out as for any lost reply.
    if (this._stub.isClosed) return
    void this._stub.send({ __r: 'dm-reply', id, ackId, reply }, { ack: false }).catch(() => {})
  }

  /** @internal Revival of a serialized `RemoteParticipant` (see `roomRemoteReviver`). */
  _reviveRemote(snap: MemberSnapshot): RemoteParticipant {
    return this._state.ensureRemoteFromSnapshot(snap)
  }

  // The roster streams in right behind the response. Its arrival is an onChange.
  snapshot(): RoomSnapshotView {
    return this._state.snapshot()
  }

  /** @internal An ack-bearing stub request. Resolves with the handler's raw return, or rejects natively (the channel rebuilds an `AbortError`/`Error` from the ack status). No envelope. */
  _request(req: RoomStubRequest): Promise<unknown> {
    return this._stub.send(req, { ack: true })
  }

  /** @internal The envelope sent upward is a claim: the server validates `from` against this stub's members and stamps the verified `fromMeta` itself before anything reaches the room. */
  async _publishText(from: string, data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    return await this._stub._publishUnreported({
      __r: 'data',
      from,
      data,
      ...(retain ? { retain: true } : {}),
    } satisfies RoomDataPublish)
  }

  /** @internal */
  async _publishBinaryFramed(framed: Uint8Array): Promise<ChannelPublishAck> {
    return await this._stub._publishBinaryUnreported(framed)
  }

  /** @internal A local participant completed its voluntary leave. */
  _dropParticipant(id: string): void {
    this._localParticipants.delete(id)
    this._state.applyLeave(id) // the relayed event is absorbed
  }

  private _onEnvelope(envelope: unknown, rawInfo: ChannelPublishInfo): void {
    assert(hasRoomTag(envelope))
    const event = envelope as RoomEnvelope | RoomDmEnvelope | RoomRosterEvent | RoomDemandEvent
    const member = heldMemberOf(event)
    if (member !== null && this._holdsForJoin(member)) {
      this._heldForJoins.push({ envelope, rawInfo })
      return
    }
    switch (event.__r) {
      case 'roster':
        // Positioned presence rosters reflect prior events; later events layer on without pruning directly granted hidden handles.
        this._applyRoster(event.members)
        return
      case 'roster-error':
        this._roster.reject(new Error('Failed to load room participants'))
        return
      case 'data':
        // Tail mode holds server-side (see `RoomStubChannel._tailPending`): text reaches this client only once it subscribes, already selected and ordered, so nothing is buffered here.
        this._state.applyData(event, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
        return
      case 'join':
        this._state.applyJoin(joinedMember(event))
        return
      case 'leave':
        this._state.applyLeave(event.id, leaveCauseFromWire(event))
        return
      case 'p-meta':
        this._acceptParticipantMeta(event.id, event)
        return
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
        // Relayed from this member's private inbox: only its own stub ever receives it.
        const local = this._localParticipants.get(event.to)
        if (local) this._deliverDm(local, inboxMessageFromWire(event))
        else if (event.ackId) this._replyDm(event.to, event.ackId, DM_FAILURE.left)
        return
      }
    }
  }

  /** Every leave the state applies ends the member's local participant: kicked (with the kick's reason), left through
   *  another handle, or missing from a roster. */
  private _onLeave(id: string, cause: LeaveCause | undefined): void {
    const local = this._localParticipants.get(id)
    if (!local) return
    this._localParticipants.delete(id)
    local._onLeft(cause ?? { type: 'removed' })
  }

  private _onBinaryFrame(framed: Uint8Array, rawInfo: ChannelPublishInfo): void {
    const frame = decodeBinaryFrame(framed)
    assert(frame)
    this._state.applyBinary(frame, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
  }

  private _applyRoster(members: MemberSnapshot[]): void {
    this._state.reconcileRoster(members)
    this._syncWants() // per-member binary wants may reference the members just learned
    this._roster.resolve()
  }

  /** @internal Apply an accepted member meta (event or own write's ack) and mirror it into a local participant. */
  _acceptParticipantMeta(id: string, accepted: AcceptedMeta): void {
    this._state.applyParticipantMeta(id, accepted.meta, accepted.seq)
    this._localParticipants.get(id)?._acceptMeta(accepted)
  }

  private _applyClosed(causeType: 'closed' | 'disconnected'): void {
    if (this._state.closed) return
    const cause: LeaveCause = { type: causeType }
    this._closedCause = cause
    this._state.applyClosed(cause)
    this._roster.resolve() // unblock any getParticipants() waiting on a wire that just died
    // After onClose, like on the server: the room-level signal fires before per-handle cleanup.
    for (const local of this._localParticipants.values()) local._onLeft(cause)
    this._localParticipants.clear()
  }

  /** Room-wide text wants ride the Broadcast subscription, which reattaches before the stub's `onOpen`. */
  private _syncWants(): void {
    const state = this._state
    if (state.closed) return this._stub._setWireSubscribed('text', false) // the stub is dead: nothing to declare
    const text = state.textWants()
    // The room-wide stream starts before the member set is cleared and stops after it is declared, so the server
    // never passes through wanting no text (which would stop its lane).
    if (text.all) this._stub._setWireSubscribed('text', true)
    // A room-level text subscription supersedes the member set, so clear it server-side.
    this._declare({ __r: 'sub-text', members: text.all ? [] : text.members, announce: state.wantsAnnounce })
    if (!text.all) this._stub._setWireSubscribed('text', false)
    this._declare({ __r: 'sub-binary', wants: state.binaryWants() })
  }

  /** Declarations are replayed channel messages, so the server keeps them across reconnects: send only changes. */
  private _declare(declaration: WantsDeclaration): void {
    if (this._stub.isClosed) return // a closing stub's server side drops its declarations with it
    const serialized = JSON.stringify(declaration)
    if (this._declared[declaration.__r] === serialized) return
    this._declared[declaration.__r] = serialized
    void this._stub.send(declaration, { ack: false }).catch(() => {})
  }
}

/** Client participant; server-side echo suppression leaves `selfDelivery`
 * as a public read-only flag here. */
abstract class ClientParticipantBase extends ParticipantBase {
  /** Per-key conflation state for `publish(data, { coalesce })`: at most one in-flight send per key; while it's in flight the newest value waits in `pending` and supersedes any earlier one. */
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

  /** The actual wire publish. Each flavor supplies it; `publish()` wraps it with conflation. */
  protected abstract _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck>
  /** The actual wire publish of a framed binary message. */
  protected abstract _sendPublishBinary(framed: Uint8Array): Promise<ChannelPublishAck>

  // Messaging is often fire-and-forget: a usage error throws, and a failure is a rejection left handled.
  publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    assertKnownOptions(options, ['coalesce', 'retain'], 'publish()')
    const key = options?.coalesce
    if (key === undefined) return markHandled(this._sendPublish(data, options?.retain))
    return markHandled(
      new Promise<ChannelPublishAck>((resolve, reject) => {
        let slot = this._coalescers.get(key)
        if (!slot) {
          slot = { sending: false, pending: null }
          this._coalescers.set(key, slot)
        }
        // Supersede any queued value; its waiters ride along and all resolve with the winning send.
        const waiters = [...(slot.pending?.waiters ?? []), { resolve, reject }]
        slot.pending = { data, retain: options?.retain, waiters }
        this._drainCoalesce(key)
      }),
    )
  }

  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    return markHandled(this._sendPublishBinary(encodeBinaryFrame(this.id, data, options)))
  }

  // Implementation of the overloaded `LocalParticipant.send`; the interface supplies precise returns.
  send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    assertKnownOptions(options, ['ack'], 'send()')
    return markHandled(this._sendDm(recipientId(to), data, options?.ack === true))
  }

  private async _sendDm(to: string, data: unknown, ack: boolean): Promise<unknown> {
    this._assertActive()
    return await this._requestParticipant({ __r: 'req-dm', to, data, ...(ack ? { ack: true } : {}) })
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(meta, 'setMeta() meta')
    this._onOwnMetaWritten((await this._requestParticipant({ __r: 'req-set-meta', meta: owned })) as AcceptedMeta)
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(attrs, 'setAttributes() attributes')
    this._onOwnMetaWritten((await this._requestParticipant({ __r: 'req-set-attrs', attrs: owned })) as AcceptedMeta)
  }

  /** Concurrent writes can commit out of request order: adopt the sequence-accepted value, as observers do. */
  protected abstract _onOwnMetaWritten(accepted: AcceptedMeta): void

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
    reportClientCallbackError(err)
  }
}

/** `LocalParticipant` returned by `ClientRoom.join()`. Operates through the room's stub. */
class ClientRoomParticipant extends ClientParticipantBase {
  private readonly _room: ClientRoom

  constructor(clientRoom: ClientRoom, id: string, meta: ParticipantMeta, selfDelivery: boolean) {
    // Client-side joins carry no identity: it's server-assigned (see JoinOptions.identity).
    super(id, meta, selfDelivery, null, (request) => clientRoom._request({ ...request, id } as RoomStubRequest))
    this._room = clientRoom
  }

  protected override _resolveSender(id: string): Sender | null {
    return this._room._getRemote(id)
  }

  protected override _onOwnMetaWritten(accepted: AcceptedMeta): void {
    this._room._acceptParticipantMeta(this.id, accepted)
  }

  protected async _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishText(this.id, data, retain)
  }

  protected async _sendPublishBinary(framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishBinaryFramed(framed)
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this._room._request({ __r: 'req-leave', id: this.id })
    this._room._dropParticipant(this.id)
    this._onLeft({ type: 'left' })
  }
}

/** `LocalParticipant` revived from a serialized `ServerLocalParticipant`. Owns its stub channel. */
class ClientStandaloneParticipant extends ClientParticipantBase {
  private readonly _channel: ClientChannel
  private readonly _request: (req: ParticipantStubRequest) => Promise<unknown>

  constructor(channel: ClientChannel, metadata: ParticipantStubMetadata) {
    const request = (req: ParticipantStubRequest) => channel.send(req, { ack: true })
    super(metadata.id, metadata.meta, metadata.selfDelivery, metadata.identity, request)
    this._channel = channel
    this._request = request

    channel.listen((notice: unknown) => {
      assert(hasRoomTag(notice))
      const msg = notice as ParticipantStubNotice
      switch (msg.__r) {
        case 'p-meta':
          return this._onOwnMetaWritten(msg)
        case 'demand':
          return this._onDemand(msg.track, msg.wanted)
        case 'dm':
          // An ack DM replies through the channel's own ack: the handler's return rides it home.
          if (msg.ackId) return this._deliverMessageAck(inboxMessageFromWire(msg))
          return this._deliverMessage(inboxMessageFromWire(msg))
        case 'left':
          return this._onLeft(leaveCauseFromWire(msg))
      }
    })
    channel.onClose(() => this._onLeft({ type: 'disconnected' }))
  }

  /** Its own writes and the room's `p-meta` both land here. */
  protected override _onOwnMetaWritten(accepted: AcceptedMeta): void {
    this._acceptMeta(accepted)
  }

  protected async _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return (await this._request({ __r: 'req-publish', data, ...(retain ? { retain: true } : {}) })) as ChannelPublishAck
  }

  protected async _sendPublishBinary(framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return (await this._channel.sendBinary(framed, { ack: true })) as ChannelPublishAck
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this._request({ __r: 'req-leave' })
    this._onLeft({ type: 'left' })
    void this._channel.close().catch(() => {})
  }
}

function reportClientCallbackError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
