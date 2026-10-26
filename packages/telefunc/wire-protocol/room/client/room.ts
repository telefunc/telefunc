export { ClientRoom, ClientStandaloneParticipant }

import { createDeferred } from '../../../utils/createDeferred.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { markHandled } from '../../../utils/markHandled.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shieldKey.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../../channel/channel.js'
import type { ClientBroadcast } from '../../channel/client/channel.js'
import { ALL, decodeBinaryFrame, encodeBinaryFrame, type BinaryWantChange, type TrackKey } from '../binary.js'
import { assertKnownOptions, normalizeJoinOptions, ownMessage, ownMetaArgument, recipientId } from '../model.js'
import {
  hasRoomTag,
  inboxMessageFromWire,
  joinedMember,
  leaveCauseFromWire,
  type DmReply,
  type InboxMessage,
  type MemberSnapshot,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type MemberMetaUpdate,
  type MemberMutationRequest,
  type RoomDemandEvent,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type SenderStamp,
  type RoomEnvelope,
  type RoomJoinedEvent,
  type RoomRosterEvent,
  type RoomSnapshotMetadata,
  type RoomStubRequest,
} from '../protocol.js'
import { RoomModel, RoomState, RoomStateView, type MembershipEvent } from '../state.js'
import { ROOM_TRACK_WANTS_MAX } from '../constants.js'
import { CLIENT_ROOM_BUG_MESSAGE, roomResultValue } from '../errors.js'
import { ParticipantBase } from '../participant.js'
import type { MetaRewinds } from '../MemberMeta.js'
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
} from '../types.js'

/** What a room's stub relays to its client on the text lane. */
type ClientRoomEvent = RoomEnvelope | RoomDmEnvelope | RoomRosterEvent | RoomJoinedEvent | RoomDemandEvent

/** A reply that can't be sent is reported, and the sender gets an error instead. */
function sendDmReply(stub: ClientBroadcast, ackId: string, reply: Promise<DmReply>): void {
  const declare = (answer: DmReply): RoomStubRequest => ({ __r: 'dm-reply', ackId, reply: answer })
  stub._sendReply(reply.then(declare), declare({ ok: false, err: CLIENT_ROOM_BUG_MESSAGE }))
}

/** The member an event concerns while that member's join may still be settling. */
function heldMemberOf(event: ClientRoomEvent): string | null {
  switch (event.__r) {
    case 'dm':
      return event.to
    case 'demand':
    case 'demand-state':
      return event.member
    case 'leave':
      return event.id
    default:
      return null
  }
}

/** One awaiter of a conflated publish, resolved with the winning send's receipt (see `_drainCoalesce`). */
type CoalesceWaiter = { resolve: (ack: ChannelPublishAck) => void; reject: (err: unknown) => void }
/** A key's conflation: at most one send in flight, while the newest value waits in `pending`, superseding any
 *  earlier one. */
type CoalesceSlot = { sending: boolean; pending: { data: unknown; retain?: boolean; waiters: CoalesceWaiter[] } | null }
/** A join awaiting its ack: the member the server admitted, once `joined` names it, and the events naming that
 *  member. */
type PendingJoin = { id: string | null; held: Array<{ envelope: unknown; rawInfo: ChannelPublishInfo }> }

/** The rosters streaming in or applying, from the first part of the first until the last applies. */
type RosterStream = {
  /** The roster streaming in: its members so far and the part due next. */
  parts: { members: MemberSnapshot[]; next: number } | null
  /** The members of the rosters streaming in or applying, by id, which an unstamped message may name. */
  pending: Map<string, MemberSnapshot>
  /** The membership events since the stream's first part. Each roster replays them all: those from before its own
   *  first part change nothing it lists. */
  events: readonly MembershipEvent[]
  /** The rosters received and not yet applied, applied in turn. */
  applying: number
  applied: Promise<void>
}

/** The members the page holds, joined through any of its room views or held through their own channel, by id. */
const heldMembers = new Map<string, ClientParticipantBase>()
/** The page's room views, each showing a member the page holds as the page's own connection to it. */
const roomViews = new Set<WeakRef<ClientRoom>>()

function heldConnection(id: string): boolean | null {
  return heldMembers.get(id)?.isConnected ?? null
}

function heldConnectionChanged(id: string, wasConnected: boolean | null): void {
  for (const view of roomViews) {
    const room = view.deref()
    if (room) room._heldConnectionChanged(id, wasConnected)
    else roomViews.delete(view)
  }
}

/** A server room through one Broadcast stub: its frames carry events and data, its acked messages requests. */
class ClientRoom extends RoomStateView implements Room {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  private readonly _stub: ClientBroadcast
  private readonly _model: RoomModel
  protected readonly _state: RoomState
  private readonly _localParticipants = new Map<string, ClientRoomParticipant>()
  /** Joins awaiting their ack, by request, and by the member `joined` named. The server relays to a member from its
   *  admission, before the ack registers it here, so events naming it wait for its ack. */
  private readonly _joins = new Map<number, PendingJoin>()
  private readonly _joiners = new Map<string, PendingJoin>()
  private _joinReqs = 0
  /** The wants the server stub holds, as last declared; a fresh stub holds none. */
  private _declaredAnnounce = false
  private readonly _declaredText = new Set<string>()
  /** Settled by the first `roster` or `roster-error` event, or when this view closes. Gates `getParticipants()`. */
  private readonly _roster = createDeferred()
  private _rosterStream: RosterStream | null = null
  /** @internal */
  readonly _bufferLimit: number

  constructor(stub: ClientBroadcast, snapshot: RoomSnapshotMetadata) {
    super()
    this._stub = stub
    this._bufferLimit = snapshot.bufferLimit
    this._model = new RoomModel({
      roomId: snapshot.roomId,
      meta: snapshot.meta,
      seed: { count: snapshot.count }, // the roster itself streams right behind the response
      metaSeq: snapshot.metaSeq,
      closed: snapshot.closed,
      onLeave: (id, cause) => this._onLeave(id, cause),
      pendingMember: (id) => this._rosterStream?.pending.get(id),
      heldConnection,
    })
    this._state = new RoomState({
      owner: this,
      model: this._model,
      onListenersChanged: (member, binary) => this._syncWants(member, binary),
      onCallbackError: reportClientCallbackError,
      trackWantsMax: ROOM_TRACK_WANTS_MAX,
    })
    if (snapshot.closed) this._roster.resolve()

    // Delivery handlers are local-only. What the server relays is driven by the declared wants: control always arrives,
    // text while subscribed, binary per `sub-binary`.
    stub._subscribeLocal('text', (envelope, info) => this._onEnvelope(envelope, info))
    stub._subscribeLocal('binary', (framed, info) => this._onBinaryFrame(framed, info))
    // Wire death; a server `Room.close()` arrives before it, as the `closed` event.
    const view = new WeakRef(this)
    roomViews.add(view)
    stub.onClose(() => {
      roomViews.delete(view)
      this._applyClosed('disconnected')
    })
    stub.onDisconnect(() => {
      for (const local of this._localParticipants.values()) local._onDisconnected()
    })
    stub.onReconnect(() => {
      for (const local of this._localParticipants.values()) local._onReconnected()
    })
    // A backend rejection can arrive before the application asks for the roster. Mark it handled here while preserving
    // the original rejection for each later getter.
    void this._roster.promise.catch(() => {})
  }

  join(options?: JoinOptions): Promise<LocalParticipant> {
    const joining: Promise<LocalParticipant> = this._join(options).then((participant) => participant._handOut(joining))
    return joining
  }

  private async _join(options?: JoinOptions): Promise<ClientRoomParticipant> {
    assertUsage(
      options?.identity === undefined,
      'join() options.identity is server-assigned: identity is trusted, so set it where trust lives, in the granting telefunction (server-side join()), not on the client.',
    )
    assertUsage(
      options?.hidden === undefined,
      'join() options.hidden is server-side only: a hidden participant is created by the granting telefunction (server-side join({ hidden: true })), not by a client.',
    )
    const { meta, selfDelivery } = normalizeJoinOptions(options)
    const reqId = this._joinReqs++
    const join: PendingJoin = { id: null, held: [] }
    this._joins.set(reqId, join)
    try {
      const { id, joinedAt } = (await this._request({ __r: 'req-join', reqId, meta, selfDelivery })) as {
        id: string
        joinedAt: number
      }
      const participant = new ClientRoomParticipant({
        clientRoom: this,
        id,
        meta,
        selfDelivery,
        rewinds: this._model.metaRewinds,
      })
      const closedCause = this._model.closedCause
      if (closedCause) {
        participant._onLeft(closedCause)
        return participant
      }
      this._localParticipants.set(id, participant)
      this._model.applyJoin({ id, meta, joinedAt, metaSeq: 0, identity: null })
      participant._holdOnPage()
      return participant
    } finally {
      this._joins.delete(reqId)
      if (join.id !== null) this._joiners.delete(join.id)
      for (const { envelope, rawInfo } of join.held) this._onEnvelope(envelope, rawInfo)
    }
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
    if (!this._model.rosterKnown) await this._roster.promise
  }

  /** @internal Sync view read for sender resolution (delivery must not wait on I/O). */
  _getRemote(id: string): RemoteParticipant | null {
    return this._state.getRemote(id)
  }

  /** Plain DM fires local listeners; ack DM returns their reply through the symmetric `dm-reply`/`dm-ack` path. */
  private _deliverDm(participant: ClientRoomParticipant, msg: InboxMessage): void {
    if (msg.ackId === undefined) {
      participant._deliverMessage(msg)
      return
    }
    sendDmReply(this._stub, msg.ackId, participant._deliverMessageAck(msg))
  }

  /** @internal Revival of a serialized `RemoteParticipant` (see `roomRemoteReviver`). */
  _reviveRemote(snap: MemberSnapshot): RemoteParticipant {
    return this._state.ensureRemoteFromSnapshot(snap)
  }

  snapshot(): RoomSnapshotView {
    return this._model.snapshot()
  }

  /** @internal */
  async _request(req: RoomStubRequest): Promise<unknown> {
    return roomResultValue(this._stub.send(req, { ack: true }))
  }

  /** @internal `from` is a claim: the server checks it against this stub's members and stamps `fromMeta` itself. */
  async _publishText(from: string, data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    const publish: RoomDataPublish = { __r: 'data', from, data }
    if (retain) publish.retain = true
    return roomResultValue<ChannelPublishAck>(this._stub._publishUnreported(publish))
  }

  /** @internal */
  async _publishBinaryFramed(framed: Uint8Array): Promise<ChannelPublishAck> {
    return roomResultValue<ChannelPublishAck>(this._stub._publishBinaryUnreported(framed))
  }

  /** @internal A local participant completed its voluntary leave. */
  _dropParticipant(id: string): void {
    this._localParticipants.delete(id)
    this._model.applyLeave(id, { type: 'left' }) // the relayed event is absorbed
  }

  private _onEnvelope(envelope: unknown, rawInfo: ChannelPublishInfo): void {
    assert(hasRoomTag(envelope))
    const event = envelope as ClientRoomEvent
    const member = heldMemberOf(event)
    const join = member === null ? undefined : this._joiners.get(member)
    if (join !== undefined) {
      join.held.push({ envelope, rawInfo })
      return
    }
    switch (event.__r) {
      case 'joined': {
        const joined = this._joins.get(event.reqId)
        if (joined === undefined) return
        joined.id = event.id
        this._joiners.set(event.id, joined)
        return
      }
      case 'roster':
        this._onRosterPart(event)
        return
      case 'roster-error':
        this._roster.reject(new Error('Failed to load room participants'))
        return
      case 'rewound':
        this._model.forgetMetaSeqs()
        return
      case 'data':
        this._state.applyData(event, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
        return
      case 'join':
        this._model.applyJoin(joinedMember(event))
        return
      case 'leave':
        this._model.applyRelayedLeave(event.id, leaveCauseFromWire(event), event.hidden === true)
        return
      case 'p-meta':
        this._acceptParticipantMeta(event.id, event)
        return
      case 'update':
        this._model.applyRoomUpdate(event)
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
      case 'demand-state':
        this._localParticipants.get(event.member)?._applyDemandState(event.tracks)
        return
      case 'dm': {
        // Relayed from this member's private inbox: only its own stub ever receives it.
        const local = this._localParticipants.get(event.to)
        if (!local) return
        const msg = inboxMessageFromWire(event)
        // A room-authored DM has no sender to resolve.
        if (event.from !== '') {
          const sender = this._state.resolveSender(event as RoomDmEnvelope & Partial<SenderStamp>)
          msg.fromMeta = sender.meta
          msg.fromIdentity = sender.identity
        }
        this._deliverDm(local, msg)
        return
      }
    }
  }

  /** Every leave the state applies ends the member's local participant: kicked (with the kick's reason), left through
   *  another handle, or missing from a roster. */
  private _onLeave(id: string, cause: LeaveCause): void {
    const local = this._localParticipants.get(id)
    if (!local) return
    this._localParticipants.delete(id)
    local._onLeft(cause)
  }

  private _onBinaryFrame(framed: Uint8Array, rawInfo: ChannelPublishInfo): void {
    const frame = decodeBinaryFrame(framed)
    assert(frame)
    this._state.applyBinary(frame, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
  }

  private _onRosterPart({ members, part, more }: Extract<RoomRosterEvent, { __r: 'roster' }>): void {
    if (part === 0) {
      this._rosterStream ??= {
        parts: null,
        pending: new Map(),
        events: this._model.recordMembership(),
        applying: 0,
        applied: Promise.resolve(),
      }
      this._rosterStream.parts = { members: [], next: 0 }
    }
    const stream = this._rosterStream
    if (stream === null) return
    const parts = stream.parts
    // A part lost while offline: the next attach sends the whole roster again.
    if (parts === null || part !== parts.next) {
      stream.parts = null
      this._stopRosterEvents()
      return
    }
    for (const member of members) {
      parts.members.push(member)
      stream.pending.set(member.id, member)
    }
    parts.next++
    if (more) return
    stream.parts = null
    const apply = () => this._applyRoster(stream, parts.members)
    stream.applying++
    stream.applied = stream.applying === 1 ? apply() : stream.applied.then(apply)
  }

  private async _applyRoster(stream: RosterStream, members: MemberSnapshot[]): Promise<void> {
    try {
      await this._model.reconcileRoster({ members, departing: new Set() }, stream.events)
      // A meta change of one's own that a gap dropped, as while offline, is in the roster whole.
      for (const [id, local] of this._localParticipants) {
        const accepted = this._model.acceptedMeta(id)
        if (accepted !== null) local._acceptMeta(accepted)
      }
      this._roster.resolve()
    } finally {
      stream.applying--
      this._stopRosterEvents()
    }
  }

  private _stopRosterEvents(): void {
    const stream = this._rosterStream
    if (stream === null || stream.parts !== null || stream.applying > 0) return
    this._rosterStream = null
    this._model.stopRecordingMembership()
  }

  /** @internal */
  get _isConnected(): boolean {
    return this._stub.isConnected
  }

  /** @internal */
  _heldConnectionChanged(id: string, wasConnected: boolean | null): void {
    this._model.heldConnectionChanged(id, wasConnected)
  }

  /** @internal Apply an accepted member meta (event or own write's ack) and mirror it into a local participant. */
  _acceptParticipantMeta(id: string, accepted: MemberMetaUpdate): void {
    this._model.applyParticipantMeta(id, accepted)
    this._localParticipants.get(id)?._acceptMeta(accepted)
  }

  private _applyClosed(causeType: 'closed' | 'disconnected'): void {
    const cause: LeaveCause = { type: causeType }
    if (!this._model.applyClosed(cause)) return
    this._roster.resolve() // unblock any getParticipants() waiting on a wire that just died
    // Local participants end after onClose, as on the server.
    for (const local of this._localParticipants.values()) local._onLeft(cause)
    this._localParticipants.clear()
  }

  /** Room-wide text wants ride the Broadcast subscription, which reattaches before the stub's `onOpen`. Declarations
   *  are replayed channel messages, so the server keeps them across reconnects: each one carries only what changed,
   *  the room-wide text wants and those of `member`, whose listeners changed, or the one binary want that did. */
  private _syncWants(member: string | null, binary: BinaryWantChange | null): void {
    if (this._model.closed) {
      this._stub._setSubscribed('text', false) // a closed room takes no declaration
      return
    }
    const text = this._state.textWants()
    this._stub._setSubscribed('text', text.all)
    // Declared under the room-wide stream too, so the server keeps these members' lane when that stream stops.
    const textChanges: Record<string, boolean> = Object.create(null)
    const memberText = member !== null && text.members.has(member) !== this._declaredText.has(member)
    if (memberText) {
      const wanted = text.members.has(member)
      textChanges[member] = wanted
      if (wanted) this._declaredText.add(member)
      else this._declaredText.delete(member)
    }
    if (memberText || this._state.wantsAnnounce !== this._declaredAnnounce) {
      this._declaredAnnounce = this._state.wantsAnnounce
      this._notify({ __r: 'sub-text', announce: this._state.wantsAnnounce, members: textChanges })
    }
    if (binary === null) return
    const { member: wantMember, track, on } = binary
    this._notify({
      __r: 'sub-binary',
      member: wantMember === ALL ? null : wantMember,
      track: track === ALL ? null : track,
      on,
    })
  }

  /** A closed stub's server side is gone with its wants. */
  private _notify(notice: RoomStubRequest): void {
    if (!this._stub.isClosed) void this._stub.send(notice, { ack: false }).catch(() => {})
  }
}

abstract class ClientParticipantBase extends ParticipantBase {
  /** `publish(data, { coalesce })`'s conflation, by key. */
  private readonly _coalescers = new Map<string, CoalesceSlot>()
  private readonly _wantedTracks = new Set<TrackKey>()
  private readonly _bufferLimit: number
  protected readonly _bugMessage = CLIENT_ROOM_BUG_MESSAGE

  private readonly _requestParticipant: (request: MemberMutationRequest) => Promise<unknown>

  /** @internal The page holds this member from now on, until it leaves. */
  _holdOnPage(): void {
    heldMembers.set(this.id, this)
    heldConnectionChanged(this.id, null)
  }

  override _onDisconnected(): void {
    heldConnectionChanged(this.id, true)
    super._onDisconnected()
  }

  override _onReconnected(): void {
    heldConnectionChanged(this.id, false)
    super._onReconnected()
  }

  override _onLeft(cause: LeaveCause): void {
    super._onLeft(cause)
    if (heldMembers.get(this.id) === this) heldMembers.delete(this.id)
  }

  constructor({
    participant,
    rewinds,
    request,
  }: {
    participant: Pick<ParticipantStubMetadata, 'id' | 'meta' | 'selfDelivery' | 'identity' | 'bufferLimit'>
    rewinds: MetaRewinds
    request: (request: MemberMutationRequest) => Promise<unknown>
  }) {
    super(participant, rewinds)
    this._bufferLimit = participant.bufferLimit
    this._requestParticipant = request
  }

  get _demandedTracks(): TrackKey[] {
    return this._left ? [] : [...this._wantedTracks]
  }

  /** @internal The whole wanted set, as a reattached holder hears it: only the tracks that changed fire. */
  _applyDemandState(tracks: TrackKey[]): void {
    const next = new Set(tracks)
    for (const track of this._wantedTracks) if (!next.has(track)) this._onDemand(track, false)
    for (const track of next) if (!this._wantedTracks.has(track)) this._onDemand(track, true)
  }

  override _onDemand(track: TrackKey, wanted: boolean): void {
    if (wanted) this._wantedTracks.add(track)
    else this._wantedTracks.delete(track)
    super._onDemand(track, wanted)
  }

  /** Each flavor's wire publish; `publish()` adds conflation. */
  protected abstract _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck>
  protected abstract _sendPublishBinary(framed: Uint8Array): Promise<ChannelPublishAck>

  // Messaging is often fire-and-forget: a usage error throws, and a failure is a rejection left handled.
  publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    assertKnownOptions(options, ['coalesce', 'retain'], 'publish()')
    assertUsage(
      options?.retain === undefined || typeof options.retain === 'boolean',
      'publish() options.retain should be a boolean',
    )
    assertUsage(
      options?.coalesce === undefined || typeof options.coalesce === 'string',
      'publish() options.coalesce should be a string',
    )
    const key = options?.coalesce
    if (key === undefined) return markHandled(this._sendPublish(data, options?.retain))
    return markHandled(
      new Promise<ChannelPublishAck>((resolve, reject) => {
        const slot = getOrCreate(this._coalescers, key, () => ({ sending: false, pending: null }))
        // Supersede any queued value; its waiters ride along and all resolve with the winning send.
        const waiters = slot.pending?.waiters ?? []
        waiters.push({ resolve, reject })
        slot.pending = { data: ownMessage(data), retain: options?.retain, waiters }
        this._drainCoalesce(key)
      }),
    )
  }

  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    return markHandled(this._sendPublishBinary(encodeBinaryFrame(this.id, data, options)))
  }

  send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    assertKnownOptions(options, ['ack'], 'send()')
    assertUsage(
      options?.ack === undefined || typeof options.ack === 'boolean',
      'send() options.ack should be a boolean',
    )
    return markHandled(this._sendDm(recipientId(to), data, options?.ack === true))
  }

  private async _sendDm(to: string, data: unknown, ack: boolean): Promise<unknown> {
    this._assertActive()
    const request: MemberMutationRequest = { __r: 'req-dm', to, data }
    if (ack) request.ack = true
    return await this._requestParticipant(request)
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(meta, 'setMeta() meta')
    this._onOwnMetaWritten((await this._requestParticipant({ __r: 'req-set-meta', meta: owned })) as MemberMetaUpdate)
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(attrs, 'setAttributes() attributes')
    this._onOwnMetaWritten((await this._requestParticipant({ __r: 'req-set-attrs', attrs: owned })) as MemberMetaUpdate)
  }

  /** Concurrent writes can commit out of request order: adopt the sequence-accepted value, as observers do. */
  protected abstract _onOwnMetaWritten(accepted: MemberMetaUpdate): void

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

  protected _inboxBufferLimit(): number {
    return this._bufferLimit
  }
}

/** Each member handle's `join()` promise, which keeps a room a telefunction returned open while reachable. */
const roomHolds = new WeakMap<LocalParticipant, Promise<LocalParticipant>>()

/** `LocalParticipant` returned by `ClientRoom.join()`. Operates through the room's stub. */
class ClientRoomParticipant extends ClientParticipantBase {
  private readonly _room: ClientRoom
  /** Weak: the room reaches this member, so a strong one would hold the room open for good. */
  private _handle: WeakRef<LocalParticipant> | null = null

  constructor({
    clientRoom,
    id,
    meta,
    selfDelivery,
    rewinds,
  }: {
    clientRoom: ClientRoom
    id: string
    meta: ParticipantMeta
    selfDelivery: boolean
    rewinds: MetaRewinds
  }) {
    super({
      // Client-side joins carry no identity: it's server-assigned (see JoinOptions.identity).
      participant: { id, meta, selfDelivery, identity: null, bufferLimit: clientRoom._bufferLimit },
      rewinds,
      request: (request) => clientRoom._request({ ...request, id }),
    })
    this._room = clientRoom
  }

  get isConnected(): boolean {
    return this._room._isConnected
  }

  protected override _resolveSender(id: string): Sender | null {
    return this._room._getRemote(id)
  }

  protected override _onOwnMetaWritten(accepted: MemberMetaUpdate): void {
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

  /** @internal The page's handle: it holds the room open until this member leaves, and nothing in the room reaches it. */
  _handOut(joining: Promise<LocalParticipant>): LocalParticipant {
    const bound = new Map<PropertyKey, unknown>()
    const handle = new Proxy<LocalParticipant>(this, {
      get(target, prop) {
        const value: unknown = Reflect.get(target, prop, target)
        return typeof value === 'function' ? getOrCreate(bound, prop, () => value.bind(target)) : value
      },
    })
    if (this._left) return handle
    roomHolds.set(handle, joining)
    this._handle = new WeakRef(handle)
    return handle
  }

  override _onLeft(cause: LeaveCause): void {
    super._onLeft(cause)
    const handle = this._handle?.deref()
    if (handle) roomHolds.delete(handle)
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
  private readonly _stub: ClientBroadcast
  private readonly _request: (req: ParticipantStubRequest) => Promise<unknown>

  constructor(stub: ClientBroadcast, metadata: ParticipantStubMetadata) {
    const request = async (req: ParticipantStubRequest) => roomResultValue(stub.send(req, { ack: true }))
    // Its own: the `rewound` notice forgets its seq.
    super({ participant: metadata, rewinds: { count: 0 }, request })
    this._stub = stub
    this._request = request

    stub._subscribeLocal('text', (notice) => this._onNotice(notice))
    stub.onClose(() => this._onLeft({ type: 'disconnected' }))
    // Held from its channel's open: before it, the channel reads disconnected.
    stub.onOpen(() => this._holdOnPage())
    stub.onDisconnect(() => this._onDisconnected())
    stub.onReconnect(() => this._onReconnected())
  }

  get isConnected(): boolean {
    return this._stub.isConnected
  }

  private _onNotice(notice: unknown): void {
    assert(hasRoomTag(notice))
    const msg = notice as ParticipantStubNotice
    switch (msg.__r) {
      case 'p-meta':
        return this._onOwnMetaWritten(msg)
      case 'rewound':
        return this._forgetMetaSeq()
      case 'demand':
        return this._onDemand(msg.track, msg.wanted)
      case 'demand-state':
        return this._applyDemandState(msg.tracks)
      case 'dm':
        if (msg.ackId === undefined) return this._deliverMessage(inboxMessageFromWire(msg))
        return sendDmReply(this._stub, msg.ackId, this._deliverMessageAck(inboxMessageFromWire(msg)))
      case 'left':
        return this._onLeft(leaveCauseFromWire(msg))
    }
  }

  /** Its own writes and the room's `p-meta` both land here. */
  protected override _onOwnMetaWritten(accepted: MemberMetaUpdate): void {
    this._acceptMeta(accepted)
  }

  protected async _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    const request: ParticipantStubRequest = { __r: 'req-publish', data }
    if (retain) request.retain = true
    return (await this._request(request)) as ChannelPublishAck
  }

  protected async _sendPublishBinary(framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return roomResultValue<ChannelPublishAck>(this._stub._publishBinaryUnreported(framed))
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this._request({ __r: 'req-leave' })
    this._onLeft({ type: 'left' })
    void this._stub.close().catch(() => {})
  }
}

function reportClientCallbackError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
