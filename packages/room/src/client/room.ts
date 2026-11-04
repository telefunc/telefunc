export { ClientRoom, ClientStandaloneParticipant }

import { roomMessages } from './messages.js'
import { assert, assertUsage } from '../utils/assert.js'
import { createDeferred, getOrCreate, markHandled } from 'telefunc/__internal'
import { getGlobalObject } from '../utils/getGlobalObject.js'
import type { TELEFUNC_SHIELDS, ChannelPublishAck, ChannelPublishInfo, ClientBroadcast } from 'telefunc/__internal'
import {
  EVERY_MEMBER,
  EVERY_TRACK,
  decodeBinaryFrame,
  encodeBinaryFrame,
  type BinaryWantChange,
  type TrackKey,
} from '../shared/binary.js'
import {
  assertKnownOptions,
  normalizeJoinOptions,
  ownMessage,
  ownMetaArgument,
  recipientId,
} from '../shared/arguments.js'
import {
  hasRoomTag,
  inboxMessageFromWire,
  joinedMember,
  leaveCauseFromWire,
  type RoomResult,
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
} from '../shared/protocol.js'
import { RoomModel, type MembershipEvent, type RoomModelOwner } from '../shared/RoomModel.js'
import { RoomState, RoomStateView, type RoomStateOwner } from '../shared/state.js'
import { ROOM_TRACK_WANTS_MAX } from '../shared/constants.js'
import { CLIENT_ROOM_BUG_MESSAGE, roomResultValue } from '../shared/errors.js'
import { ParticipantBase } from '../shared/ParticipantBase.js'
import type { MetaRewinds } from '../shared/MemberMeta.js'
import type {
  BinaryPublishOptions,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  Room,
  RoomMessage,
  RoomSnapshotView,
  Sender,
} from '../types.js'

/** What a room's stub relays to its client on the text lane. */
type ClientRoomEvent = RoomEnvelope | RoomDmEnvelope | RoomRosterEvent | RoomJoinedEvent | RoomDemandEvent

/** A reply that can't be sent is reported, and the sender gets an error instead. */
function sendDmReply(end: ClientBroadcast, ackId: string, reply: Promise<RoomResult>): void {
  const declare = (answer: RoomResult): RoomStubRequest => ({ __r: 'dm-reply', ackId, reply: answer })
  end._sendReply(reply.then(declare), declare({ ok: false, kind: 'error', message: CLIENT_ROOM_BUG_MESSAGE }))
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
    case 'p-meta':
      return event.id
    default:
      return null
  }
}

/** One awaiter of a conflated publish, resolved with the winning send's receipt (see `drainCoalesce`). */
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

const { heldMembers, roomViews, roomHolds } = getGlobalObject('client/room.ts', () => ({
  /** The members the page holds, joined through any of its room views or held through their own channel, by id. */
  heldMembers: new Map<string, ClientParticipantBase>(),
  /** The page's room views, each showing a member the page holds as the page's own connection to it. */
  roomViews: new Set<WeakRef<ClientRoom>>(),
  /** Each member handle's `join()` promise, which keeps a room a telefunction returned open while reachable. */
  roomHolds: new WeakMap<LocalParticipant, Promise<LocalParticipant>>(),
}))

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

type ClientRoomOptions = { end: ClientBroadcast; snapshot: RoomSnapshotMetadata }

/** A server room through one Broadcast channel: its frames carry events and data, its acked messages requests. */
class ClientRoom extends RoomStateView implements Room {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  private readonly end: ClientBroadcast
  private readonly model: RoomModel
  /** @internal */
  readonly _state: RoomState
  private readonly localParticipants = new Map<string, ClientRoomParticipant>()
  /** Joins awaiting their ack, by request, and by the member `joined` named. The server relays to a member from its
   *  admission, before the ack registers it here, so events naming it wait for its ack. */
  private readonly joins = new Map<number, PendingJoin>()
  private readonly joiners = new Map<string, PendingJoin>()
  private joinReqs = 0
  /** The wants the server stub holds, as last declared; a fresh stub holds none. */
  private declaredAnnounce = false
  private readonly declaredText = new Set<string>()
  /** Settled by the first `roster` or `roster-error` event, or when this view closes. Gates `getParticipants()`. */
  private readonly roster = createDeferred()
  /** @internal */
  _rosterStream: RosterStream | null = null
  /** @internal */
  readonly _bufferLimit: number

  messages(options?: { signal?: AbortSignal }): AsyncIterableIterator<RoomMessage> {
    return roomMessages(this, this._bufferLimit, options?.signal)
  }

  constructor({ end, snapshot }: ClientRoomOptions) {
    super()
    this.end = end
    this._bufferLimit = snapshot.bufferLimit
    const owner = new ClientRoomOwner({ room: this })
    this.model = new RoomModel({
      roomId: snapshot.roomId,
      meta: snapshot.meta,
      seed: { count: snapshot.count }, // the roster itself streams right behind the response
      metaSeq: snapshot.metaSeq,
      closed: snapshot.closed,
      owner,
    })
    this._state = new RoomState({ view: this, model: this.model, owner, trackWantsMax: ROOM_TRACK_WANTS_MAX })
    if (snapshot.closed) this.roster.resolve()

    // Delivery handlers are local-only. What the server relays is driven by the declared wants: control always arrives,
    // text while subscribed, binary per `sub-binary`.
    end._subscribeLocal('text', (envelope, info) => this.onEnvelope(envelope, info))
    end._subscribeLocal('binary', (framed, info) => this.onBinaryFrame(framed, info))
    // Wire death; a server `Room.close()` arrives before it, as the `closed` event.
    const view = new WeakRef(this)
    roomViews.add(view)
    end.onClose(() => {
      roomViews.delete(view)
      this.applyClosed('disconnected')
    })
    end.onDisconnect(() => {
      for (const local of this.localParticipants.values()) local._onDisconnected()
    })
    end.onReconnect(() => {
      for (const local of this.localParticipants.values()) local._onReconnected()
    })
    // A backend rejection can arrive before the application asks for the roster. Mark it handled here while preserving
    // the original rejection for each later getter.
    void this.roster.promise.catch(() => {})
  }

  join(options?: JoinOptions): Promise<LocalParticipant> {
    const joining: Promise<LocalParticipant> = this.requestJoin(options).then((participant) =>
      participant._handOut(joining),
    )
    return joining
  }

  private async requestJoin(options?: JoinOptions): Promise<ClientRoomParticipant> {
    assertUsage(
      options?.identity === undefined,
      'join() options.identity is server-assigned: identity is trusted, so set it where trust lives, in the ' +
        'granting telefunction (server-side join()), not on the client.',
    )
    assertUsage(
      options?.hidden === undefined,
      'join() options.hidden is server-side only: a hidden participant is created by the granting telefunction ' +
        '(server-side join({ hidden: true })), not by a client.',
    )
    const { meta, selfDelivery } = normalizeJoinOptions(options)
    const reqId = this.joinReqs++
    const join: PendingJoin = { id: null, held: [] }
    this.joins.set(reqId, join)
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
        rewinds: this.model.metaRewinds,
      })
      const closedCause = this.model.closedCause
      if (closedCause) {
        participant._onLeft(closedCause)
        return participant
      }
      this.localParticipants.set(id, participant)
      this.model.applyJoin({ id, meta, joinedAt, metaSeq: 0, identity: null })
      participant._holdOnPage()
      return participant
    } finally {
      this.joins.delete(reqId)
      if (join.id !== null) this.joiners.delete(join.id)
      for (const { envelope, rawInfo } of join.held) this.onEnvelope(envelope, rawInfo)
    }
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    assertUsage(!options?.hidden, 'Hidden participants can only be enumerated on the server')
    await this.awaitRoster()
    return this._state.listVisible()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this.awaitRoster()
    return this._state.getRemote(id)
  }

  private async awaitRoster(): Promise<void> {
    if (!this.model.rosterKnown) await this.roster.promise
  }

  /** Plain DM fires local listeners; ack DM returns their reply through the symmetric `dm-reply`/`dm-ack` path. */
  private deliverDm(participant: ClientRoomParticipant, msg: InboxMessage): void {
    if (msg.ackId === undefined) {
      participant._deliverMessage(msg)
      return
    }
    sendDmReply(this.end, msg.ackId, participant._deliverMessageAck(msg))
  }

  snapshot(): RoomSnapshotView {
    return this.model.snapshot()
  }

  /** @internal */
  async _request(req: RoomStubRequest): Promise<unknown> {
    return roomResultValue(this.end.send(req, { ack: true }))
  }

  /** @internal `from` is a claim: the server checks it against this stub's members and stamps `fromMeta` itself. */
  async _publishText(from: string, data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    const publish: RoomDataPublish = { __r: 'data', from, data }
    if (retain) publish.retain = true
    return roomResultValue<ChannelPublishAck>(this.end._publishUnreported(publish))
  }

  /** @internal */
  async _publishBinaryFramed(framed: Uint8Array): Promise<ChannelPublishAck> {
    return roomResultValue<ChannelPublishAck>(this.end._publishBinaryUnreported(framed))
  }

  /** @internal A local participant completed its voluntary leave. */
  _dropParticipant(id: string): void {
    this.localParticipants.delete(id)
    this.model.applyLeave(id, { type: 'left' }) // the relayed event is absorbed
  }

  private onEnvelope(envelope: unknown, rawInfo: ChannelPublishInfo): void {
    assert(hasRoomTag(envelope))
    const event = envelope as ClientRoomEvent
    const member = heldMemberOf(event)
    const join = member === null ? undefined : this.joiners.get(member)
    if (join !== undefined) {
      join.held.push({ envelope, rawInfo })
      return
    }
    switch (event.__r) {
      case 'joined': {
        const joined = this.joins.get(event.reqId)
        if (joined === undefined) return
        joined.id = event.id
        this.joiners.set(event.id, joined)
        return
      }
      case 'roster':
        this.onRosterPart(event)
        return
      case 'roster-error':
        this.roster.reject(new Error('Failed to load room participants'))
        return
      case 'rewound':
        this.model.forgetMetaSeqs()
        return
      case 'data':
        this._state.applyData(event, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
        return
      case 'join':
        this.model.applyJoin(joinedMember(event))
        return
      case 'leave':
        this.model.applyRelayedLeave(event.id, leaveCauseFromWire(event), event.hidden === true)
        return
      case 'p-meta':
        this._acceptParticipantMeta(event.id, event)
        return
      case 'update':
        this.model.applyRoomUpdate(event)
        return
      case 'closed':
        this.applyClosed('closed')
        return
      case 'announce':
        this._state.applyAnnounce(event.data, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
        return
      case 'demand':
        // Whether anyone wants one of our own members' tracks flipped (onDemand).
        this.localParticipants.get(event.member)?._onDemand(event.track, event.wanted)
        return
      case 'demand-state':
        this.localParticipants.get(event.member)?._applyDemandState(event.tracks)
        return
      case 'dm': {
        // Relayed from this member's private inbox: only its own stub ever receives it.
        const local = this.localParticipants.get(event.to)
        if (!local) return
        const msg = inboxMessageFromWire(event)
        // A room-authored DM has no sender to resolve.
        if (event.from !== '') {
          const sender = this._state.resolveSender(event as RoomDmEnvelope & Partial<SenderStamp>)
          msg.fromMeta = sender.meta
          msg.fromIdentity = sender.identity
        }
        this.deliverDm(local, msg)
        return
      }
    }
  }

  /** @internal Every leave the state applies ends the member's local participant: kicked (with the kick's reason),
   *  left through another handle, or missing from a roster. */
  _onLeave(id: string, cause: LeaveCause): void {
    const local = this.localParticipants.get(id)
    if (!local) return
    this.localParticipants.delete(id)
    local._onLeft(cause)
  }

  private onBinaryFrame(framed: Uint8Array, rawInfo: ChannelPublishInfo): void {
    const frame = decodeBinaryFrame(framed)
    assert(frame)
    this._state.applyBinary(frame, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
  }

  private onRosterPart({ members, part, more }: Extract<RoomRosterEvent, { __r: 'roster' }>): void {
    if (part === 0) {
      this._rosterStream ??= {
        parts: null,
        pending: new Map(),
        events: this.model.recordMembership(),
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
      this.stopRosterEvents()
      return
    }
    for (const member of members) {
      parts.members.push(member)
      stream.pending.set(member.id, member)
    }
    parts.next++
    if (more) return
    stream.parts = null
    const apply = () => this.applyRoster(stream, parts.members)
    stream.applying++
    stream.applied = stream.applying === 1 ? apply() : stream.applied.then(apply)
  }

  private async applyRoster(stream: RosterStream, members: MemberSnapshot[]): Promise<void> {
    try {
      await this.model.reconcileRoster({ members }, stream.events)
      // A meta change of one's own that a gap dropped, as while offline, is in the roster whole.
      for (const [id, local] of this.localParticipants) {
        const accepted = this.model.acceptedMeta(id)
        if (accepted !== null) local._acceptMeta(accepted)
      }
      this.roster.resolve()
    } finally {
      stream.applying--
      this.stopRosterEvents()
    }
  }

  private stopRosterEvents(): void {
    const stream = this._rosterStream
    if (stream === null || stream.parts !== null || stream.applying > 0) return
    this._rosterStream = null
    this.model.stopRecordingMembership()
  }

  /** @internal */
  get _isConnected(): boolean {
    return this.end.isConnected
  }

  /** @internal */
  _heldConnectionChanged(id: string, wasConnected: boolean | null): void {
    this.model.heldConnectionChanged(id, wasConnected)
  }

  /** @internal Apply an accepted member meta (event or own write's ack) and mirror it into a local participant. */
  _acceptParticipantMeta(id: string, accepted: MemberMetaUpdate): void {
    this.model.applyParticipantMeta(id, accepted)
    this.localParticipants.get(id)?._acceptMeta(accepted)
  }

  private applyClosed(causeType: 'closed' | 'disconnected'): void {
    const cause: LeaveCause = { type: causeType }
    if (!this.model.applyClosed(cause)) return
    this.roster.resolve() // unblock any getParticipants() waiting on a wire that just died
    // Local participants end after onClose, as on the server.
    for (const local of this.localParticipants.values()) local._onLeft(cause)
    this.localParticipants.clear()
  }

  /** Room-wide text wants ride the Broadcast subscription, which attaches again before the channel's `onOpen`.
   *  Declarations are replayed channel messages, so the server keeps them across reconnects: each carries only what
   *  changed, the room-wide text wants and those of `member`, whose listeners changed, or the binary want that did. */
  /** @internal */
  _syncWants(member: string | null, binary: BinaryWantChange | null): void {
    if (this.model.closed) {
      this.end._setSubscribed('text', false) // a closed room takes no declaration
      return
    }
    const text = this._state.textWants()
    this.end._setSubscribed('text', text.all)
    // Declared under the room-wide stream too, so the server keeps these members' lane when that stream stops.
    const textChanges: Record<string, boolean> = Object.create(null)
    const memberText = member !== null && text.members.has(member) !== this.declaredText.has(member)
    if (memberText) {
      const wanted = text.members.has(member)
      textChanges[member] = wanted
      if (wanted) this.declaredText.add(member)
      else this.declaredText.delete(member)
    }
    if (memberText || this._state.wantsAnnounce !== this.declaredAnnounce) {
      this.declaredAnnounce = this._state.wantsAnnounce
      this.notify({ __r: 'sub-text', announce: this._state.wantsAnnounce, members: textChanges })
    }
    if (binary === null) return
    const { member: wantMember, track, on } = binary
    this.notify({
      __r: 'sub-binary',
      member: wantMember === EVERY_MEMBER ? null : wantMember,
      track: track === EVERY_TRACK ? null : track,
      on,
    })
  }

  /** A closed channel's server side is gone with its wants. */
  private notify(notice: RoomStubRequest): void {
    if (!this.end.isClosed) void this.end.send(notice, { ack: false }).catch(() => {})
  }
}

type ClientParticipantBaseOptions = {
  participant: Pick<ParticipantStubMetadata, 'id' | 'meta' | 'selfDelivery' | 'identity' | 'bufferLimit'>
  rewinds: MetaRewinds
}

abstract class ClientParticipantBase extends ParticipantBase {
  /** `publish(data, { coalesce })`'s conflation, by key. */
  private readonly coalescers = new Map<string, CoalesceSlot>()
  private readonly wantedTracks = new Set<TrackKey>()
  private readonly bufferLimit: number
  protected readonly _bugMessage = CLIENT_ROOM_BUG_MESSAGE

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

  constructor({ participant, rewinds }: ClientParticipantBaseOptions) {
    super({ participant, rewinds })
    this.bufferLimit = participant.bufferLimit
  }

  get _demandedTracks(): TrackKey[] {
    return this._left ? [] : [...this.wantedTracks]
  }

  /** @internal The whole wanted set, as a holder hears it when it attaches again: only the tracks that changed fire. */
  _applyDemandState(tracks: TrackKey[]): void {
    const next = new Set(tracks)
    for (const track of this.wantedTracks) if (!next.has(track)) this._onDemand(track, false)
    for (const track of next) if (!this.wantedTracks.has(track)) this._onDemand(track, true)
  }

  override _onDemand(track: TrackKey, wanted: boolean): void {
    if (wanted) this.wantedTracks.add(track)
    else this.wantedTracks.delete(track)
    super._onDemand(track, wanted)
  }

  /** Each flavor's wire publish; `publish()` adds conflation. */
  protected abstract _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck>
  protected abstract _sendPublishBinary(framed: Uint8Array): Promise<ChannelPublishAck>
  /** The wire request of a flavor: the base's DMs and metadata writes, and the standalone's publish and leave. */
  protected abstract _request(request: MemberMutationRequest): Promise<unknown>

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
        const slot = getOrCreate(this.coalescers, key, () => ({ sending: false, pending: null }))
        // Supersede any queued value; its waiters ride along and all resolve with the winning send.
        const waiters = slot.pending?.waiters ?? []
        waiters.push({ resolve, reject })
        slot.pending = { data: ownMessage(data), retain: options?.retain, waiters }
        this.drainCoalesce(key)
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
    return markHandled(this.sendDm(recipientId(to), data, options?.ack === true))
  }

  private async sendDm(to: string, data: unknown, ack: boolean): Promise<unknown> {
    this._assertActive()
    const request: MemberMutationRequest = { __r: 'req-dm', to, data }
    if (ack) request.ack = true
    return await this._request(request)
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(meta, 'setMeta() meta')
    this._onOwnMetaWritten((await this._request({ __r: 'req-set-meta', meta: owned })) as MemberMetaUpdate)
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(attrs, 'setAttributes() attributes')
    this._onOwnMetaWritten((await this._request({ __r: 'req-set-attrs', attrs: owned })) as MemberMetaUpdate)
  }

  /** Concurrent writes can commit out of request order: adopt the sequence-accepted value, as observers do. */
  protected abstract _onOwnMetaWritten(accepted: MemberMetaUpdate): void

  private drainCoalesce(key: string): void {
    const slot = this.coalescers.get(key)
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
        if (slot.pending) this.drainCoalesce(key)
        else this.coalescers.delete(key)
      })
  }

  protected _reportError(err: unknown): void {
    reportClientCallbackError(err)
  }

  protected _inboxBufferLimit(): number {
    return this.bufferLimit
  }
}

type ClientRoomOwnerOptions = { room: ClientRoom }

/** What a `ClientRoom`'s model and state ask of it, kept off the room itself, which is the user's `Room`. */
class ClientRoomOwner implements RoomModelOwner, RoomStateOwner {
  private readonly room: ClientRoom

  constructor({ room }: ClientRoomOwnerOptions) {
    this.room = room
  }

  reportCallbackError(error: unknown): void {
    reportClientCallbackError(error)
  }

  onMemberLeft(id: string, cause: LeaveCause): void {
    this.room._onLeave(id, cause)
  }

  pendingMember(id: string): MemberSnapshot | undefined {
    return this.room._rosterStream?.pending.get(id)
  }

  heldConnection(id: string): boolean | null {
    return heldConnection(id)
  }

  onListenersChanged(member: string | null, binary: BinaryWantChange | null): void {
    this.room._syncWants(member, binary)
  }
}

type ClientRoomParticipantOptions = {
  clientRoom: ClientRoom
  id: string
  meta: ParticipantMeta
  selfDelivery: boolean
  rewinds: MetaRewinds
}

/** `LocalParticipant` returned by `ClientRoom.join()`. Operates through the room's channel. */
class ClientRoomParticipant extends ClientParticipantBase {
  private readonly room: ClientRoom
  /** Weak: the room reaches this member, so a strong one would hold the room open for good. */
  private handle: WeakRef<LocalParticipant> | null = null

  constructor({ clientRoom, id, meta, selfDelivery, rewinds }: ClientRoomParticipantOptions) {
    super({
      // Client-side joins carry no identity: it's server-assigned (see JoinOptions.identity).
      participant: { id, meta, selfDelivery, identity: null, bufferLimit: clientRoom._bufferLimit },
      rewinds,
    })
    this.room = clientRoom
  }

  get isConnected(): boolean {
    return this.room._isConnected
  }

  protected override _resolveSender(id: string): Sender | null {
    // A sync read: delivery must not wait on I/O.
    return this.room._state.getRemote(id)
  }

  protected override _onOwnMetaWritten(accepted: MemberMetaUpdate): void {
    this.room._acceptParticipantMeta(this.id, accepted)
  }

  protected async _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this.room._publishText(this.id, data, retain)
  }

  protected async _sendPublishBinary(framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this.room._publishBinaryFramed(framed)
  }

  protected _request(request: MemberMutationRequest): Promise<unknown> {
    return this.room._request({ ...request, id: this.id })
  }

  /** @internal The page's handle: it holds the room open until this member leaves, and nothing in the room reaches
   *  it. */
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
    this.handle = new WeakRef(handle)
    return handle
  }

  override _onLeft(cause: LeaveCause): void {
    super._onLeft(cause)
    const handle = this.handle?.deref()
    if (handle) roomHolds.delete(handle)
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this.room._request({ __r: 'req-leave', id: this.id })
    this.room._dropParticipant(this.id)
    this._onLeft({ type: 'left' })
  }
}

type ClientStandaloneParticipantOptions = { end: ClientBroadcast; metadata: ParticipantStubMetadata }

/** `LocalParticipant` revived from a serialized `ServerLocalParticipant`. Owns its channel. */
class ClientStandaloneParticipant extends ClientParticipantBase {
  private readonly end: ClientBroadcast

  constructor({ end, metadata }: ClientStandaloneParticipantOptions) {
    // Its own: the `rewound` notice forgets its seq.
    super({ participant: metadata, rewinds: { count: 0 } })
    this.end = end

    end._subscribeLocal('text', (notice) => this.onNotice(notice))
    end.onClose(() => this._onLeft({ type: 'disconnected' }))
    // Held from its channel's open: before it, the channel reads disconnected.
    end.onOpen(() => this._holdOnPage())
    end.onDisconnect(() => this._onDisconnected())
    end.onReconnect(() => this._onReconnected())
  }

  get isConnected(): boolean {
    return this.end.isConnected
  }

  private onNotice(notice: unknown): void {
    assert(hasRoomTag(notice))
    const msg = notice as ParticipantStubNotice
    switch (msg.__r) {
      case 'p-meta':
        return this._onOwnMetaWritten(msg)
      case 'rewound':
        return this.memberMeta.forget()
      case 'demand':
        return this._onDemand(msg.track, msg.wanted)
      case 'demand-state':
        return this._applyDemandState(msg.tracks)
      case 'dm':
        if (msg.ackId === undefined) return this._deliverMessage(inboxMessageFromWire(msg))
        return sendDmReply(this.end, msg.ackId, this._deliverMessageAck(inboxMessageFromWire(msg)))
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
    return roomResultValue<ChannelPublishAck>(this.end._publishBinaryUnreported(framed))
  }

  protected async _request(request: ParticipantStubRequest): Promise<unknown> {
    return roomResultValue(this.end.send(request, { ack: true }))
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this._request({ __r: 'req-leave' })
    this._onLeft({ type: 'left' })
    void this.end.close().catch(() => {})
  }
}

function reportClientCallbackError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
