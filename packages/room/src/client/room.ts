export { ClientRoom }

import { assert, assertUsage } from '../utils/assert.js'
import type { ChannelPublishAck, ChannelPublishInfo, ClientBroadcast } from 'telefunc/__internal'
import { EVERY_MEMBER, EVERY_TRACK, decodeBinaryFrame, type BinaryWantChange } from '../shared/binary.js'
import { assertKnownOptions, normalizeJoinOptions } from '../shared/arguments.js'
import {
  hasRoomTag,
  inboxMessageFromWire,
  joinedMember,
  leaveCauseFromWire,
  type InboxMessage,
  type MemberSnapshot,
  type MemberMetaUpdate,
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
import { RoomModel, departureCause, type RoomModelOwner } from '../shared/RoomModel.js'
import { RosterIntake } from './RosterIntake.js'
import { RoomState, RoomStateView, type RoomStateOwner } from '../shared/state.js'
import { ROOM_TRACK_WANTS_MAX } from '../shared/constants.js'
import { roomResultValue } from '../shared/errors.js'
import { ClientRoomParticipant, reportClientCallbackError, request, sendDmReply } from './ClientParticipant.js'
import { heldConnection, roomViews } from './page.js'
import type { JoinOptions, LeaveCause, LocalParticipant, RemoteParticipant, Room } from '../types.js'

/** What a room's stub relays to its client on the text lane. */
type ClientRoomEvent = RoomEnvelope | RoomDmEnvelope | RoomRosterEvent | RoomJoinedEvent | RoomDemandEvent

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

/** A join awaiting its ack: the member the server admitted, once `joined` names it, and the events naming that
 *  member. */
type PendingJoin = { id: string | null; held: Array<{ envelope: unknown; rawInfo: ChannelPublishInfo }> }

type ClientRoomOptions = { end: ClientBroadcast; snapshot: RoomSnapshotMetadata }

/** A server room through one Broadcast channel: its frames carry events and data, its acked messages requests. */
class ClientRoom extends RoomStateView implements Room {
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
  /** @internal */
  readonly _rosters: RosterIntake

  constructor({ end, snapshot }: ClientRoomOptions) {
    super()
    this.end = end
    const owner = new ClientRoomOwner({ room: this })
    this.model = new RoomModel({
      roomId: snapshot.roomId,
      meta: snapshot.meta,
      seed: { members: snapshot.members },
      metaSeq: snapshot.metaSeq,
      closed: snapshot.closed,
      owner,
    })
    this._state = new RoomState({ view: this, model: this.model, owner, trackWantsMax: ROOM_TRACK_WANTS_MAX })
    // A meta change of one's own that a gap dropped, as while offline, is in the roster whole.
    this._rosters = new RosterIntake({ model: this.model, onApplied: () => this.acceptOwnMeta() })

    // Delivery handlers are local-only. What the server relays is driven by the declared wants: control always arrives,
    // text while subscribed, binary per `sub-binary`.
    end._subscribeLocal('text', (envelope, info) => this.onEnvelope(envelope, info))
    end._subscribeLocal('binary', (framed, info) => this.onBinaryFrame(framed, info))
    // Wire death; a server `Room.close()` arrives before it, as the `closed` event.
    const view = new WeakRef(this)
    roomViews.add(view)
    end.onClose(() => {
      roomViews.delete(view)
      this.applyClosed({ type: 'disconnected' })
    })
    end.onDisconnect(() => {
      for (const local of this.localParticipants.values()) local._onDisconnected()
    })
    end.onReconnect(() => {
      for (const local of this.localParticipants.values()) local._onReconnected()
    })
    // A page's room runs the package fields until it closes.
    this._state.attachFields()
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
        participant._onLeft(departureCause(closedCause))
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

  getParticipants(options?: { hidden?: boolean }): RemoteParticipant[] {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    assertUsage(!options?.hidden, 'Hidden participants can only be enumerated on the server')
    return this._read.listVisible()
  }

  /** Plain DM fires local listeners; ack DM returns their reply through the symmetric `dm-reply`/`dm-ack` path. */
  private deliverDm(participant: ClientRoomParticipant, msg: InboxMessage): void {
    if (msg.ackId === undefined) {
      participant._deliverMessage(msg)
      return
    }
    sendDmReply(this.end, msg.ackId, participant._deliverMessageAck(msg))
  }

  /** @internal */
  _request(req: RoomStubRequest): Promise<unknown> {
    return request(this.end, req)
  }

  /** @internal `from` is a claim: the server checks it against this stub's members and stamps `fromMeta` itself. */
  async _publishText(from: string, data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    const publish: RoomDataPublish = { __r: 'data', from, data }
    if (retain) publish.retain = true
    return roomResultValue<ChannelPublishAck>(this.end._publishUnreported(publish))
  }

  /** @internal */
  async _publishBinaryFramed(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    return roomResultValue<ChannelPublishAck>(this.end._publishBinaryUnreported(framed, stream, lossy))
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
        this._rosters.part(event)
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
        this.model.applyLeave(event.id, leaveCauseFromWire(event))
        return
      case 'p-meta':
        this._acceptParticipantMeta(event.id, event)
        return
      case 'update':
        this.model.applyRoomUpdate(event)
        return
      case 'closed':
        this.applyClosed(event.cause === 'removed' ? leaveCauseFromWire(event) : { type: 'closed' })
        return
      case 'announce':
        this._state.applyAnnounce(event.data, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
        return
      case 'demand':
        // Who receives one of our own members' tracks changed.
        this.localParticipants.get(event.member)?._applyDemand(event.track, event)
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
    if (frame.lossy) this.end._confirmSoon()
    this._state.applyBinary(frame, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
  }

  private acceptOwnMeta(): void {
    for (const [id, local] of this.localParticipants) {
      const accepted = this.model.acceptedMeta(id)
      if (accepted !== null) local._acceptMeta(accepted)
    }
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

  private applyClosed(cause: LeaveCause): void {
    if (!this.model.applyClosed(cause)) return
    this._state.detachFields()
    // Local participants end after onClose, as on the server.
    for (const local of this.localParticipants.values()) local._onLeft(departureCause(cause))
    this.localParticipants.clear()
  }

  /** @internal Room-wide text wants ride the Broadcast subscription, which attaches again before the channel's
   *  `onOpen`. Declarations are replayed channel messages, so the server keeps them across reconnects: each carries
   *  only what changed, the room-wide text wants and those of `member`, whose listeners changed, or the binary want
   *  that did. */
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
    return this.room._rosters.pending(id)
  }

  heldConnection(id: string): boolean | null {
    return heldConnection(id)
  }

  onListenersChanged(member: string | null, binary: BinaryWantChange | null): void {
    this.room._syncWants(member, binary)
  }
}
