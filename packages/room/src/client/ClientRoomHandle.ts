export { ClientRoomHandle }
export type { ClientRoomHandleOptions }

import { assert, assertUsage } from '../utils/assert.js'
import type { ChannelPublishAck, ChannelPublishInfo, ClientBroadcast } from 'telefunc/__internal'
import { EVERY_MEMBER, EVERY_TRACK, decodeBinaryFrame, type BinaryWantChange } from '../shared/binary.js'
import { normalizeJoinOptions } from '../shared/arguments.js'
import {
  hasRoomTag,
  inboxMessageFromWire,
  leaveCauseFromWire,
  type InboxMessage,
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
import { departureCause } from '../shared/RoomModel.js'
import { RoomState, type RoomStateOwner, type RoomStateView } from '../shared/state.js'
import { ROOM_TRACK_WANTS_MAX } from '../shared/constants.js'
import { roomResultValue } from '../shared/errors.js'
import { RoomMirror } from './RoomMirror.js'
import { ClientRoomParticipant, reportClientCallbackError, request, sendDmReply } from './ClientParticipant.js'
import { addRoom } from './page.js'
import type { JoinOptions, LeaveCause } from '../types.js'

/** What a room's stub relays to its client on the text lane. */
type ClientRoomEvent = RoomEnvelope | RoomDmEnvelope | RoomRosterEvent | RoomJoinedEvent | RoomDemandEvent

type ClientRoomHandleOptions = {
  end: ClientBroadcast
  snapshot: RoomSnapshotMetadata
  /** The user's `Room` over the handle, which the room's state is the state of. */
  view: RoomStateView
}

/** What one room a telefunction returned is to its server stub, through the room's own Broadcast channel: its frames
 *  carry events and data, its acked messages requests. The user's `Room` over it carries none of this. */
class ClientRoomHandle implements RoomStateOwner {
  readonly state: RoomState
  private readonly end: ClientBroadcast
  private readonly mirror: RoomMirror
  private readonly localParticipants = new Map<string, ClientRoomParticipant>()
  private readonly joins = new PendingJoins()
  /** The wants the server stub holds, as last declared; a fresh stub holds none. */
  private declaredAnnounce = false
  private readonly declaredText = new Set<string>()

  constructor({ end, snapshot, view }: ClientRoomHandleOptions) {
    this.end = end
    this.mirror = new RoomMirror({
      snapshot,
      onMemberLeft: (id, cause) => this.memberLeft(id, cause),
      // A meta change of one's own that a gap dropped, as while offline, is in the roster whole.
      onRosterApplied: () => this.acceptOwnMeta(),
    })
    this.state = new RoomState({ view, model: this.mirror.model, owner: this, trackWantsMax: ROOM_TRACK_WANTS_MAX })

    // Delivery handlers are local-only. What the server relays is driven by the declared wants: control always arrives,
    // text while subscribed, binary per `sub-binary`.
    end._subscribeLocal('text', (envelope, info) => this.onEnvelope(envelope, info))
    end._subscribeLocal('binary', (framed, info) => this.onBinaryFrame(framed, info))
    // Wire death; a server `Room.close()` arrives before it, as the `closed` event.
    const removeRoom = addRoom(this)
    end.onClose(() => {
      removeRoom()
      this.applyClosed({ type: 'disconnected' })
    })
    end.onDisconnect(() => {
      for (const local of this.localParticipants.values()) local._onDisconnected()
    })
    end.onReconnect(() => {
      for (const local of this.localParticipants.values()) local._onReconnected()
    })
    // A page's room runs the package fields until it closes.
    this.state.attachFields()
  }

  get id(): string {
    return this.mirror.model.roomId
  }

  async join(options?: JoinOptions): Promise<ClientRoomParticipant> {
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
    const reqId = this.joins.start()
    try {
      const { id, joinedAt } = (await this.request({ __r: 'req-join', reqId, meta, selfDelivery })) as {
        id: string
        joinedAt: number
      }
      const participant = new ClientRoomParticipant({
        room: this,
        id,
        meta,
        selfDelivery,
        rewinds: this.mirror.model.metaRewinds,
      })
      const closedCause = this.mirror.model.closedCause
      if (closedCause) {
        participant._onLeft(departureCause(closedCause))
        return participant
      }
      this.localParticipants.set(id, participant)
      this.mirror.model.applyJoin({ id, meta, joinedAt, metaSeq: 0, identity: null })
      participant._holdOnPage()
      return participant
    } finally {
      for (const { envelope, rawInfo } of this.joins.settle(reqId)) this.onEnvelope(envelope, rawInfo)
    }
  }

  request(req: RoomStubRequest): Promise<unknown> {
    return request(this.end, req)
  }

  /** `from` is a claim: the server checks it against this stub's members and stamps `fromMeta` itself. */
  async publishText(from: string, data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    const publish: RoomDataPublish = { __r: 'data', from, data }
    if (retain) publish.retain = true
    return roomResultValue<ChannelPublishAck>(this.end._publishUnreported(publish))
  }

  async publishFrame(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    return roomResultValue<ChannelPublishAck>(this.end._publishBinaryUnreported(framed, stream, lossy))
  }

  /** A local participant completed its voluntary leave. */
  dropParticipant(id: string): void {
    this.localParticipants.delete(id)
    this.mirror.model.applyLeave(id, { type: 'left' }) // the relayed event is absorbed
  }

  get isConnected(): boolean {
    return this.end.isConnected
  }

  heldConnectionChanged(id: string, wasConnected: boolean | null): void {
    this.mirror.model.heldConnectionChanged(id, wasConnected)
  }

  /** Apply an accepted member meta (event or own write's ack) and mirror it into a local participant. */
  acceptParticipantMeta(id: string, accepted: MemberMetaUpdate): void {
    this.mirror.model.applyParticipantMeta(id, accepted)
    this.localParticipants.get(id)?._acceptMeta(accepted)
  }

  reportCallbackError(error: unknown): void {
    reportClientCallbackError(error)
  }

  /** Every leave the model applies ends the member's local participant: kicked (with the kick's reason), left through
   *  another handle, or missing from a roster. */
  private memberLeft(id: string, cause: LeaveCause): void {
    const local = this.localParticipants.get(id)
    if (!local) return
    this.localParticipants.delete(id)
    local._onLeft(cause)
  }

  /** Room-wide text wants ride the Broadcast subscription, which attaches again before the channel's `onOpen`.
   *  Declarations are replayed channel messages, so the server keeps them across reconnects: each carries only what
   *  changed, the room-wide text wants and those of `member`, whose listeners changed, or the binary want that did. */
  onListenersChanged(member: string | null, binary: BinaryWantChange | null): void {
    if (this.mirror.model.closed) {
      this.end._setSubscribed('text', false) // a closed room takes no declaration
      return
    }
    const text = this.state.textWants()
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
    if (memberText || this.state.wantsAnnounce !== this.declaredAnnounce) {
      this.declaredAnnounce = this.state.wantsAnnounce
      this.notify({ __r: 'sub-text', announce: this.state.wantsAnnounce, members: textChanges })
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

  /** Plain DM fires local listeners; ack DM returns their reply through the symmetric `dm-reply`/`dm-ack` path. */
  private deliverDm(participant: ClientRoomParticipant, msg: InboxMessage): void {
    if (msg.ackId === undefined) {
      participant._deliverMessage(msg)
      return
    }
    sendDmReply(this.end, msg.ackId, participant._deliverMessageAck(msg))
  }

  private onEnvelope(envelope: unknown, rawInfo: ChannelPublishInfo): void {
    assert(hasRoomTag(envelope))
    const event = envelope as ClientRoomEvent
    if (this.joins.hold(event, rawInfo)) return
    switch (event.__r) {
      case 'joined':
        this.joins.named(event.reqId, event.id)
        return
      case 'roster':
      case 'rewound':
      case 'join':
      case 'leave':
      case 'update':
        this.mirror.apply(event)
        return
      case 'data':
        this.state.applyData(event, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
        return
      case 'p-meta':
        this.acceptParticipantMeta(event.id, event)
        return
      case 'closed':
        this.applyClosed(event.cause === 'removed' ? leaveCauseFromWire(event) : { type: 'closed' })
        return
      case 'announce':
        this.state.applyAnnounce(event.data, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
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
          const sender = this.mirror.senderOf(event as RoomDmEnvelope & Partial<SenderStamp>)
          msg.fromMeta = sender.meta
          msg.fromIdentity = sender.identity
        }
        this.deliverDm(local, msg)
        return
      }
    }
  }

  private onBinaryFrame(framed: Uint8Array, rawInfo: ChannelPublishInfo): void {
    const frame = decodeBinaryFrame(framed)
    assert(frame)
    if (frame.lossy) this.end._confirmSoon()
    this.state.applyBinary(frame, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
  }

  private acceptOwnMeta(): void {
    for (const [id, local] of this.localParticipants) {
      const accepted = this.mirror.model.acceptedMeta(id)
      if (accepted !== null) local._acceptMeta(accepted)
    }
  }

  private applyClosed(cause: LeaveCause): void {
    if (!this.mirror.model.applyClosed(cause)) return
    this.state.detachFields()
    // Local participants end after onClose, as on the server.
    for (const local of this.localParticipants.values()) local._onLeft(departureCause(cause))
    this.localParticipants.clear()
  }

  /** A closed channel's server side is gone with its wants. */
  private notify(notice: RoomStubRequest): void {
    if (!this.end.isClosed) void this.end.send(notice, { ack: false }).catch(() => {})
  }
}

/** A join awaiting its ack: the member the server admitted, once `joined` names it, and the events naming that
 *  member. */
type PendingJoin = { id: string | null; held: Array<{ envelope: unknown; rawInfo: ChannelPublishInfo }> }

/** The joins awaiting their ack, by request, and by the member `joined` named. The server relays to a member from its
 *  admission, before the ack registers it on the page, so the events naming it wait for its ack. */
class PendingJoins {
  private readonly byRequest = new Map<number, PendingJoin>()
  private readonly byMember = new Map<string, PendingJoin>()
  private requests = 0

  /** A join request goes out: the id its `joined` event and its ack carry. */
  start(): number {
    const reqId = this.requests++
    this.byRequest.set(reqId, { id: null, held: [] })
    return reqId
  }

  /** `joined` named the member the request `reqId` admitted. */
  named(reqId: number, id: string): void {
    const join = this.byRequest.get(reqId)
    if (join === undefined) return
    join.id = id
    this.byMember.set(id, join)
  }

  /** Whether the event names a member whose join awaits its ack, which then holds it. */
  hold(event: ClientRoomEvent, rawInfo: ChannelPublishInfo): boolean {
    const member = heldMemberOf(event)
    const join = member === null ? undefined : this.byMember.get(member)
    if (join === undefined) return false
    join.held.push({ envelope: event, rawInfo })
    return true
  }

  /** The join `reqId` settled: the events it held, in order. */
  settle(reqId: number): PendingJoin['held'] {
    const join = this.byRequest.get(reqId)
    assert(join)
    this.byRequest.delete(reqId)
    if (join.id !== null) this.byMember.delete(join.id)
    return join.held
  }
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
