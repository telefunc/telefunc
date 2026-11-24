export { ClientRoomHandle }
export type { ClientRoomEvent, ClientRoomHandleOptions }

import { assert, assertUsage } from '../utils/assert.js'
import type { ChannelPublishAck, ChannelPublishInfo } from 'telefunc/__internal'
import { EVERY_MEMBER, EVERY_TRACK, decodeBinaryFrame, type BinaryWantChange } from '../shared/binary.js'
import { normalizeJoinOptions } from '../shared/arguments.js'
import {
  inboxMessageFromWire,
  leaveCauseFromWire,
  type InboxMessage,
  type MemberRequest,
  type RoomDemandEvent,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomJoinedEvent,
  type RoomRosterEvent,
  type RoomSnapshotMetadata,
  type RoomWantParts,
} from '../shared/protocol.js'
import { departureCause } from '../shared/RoomModel.js'
import { RoomState, type RoomStateOwner, type WantChange } from '../shared/RoomState.js'
import { RoomRemotes } from '../shared/RoomView.js'
import { ROOM_TRACK_WANTS_MAX } from '../shared/constants.js'
import { publishAck, roomResultValue } from '../shared/errors.js'
import { RoomMirror } from './RoomMirror.js'
import {
  ClientParticipant,
  reportClientCallbackError,
  request,
  sendDmReply,
  type PageLink,
  type RoomChannel,
} from './ClientParticipant.js'
import { addRoom } from './page.js'
import { toggle } from '../utils/sets.js'
import type { JoinOptions, LeaveCause, Sender } from '../types.js'

/** What a room's stub relays to its client on the text lane. */
type ClientRoomEvent = RoomEnvelope | RoomDmEnvelope | RoomRosterEvent | RoomJoinedEvent | RoomDemandEvent

type ClientRoomHandleOptions = {
  end: RoomChannel<ClientRoomEvent, RoomDataPublish>
  snapshot: RoomSnapshotMetadata
}

/** What one room a telefunction returned is to its server stub, through the room's own Broadcast channel: its frames
 *  carry events and data, its acked messages requests. The user's `Room` over it carries none of this. */
class ClientRoomHandle implements RoomStateOwner, PageLink {
  readonly state: RoomState
  /** The app's participants of the room, which the user's `Room` and the handle's participants hand out. */
  readonly remotes: RoomRemotes
  private readonly end: RoomChannel<ClientRoomEvent, RoomDataPublish>
  private readonly mirror: RoomMirror
  private readonly localParticipants = new Map<string, ClientParticipant>()
  private readonly joins = new PendingJoins()
  /** The wants turned off in this task, by `wantKey`. */
  private readonly offsDue = new Map<string, WantChange>()
  /** The members whose messages it wants, and the member and track pairs whose bytes it wants, by `wantKey`: its
   *  want's parts. */
  private readonly memberTexts = new Set<string>()
  private readonly tracks = new Map<string, [member: string | null, track: string | null]>()

  constructor({ end, snapshot }: ClientRoomHandleOptions) {
    this.end = end
    this.mirror = new RoomMirror({
      snapshot,
      onMemberLeft: (id, cause) => this.memberLeft(id, cause),
      onMemberMeta: (id, update) => this.localParticipants.get(id)?._acceptMeta(update),
    })
    this.state = new RoomState({ model: this.mirror.model, owner: this, trackWantsMax: ROOM_TRACK_WANTS_MAX })
    this.remotes = new RoomRemotes({ state: this.state, adder: 'app' })

    // Delivery handlers are local-only. What the server relays is driven by the declared wants: control always arrives,
    // text while subscribed room-wide or to its member, binary per its pairs.
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

  async join(options?: JoinOptions): Promise<ClientParticipant> {
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
      const { id, joinedAt } = await request(this.end, { __r: 'req-join', reqId, meta, selfDelivery })
      const participant = new ClientParticipant({
        link: this,
        // A page's join carries no identity: it's server-assigned (see JoinOptions.identity).
        participant: { id, meta, selfDelivery, identity: null },
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

  /** Applied to the mirror once the server performed it, as the server applies it to its model: the model hands the
   *  participant its accepted meta, as each member meta it applies, and its leave ends the participant (the relayed one
   *  is absorbed). */
  async request(from: string, req: MemberRequest): Promise<unknown> {
    if (req.__r === 'req-set-meta' || req.__r === 'req-update-meta') {
      const accepted = await request(this.end, { ...req, id: from })
      this.mirror.model.applyParticipantMeta(from, accepted)
      return accepted
    }
    const value = await request(this.end, { ...req, id: from })
    if (req.__r === 'req-leave') this.mirror.model.applyLeave(from, { type: 'left' })
    return value
  }

  /** `from` is a claim: the server checks it against this stub's members and stamps `fromMeta` itself. */
  async publishText(from: string, data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    const publish: RoomDataPublish = { __r: 'data', from, data }
    if (retain) publish.retain = true
    return publishAck(await roomResultValue(this.end._publishUnreported(publish)))
  }

  async publishFrame(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    return publishAck(await roomResultValue(this.end._publishBinaryUnreported(framed, stream, lossy)))
  }

  get isConnected(): boolean {
    return this.end.isConnected
  }

  heldConnectionChanged(id: string, wasConnected: boolean | null): void {
    this.mirror.model.heldConnectionChanged(id, wasConnected)
  }

  resolveSender(id: string): Sender | null {
    return this.remotes.get(id)
  }

  deliverPrivate(field: string, from: string, data: unknown): void {
    this.state.applyPrivate(field, from, data)
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

  /** A want turned on is declared at once, so it reaches the server before anything sent after it. One turned off is
   *  declared as the task ends, unless it is turned on again before, as a React effect that runs again does: then
   *  nothing is declared, and the server replays no retained message. */
  onWantChanged(change: WantChange): void {
    if (change.kind === 'listeners') return this.declare(change)
    const want = wantKey(change)
    if (change.kind === 'text' ? change.on : change.change.on) {
      if (!this.offsDue.delete(want)) this.declare(change)
      return
    }
    if (this.offsDue.size === 0) queueMicrotask(() => this.declareOffsDue())
    this.offsDue.set(want, change)
  }

  private declareOffsDue(): void {
    for (const change of this.offsDue.values()) this.declare(change)
    this.offsDue.clear()
  }

  /** Every want rides the broadcast's: the room-wide text as its text, the members' and the pairs' as its parts. The
   *  page declares it at each attach, and each call it makes before the server confirmed it carries it, so the server
   *  applies it before that call runs. */
  private declare(change: WantChange): void {
    if (this.mirror.model.closed) {
      this.end._setSubscribed('text', false) // a closed room takes no declaration
      return
    }
    switch (change.kind) {
      case 'listeners':
        return
      case 'text':
        if (change.member === null) return this.end._setSubscribed('text', change.on)
        toggle(this.memberTexts, change.member, change.on)
        return this.declareParts()
      case 'track': {
        const { member, track, on } = trackWantOnWire(change.change)
        if (on) this.tracks.set(wantKey(change), [member, track])
        else this.tracks.delete(wantKey(change))
        return this.declareParts()
      }
    }
  }

  private declareParts(): void {
    this.end._setWantParts((): RoomWantParts => ({ members: [...this.memberTexts], tracks: [...this.tracks.values()] }))
  }

  /** Plain DM fires local listeners; ack DM returns their reply through the symmetric `dm-reply`/`dm-ack` path. */
  private deliverDm(participant: ClientParticipant, msg: InboxMessage): void {
    if (msg.ackId === undefined) {
      participant._deliverMessage(msg)
      return
    }
    sendDmReply(this.end, msg.ackId, participant._deliverMessageAck(msg))
  }

  private onEnvelope(event: ClientRoomEvent, rawInfo: ChannelPublishInfo): void {
    if (this.joins.hold(event, rawInfo)) return
    switch (event.__r) {
      case 'joined':
        this.joins.named(event.reqId, event.id)
        return
      case 'roster':
      case 'rewound':
      case 'join':
      case 'leave':
      case 'p-meta':
      case 'update':
        this.mirror.apply(event)
        return
      case 'data':
        this.state.applyData(event, { key: this.id, seq: rawInfo.seq, timestamp: rawInfo.timestamp })
        return
      case 'closed':
        this.applyClosed(event.cause === 'removed' ? leaveCauseFromWire(event) : { type: 'closed' })
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
          const sender = this.mirror.senderOf(event)
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

  private applyClosed(cause: LeaveCause): void {
    if (!this.mirror.model.applyClosed(cause)) return
    this.state.detachFields()
    // Local participants end after onClose, as on the server.
    for (const local of this.localParticipants.values()) local._onLeft(departureCause(cause))
    this.localParticipants.clear()
  }
}

/** A join awaiting its ack: the member the server admitted, once `joined` names it, and the events naming that
 *  member. */
type PendingJoin = { id: string | null; held: Array<{ envelope: ClientRoomEvent; rawInfo: ChannelPublishInfo }> }

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

/** The one want a change turns on or off: the room's text, a member's text, or a track. */
function wantKey(change: Exclude<WantChange, { kind: 'listeners' }>): string {
  if (change.kind === 'text') return JSON.stringify(['text', change.member])
  const { member, track } = trackWantOnWire(change.change)
  return JSON.stringify(['track', member, track])
}

/** The wire names every member and every track `null`. */
function trackWantOnWire({ member, track, on }: BinaryWantChange) {
  return { member: member === EVERY_MEMBER ? null : member, track: track === EVERY_TRACK ? null : track, on }
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
