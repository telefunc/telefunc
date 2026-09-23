export { ServerRoom, ServerLocalParticipant }

import { parse } from '@brillout/json-serializer/parse'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shield-key.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { makePublishInfo, type ChannelPublishAck } from '../../channel.js'
import {
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS,
} from '../constants.js'
import { getRoomBackend } from '../../backend/install.js'
import type { LaneId } from '../../backend/room/contract.js'
import type { BackendSubscription } from '../../backend/subscription.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../../shared-ws.js'
import {
  frameWithMemberId,
  unframeMemberId,
  DEFAULT_TRACK,
  emptyTrackWants,
  mergeTrackWants,
  binaryWantsCovers,
  wantsAnyBinary,
  type BinaryWants,
  type TrackWants,
} from '../binary.js'
import { DM_PARTICIPANT_LEFT, RoomError, participantGoneError, roomClosedError, roomFailureError } from '../errors.js'
import { leaveCauseFromWire, mergeAttributes, normalizeJoinOptions, ownMetaArgument, recipientId } from '../model.js'
import {
  hasRoomTag,
  type MemberWants,
  type MemberSnapshot,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
  type RoomDmEnvelope,
  type RoomDmAckEnvelope,
  type DmReply,
  type AcceptedMeta,
  type RoomEnvelope,
} from '../protocol.js'
import { RoomState, RoomStateView } from '../state.js'
import { RoomDemand } from '../demand.js'
import { ParticipantBase, type InboxMessage } from '../participant.js'
import type { RoomStubChannel } from './stub.js'
import type { RoomRequest } from './requests.js'
import { LocalHolder, type LaneHolder } from './replay.js'
import { TailHold } from './tail.js'
import { LaneSubscription } from './lane-subscription.js'
import {
  CONTROL_LANE,
  SEMANTIC_LANE,
  commitRoomLane,
  decodeRoomText,
  encodeRoomRecord,
  publishCtrl,
  staleCommitError,
  commitRoomLaneOrThrow,
  openConfig,
  withinRoomHorizon,
} from './lanes.js'
import { reportCallbackError, reportRoomError } from './errors.js'
import {
  createMember,
  evictMember,
  memberCellKey,
  readAllMembers,
  readMembersById,
  renewMemberLease,
  updateMemberRecord,
} from './membership.js'
import type {
  BinaryPublishOptions,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  RoomSendReceipt,
  RoomAckReceipt,
  RoomSnapshotView,
  Sender,
} from '../types.js'
import type { Room, RoomGuards } from './statics.js'
assertIsNotBrowser()

const ROOM_REPLAN_LIMIT = 5
const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')

/** Lanes carry only Room's own encodings. */
function decodeLaneEnvelope(serialized: string): { __r: string } {
  const envelope: unknown = parse(serialized)
  assert(hasRoomTag(envelope))
  return envelope
}
/** The owner that wrote the event flags a hidden member; room-level events always reach clients. */
function hiddenMemberOf(event: RoomCtrlEnvelope): string | null {
  if (event.__r === 'join' || event.__r === 'leave' || event.__r === 'p-meta' || event.__r === 'track')
    return event.hidden === true ? event.id : null
  return null
}

type SubscriptionPlan = {
  backend: ReturnType<typeof getRoomBackend>
  open: boolean
  observed: boolean
  becomesObserved: boolean
  wantSemantic: boolean
  wantAnyBinary: boolean
  needsRoster: boolean
  binaryPairs: Array<[string, string]>
}
type Admission = { id: string; meta: ParticipantMeta; identity: string | null; joinedAt: number; hidden: boolean }

/**
 * A Server Room is not a channel; each serialization attaches a fresh wire-unique stub.
 * Repeated serialization preserves the domain `room.id` while channel IDs stay unique.
 * The source applies mutations locally and commits them through backend lanes.
 * Every observer, including the source echo, applies the subscribed payload.
 * Idempotent application makes that overlap converge safely.
 */
class ServerRoom extends RoomStateView implements Room {
  readonly [SERVER_ROOM_BRAND] = true
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }

  /** Every authority check and member write carries this incarnation, rejecting stale handles after recreate. */
  readonly _inc: string
  /** Tail mode ingests from `Room.get` until stub attach or close, closing the pre-serialization gap; the attaching stub takes the hold. */
  private _tail: TailHold | null = null
  /** In-flight `send(…, { ack: true })`s awaiting the recipient's reply, keyed by `ackId`. `to` is the recipient, so a leave/close can fail the ones it strands. Empty at steady state. */
  private readonly _pendingDmAcks = new Map<string, { to: string; settle: (reply: DmReply) => void }>()
  private _guards: RoomGuards | null = null
  /** @internal */ readonly _state: RoomState
  private readonly _local: LocalHolder
  private readonly _stubs = new Set<RoomStubChannel>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()
  /** Members registered with their holder so their inbox can establish, but not yet durable. They own inbox routes; heartbeat must not renew/reap them until the member cell commits. */
  private readonly _pendingAdmissions = new Set<string>()

  private readonly _ctrlSub = this._newLaneSubscription()
  private readonly _textSub = this._newLaneSubscription()
  /** Keyed by (member, track) and by member. */
  private readonly _binarySubs = new Map<string, LaneSubscription>()
  private readonly _inboxSubs = new Map<string, LaneSubscription>()
  /** (member, track) pairs this instance has already announced — first publish pays the KV append + ctrl event, every further frame is a Set lookup. */
  private readonly _announcedTracks = new Map<string, Set<string>>()
  /** Cross-node binary-demand aggregation (`onDemand`) — constructed once `roomId` and the ownership/delivery callbacks are available (see the constructor). */
  private readonly _demand: RoomDemand
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _heartbeatBusy = false
  private _pendingRefresh: Promise<void> | null = null
  private readonly _recoveringSubscriptions = new Set<LaneSubscription>()
  private _controlSeq = 0

  constructor(roomId: string, config: RoomConfigRecord, seed: { members: MemberSnapshot[] } | { count: number }) {
    super()
    this._inc = config.inc
    this._state = new RoomState({
      roomId,
      meta: config.meta,
      seed,
      updateStamp: { at: config.at, by: config.by },
      onListenersChanged: () => this._onLocalListenersChanged(),
      onCallbackError: reportCallbackError,
    })
    this._state._owner = this
    this._local = new LocalHolder(this._state, (member) => this._suppress(member))
    this._demand = new RoomDemand(
      (event) => void publishCtrl(roomId, config.inc, { __r: 'want', ...event }).catch(reportRoomError),
      (id) => this._holderOf(id) !== undefined,
      (member, track, wanted) => this._deliverDemand(member, track, wanted),
    )
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
  }

  /** @internal — see `Room.guard()`. One declaration per instance keeps the grant declarative. */
  _setGuards(guards: RoomGuards): void {
    assertUsage(
      this._guards === null,
      'Room.guard() was already called for this room instance — declare all guards in one call',
    )
    this._guards = guards
  }

  async join(options?: JoinOptions): Promise<LocalParticipant> {
    const { meta, selfDelivery, identity, hidden } = normalizeJoinOptions(options)
    await this._assertOpen()
    const admission = { id: crypto.randomUUID(), meta, identity, joinedAt: Date.now(), hidden }
    await this._guardAdmission(admission)
    const participant = new ServerLocalParticipant(this, admission.id, meta, selfDelivery, identity)
    this._localParticipants.set(admission.id, participant)
    await this._commitAdmission(admission)
    return participant
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    await this._ensureRoster()
    return options?.hidden ? this._state.listHidden() : this._state.listRemotes()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this._ensureRoster()
    return this._state.getRemote(id)
  }

  snapshot(): RoomSnapshotView {
    // Snapshot consumers want the member view — load it (need-driven, single-flight); the arrival lands as an onChange, and the next snapshot() is complete.
    if (!this._state.rosterKnown) void this._ensureRoster().catch(reportRoomError)
    return this._state.snapshot()
  }

  private async _guardAdmission({ id, meta, identity, hidden }: Admission): Promise<void> {
    const onBeforeJoin = this._guards?.onBeforeJoin
    if (!hidden && onBeforeJoin) await onBeforeJoin({ id, meta, identity })
  }

  /** Both join paths register ownership before inbox/heartbeat sync and join announcement. */
  private async _commitAdmission({ id, meta, identity, joinedAt, hidden }: Admission): Promise<void> {
    this._pendingAdmissions.add(id)
    this._syncSubs()
    let created = false
    try {
      const inbox = this._admittedInbox(id)
      await withinRoomHorizon(inbox.ready, ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS)
      this._admittedInbox(id)
      await createMember(this.id, this._inc, id, {
        meta,
        joinedAt,
        seenAt: joinedAt,
        metaSeq: 0,
        ...(identity === null ? {} : { identity }),
        ...(hidden ? { hidden: true } : {}),
      })
      created = true
      this._admittedInbox(id)
      this._pendingAdmissions.delete(id)
      this._state.applyJoin(id, meta, joinedAt, identity, hidden)
      await publishCtrl(this.id, this._inc, {
        __r: 'join',
        id,
        meta,
        joinedAt,
        ...(identity === null ? {} : { identity }),
        ...(hidden ? { hidden: true } : {}),
      })
    } catch (error) {
      this._pendingAdmissions.delete(id)
      if (created) {
        try {
          await evictMember(this.id, this._inc, id, identity, { type: 'left' })
        } catch (rollbackError) {
          reportRoomError(rollbackError)
        }
      }
      this._applyLeave(id, { type: 'left' })
      throw error
    }
    if (hidden) return // announced above; a hidden participant has no post-join hook
    const onAfterJoin = this._guards?.onAfterJoin
    if (onAfterJoin) await runAfterHook(() => onAfterJoin({ id, meta, identity }, { joinedAt }))
  }

  /** The member's inbox slot exists exactly while the room is open and its holder owns the member. */
  private _admittedInbox(id: string): LaneSubscription {
    const inbox = this._inboxSubs.get(id)
    if (inbox === undefined)
      throw new RoomError(this._state.closed ? `Room is closed: ${this.id}` : 'Participant left the room')
    return inbox
  }

  /** @internal */
  async _removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this._state.closed) return // close() already removed everyone
    const identity = this._state.getRemote(id)?.identity ?? null
    await evictMember(this.id, this._inc, id, identity, cause)
    this._applyLeave(id, cause)
  }

  /** @internal — the member's holder is gone and nothing will retry: ownership ends even if eviction fails, leaving a record whose lease expires. */
  async _removeDepartedMember(id: string): Promise<void> {
    const cause = { type: 'disconnected' } as const
    try {
      await this._removeMember(id, cause)
    } catch (error) {
      this._applyLeave(id, cause)
      throw error
    }
  }

  /** @internal — full replace (`setMeta`). */
  async _setMemberMeta(id: string, meta: ParticipantMeta): Promise<AcceptedMeta> {
    const owned = ownMetaArgument(meta, 'setMeta() meta')
    return await this._writeMemberMeta(id, () => owned)
  }

  /** @internal — per-key merge (`setAttributes`); an `undefined` value deletes the key. */
  async _mergeMemberMeta(id: string, attrs: ParticipantMeta): Promise<AcceptedMeta> {
    const owned = ownMetaArgument(attrs, 'setAttributes() attributes')
    return await this._writeMemberMeta(id, (current) => mergeAttributes(current, owned))
  }

  private async _writeMemberMeta(
    id: string,
    computeMeta: (current: ParticipantMeta) => ParticipantMeta,
  ): Promise<AcceptedMeta> {
    const { meta, seq, hidden } = await updateMemberRecord(this.id, this._inc, id, (record) => {
      const meta = computeMeta(record.meta)
      const seq = record.metaSeq + 1
      return { value: { meta, seq, hidden: record.hidden === true }, next: { ...record, meta, metaSeq: seq } }
    })
    this._state.applyParticipantMeta(id, meta, seq)
    this._syncLocalMemberMeta(id)
    await publishCtrl(this.id, this._inc, { __r: 'p-meta', id, meta, seq, ...(hidden ? { hidden: true } : {}) })
    return { meta, seq }
  }

  async _publishText(from: string, data: unknown, retain = false): Promise<ChannelPublishAck> {
    const sender = await this._admitPublish(from, data)
    const envelope: RoomDataEnvelope = {
      __r: 'data',
      from,
      fromMeta: sender.meta,
      ...(sender.identity === null ? {} : { fromIdentity: sender.identity }),
      data,
    }
    const commit = await commitRoomLaneOrThrow(this.id, this._inc, SEMANTIC_LANE, encodeRoomRecord(envelope), {
      retain,
      requiredCellKeys: [memberCellKey(from)],
    })
    return this._finishPublish(sender, data, commit)
  }

  async _publishBinaryFramed(from: string, framed: Uint8Array): Promise<ChannelPublishAck> {
    const frame = unframeMemberId(framed)
    // Receivers trust the frame's own sender id, so it must be the publisher's.
    if (frame?.from !== from) throw new RoomError('Malformed binary frame')
    const sender = await this._admitPublish(from, frame.payload)
    if (frame.track !== null) await this._ensureTrackAnnounced(from, frame.track)
    const commit = await commitRoomLaneOrThrow(
      this.id,
      this._inc,
      { kind: 'binary', member: from, track: frame.track ?? DEFAULT_TRACK },
      framed,
      { retain: frame.retain, requiredCellKeys: [memberCellKey(from)] },
    )
    return await this._finishPublish(sender, frame.payload, commit)
  }

  private async _admitPublish(from: string, payload: unknown): Promise<Sender> {
    if (this._state.closed) throw roomClosedError(this.id)
    const sender = this._memberSender(from)
    const onBeforePublish = this._guards?.onBeforePublish
    if (onBeforePublish) await onBeforePublish(sender, payload)
    return sender
  }

  private async _finishPublish(
    sender: Sender,
    payload: unknown,
    info: { seq: number; timestamp: number; receivers?: number; meta?: Record<string, unknown> },
  ): Promise<ChannelPublishAck> {
    const ack = Object.assign(makePublishInfo(this.id, info.seq, info.timestamp), {
      meta: info.meta,
      ...(info.receivers === undefined ? {} : { receivers: info.receivers }),
    })
    const onAfterPublish = this._guards?.onAfterPublish
    if (onAfterPublish) {
      await runAfterHook(() =>
        onAfterPublish(sender, payload, {
          seq: ack.seq,
          timestamp: ack.timestamp,
          ...(ack.receivers === undefined ? {} : { receivers: ack.receivers }),
        }),
      )
    }
    return ack
  }

  /** A new track is durably recorded and announced before its first frame; later frames use the idempotent cache. */
  private async _ensureTrackAnnounced(from: string, track: string): Promise<void> {
    let announced = this._announcedTracks.get(from)
    if (announced?.has(track)) return
    if (!announced) {
      announced = new Set()
      this._announcedTracks.set(from, announced)
    }
    const hidden = await updateMemberRecord(this.id, this._inc, from, (record) => {
      const tracks = record.tracks ?? []
      // Already recorded by an attempt whose announcement failed: announce it now.
      if (tracks.includes(track)) return { value: record.hidden === true }
      return { value: record.hidden === true, next: { ...record, tracks: [...tracks, track] } }
    })
    await publishCtrl(this.id, this._inc, { __r: 'track', id: from, track, ...(hidden ? { hidden: true } : {}) })
    this._state.applyTrack(from, track)
    announced.add(track)
  }

  private _memberSender(from: string): Sender {
    const remote = this._state.getRemote(from)
    if (remote) return { id: from, meta: remote.meta, identity: remote.identity }
    const local = this._localParticipants.get(from)
    if (local) return { id: from, meta: local.meta, identity: local.identity }
    return { id: from, meta: {}, identity: null }
  }

  async _sendDm(from: string, to: string, data: unknown, ack: boolean): Promise<RoomSendReceipt | RoomAckReceipt> {
    if (!ack) return await this._publishDm(from, to, data)
    const { receipt, reply } = await this._sendDmAck(from, to, data)
    if (!reply.ok) throw roomFailureError(reply)
    return { ...receipt, response: reply.result }
  }
  private async _sendDmAck(
    from: string,
    to: string,
    data: unknown,
  ): Promise<{ receipt: RoomSendReceipt; reply: DmReply }> {
    const ackId = crypto.randomUUID()
    let timer: ReturnType<typeof setTimeout> | undefined
    const reply = new Promise<DmReply>((settle) => {
      this._pendingDmAcks.set(ackId, { to, settle })
      // The recipient replying/leaving/overflowing settles this promptly; this bounds the one case none of those cover — a recipient that joined but never listens and never leaves.
      timer = unrefTimer(
        setTimeout(() => {
          if (this._pendingDmAcks.delete(ackId))
            settle({ ok: false, err: 'send({ ack: true }) timed out — the recipient never handled the message' })
        }, ROOM_DM_ACK_TIMEOUT_MS),
      )
    })
    let receipt: RoomSendReceipt
    try {
      receipt = await this._publishDm(from, to, data, ackId)
    } catch (err) {
      this._pendingDmAcks.delete(ackId)
      clearTimeout(timer)
      throw err
    }
    const settled = await reply
    clearTimeout(timer)
    return { receipt, reply: settled }
  }

  async _publishDm(from: string, to: string, data: unknown, ackId?: string): Promise<RoomSendReceipt> {
    if (this._state.closed) throw roomClosedError(this.id)
    const target = await this._resolveMember(to)
    if (!target) throw participantGoneError(to)
    const sender = this._memberSender(from)
    const onBeforeSend = this._guards?.onBeforeSend
    if (onBeforeSend) await onBeforeSend(sender, target, data)
    const envelope: RoomDmEnvelope = {
      __r: 'dm',
      to,
      from,
      fromMeta: sender.meta,
      ...(sender.identity === null ? {} : { fromIdentity: sender.identity }),
      data,
      ...(ackId ? { ackId } : {}),
    }
    const receipt = await commitRoomLaneOrThrow(
      this.id,
      this._inc,
      { kind: 'inbox', member: to },
      encodeRoomRecord(envelope),
      { requiredCellKeys: [memberCellKey(from), memberCellKey(to)] },
    )
    const info: RoomSendReceipt = { seq: receipt.seq, timestamp: receipt.timestamp }
    const onAfterSend = this._guards?.onAfterSend
    if (onAfterSend) await runAfterHook(() => onAfterSend(sender, target, data, info))
    return info
  }

  async _publishDmAck(to: string, ackId: string, reply: DmReply): Promise<void> {
    const envelope: RoomDmAckEnvelope = { __r: 'dm-ack', to, ackId, ...reply }
    const committed = await commitRoomLane(
      this.id,
      this._inc,
      { kind: 'inbox', member: to },
      encodeRoomRecord(envelope),
      { requiredCellKeys: [memberCellKey(to)] },
    )
    // A sender that left has no ack left to settle; only a closed room is an error.
    if ('stale' in committed && committed.stale === 'incarnation') throw staleCommitError(this.id, committed)
  }

  private _resolveDmAck(envelope: RoomDmAckEnvelope): void {
    const pending = this._pendingDmAcks.get(envelope.ackId)
    if (!pending) return
    this._pendingDmAcks.delete(envelope.ackId)
    pending.settle(envelope)
  }

  private _rejectDmAcks(reply: DmReply, to?: string): void {
    for (const [ackId, pending] of this._pendingDmAcks) {
      if (to !== undefined && pending.to !== to) continue
      this._pendingDmAcks.delete(ackId)
      pending.settle(reply)
    }
  }

  private async _resolveMember(id: string): Promise<Sender | null> {
    const remote = this._state.getRemote(id)
    if (remote) return remote
    const [member] = await readMembersById(this.id, this._inc, [id])
    return member === undefined ? null : { id, meta: member.meta, identity: member.identity ?? null }
  }
  private async _readOpenConfig(): Promise<RoomConfigRecord | null> {
    return openConfig(await getRoomBackend().readHead(this.id), this._inc)
  }
  private async _assertOpen(): Promise<void> {
    if (this._state.closed || (await this._readOpenConfig()) === null) throw roomClosedError(this.id)
  }
  private _onCtrlMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const event = decodeLaneEnvelope(serialized) as RoomCtrlEnvelope
    const previousSeq = this._controlSeq
    if (rawInfo.seq <= previousSeq) return
    this._controlSeq = rawInfo.seq
    if (previousSeq !== 0 && rawInfo.seq !== previousSeq + 1) {
      void this._reconcileAuthority().catch(reportRoomError)
    }
    if (event.__r === 'want') {
      this._demand.applyWant(event) // demand gossip — node-to-node only, never relayed to clients
      return
    }
    const wasClosed = this._state.closed
    const hiddenMember = hiddenMemberOf(event)
    this._applyCtrl(event)
    if (this._stubs.size > 0) {
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) stub._relayControl(wireText, hiddenMember)
    }
    if (this._state.closed && !wasClosed) this._teardown()
  }

  private _applyAnnouncement(
    announce: Extract<RoomEnvelope, { __r: 'announce' }>,
    serialized: string,
    rawInfo: WirePublishInfo,
  ): void {
    this._local.relayAnnouncement(announce.data, rawInfo)
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) stub._relayAnnouncement(wireText, rawInfo)
  }

  private _applyMemberData(event: RoomDataEnvelope, rawInfo: WirePublishInfo): void {
    this._local.relayText(event, rawInfo)
    this._healUnknownSender(event.from)
  }

  private _relayMemberData(serialized: string, event: RoomDataEnvelope, rawInfo: WirePublishInfo): void {
    if (this._stubs.size === 0) {
      this._tail?.push({ serialized, ord: rawInfo, from: event.from })
      return
    }
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) stub._relayText(serialized, wireText, event.from, rawInfo)
  }
  private _onTextData(serialized: string, rawInfo: WirePublishInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDataEnvelope | Extract<RoomEnvelope, { __r: 'announce' }>
    if (envelope.__r === 'announce') return this._applyAnnouncement(envelope, serialized, rawInfo)
    this._applyMemberData(envelope, rawInfo)
    this._relayMemberData(serialized, envelope, rawInfo)
  }
  private _onBinary(framed: Uint8Array, rawInfo: WirePublishInfo): void {
    const unframed = unframeMemberId(framed)
    assert(unframed)
    this._local.relayBinary(unframed, rawInfo)
    this._healUnknownSender(unframed.from)
    if (this._stubs.size > 0) {
      const wireData = encodePublishBinary(framed, rawInfo)
      const track = unframed.track ?? DEFAULT_TRACK
      for (const stub of this._stubs) stub._relayBinary(wireData, unframed.from, track, rawInfo)
    }
  }
  /** A message on the inbox key of a member this instance owns — route it to the holder: a server-side participant's listeners, or the one client stub the member joined through. */
  private _onDm(serialized: string, rawInfo: WirePublishInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDmEnvelope | RoomDmAckEnvelope
    // A reply to one of our own `send(…, { ack: true })`s, riding our inbox back home.
    if (envelope.__r === 'dm-ack') return this._resolveDmAck(envelope)
    const dm = envelope
    const holder = this._holderOf(dm.to)
    // A client held through a room stub gets the DM relayed (its `ackId` rides along) and replies with `dm-reply`.
    if (holder === undefined || !(holder instanceof ServerLocalParticipant))
      return holder?._relayDm(encodePublishText(serialized, rawInfo), dm)
    const msg: InboxMessage = {
      from: dm.from,
      fromMeta: dm.fromMeta,
      fromIdentity: dm.fromIdentity ?? null,
      data: dm.data,
      ...(dm.ackId ? { ackId: dm.ackId } : {}),
    }
    // A server-side participant, or one a client holds (its forwarder replies). Either way, for an ack DM we route the handler's reply back to the sender's inbox.
    if (dm.ackId) {
      void holder
        ._deliverMessageAck(msg)
        .then((reply) => this._publishDmAck(dm.from, dm.ackId!, reply))
        .catch(reportRoomError)
    } else {
      holder._deliverMessage(msg)
    }
  }
  private _applyCtrl(event: RoomCtrlEnvelope): void {
    switch (event.__r) {
      case 'join':
        this._state.applyJoin(event.id, event.meta, event.joinedAt, event.identity ?? null, event.hidden)
        this._syncSubs() // a new member means a new per-member key candidate
        return
      case 'track':
        this._state.applyTrack(event.id, event.track)
        this._syncSubs() // all-track subscribers need the new (member, track) key
        return
      case 'leave':
        this._applyLeave(event.id, leaveCauseFromWire(event))
        return
      case 'p-meta': {
        this._state.applyParticipantMeta(event.id, event.meta, event.seq)
        this._syncLocalMemberMeta(event.id)
        return
      }
      case 'update':
        this._state.applyRoomUpdate(event.meta, event.at, event.by)
        return
      case 'closed':
        this._state.applyClosed()
    }
  }
  private _applyLeave(id: string, cause?: LeaveCause): void {
    this._state.applyLeave(id, cause)
    this._announcedTracks.delete(id)
    this._rejectDmAcks(DM_PARTICIPANT_LEFT, id) // strand no waiter on a gone member
    const local = this._localParticipants.get(id)
    if (local) {
      this._localParticipants.delete(id)
      // A live-heartbeating owner can't be reaped (heartbeats outpace the TTL by 4x), so a vanished record with no observed event means the member was removed.
      local._onLeft(cause ?? { type: 'removed' })
    }
    for (const stub of this._stubs) stub._forgetMember(id)
    this._local.forgetMember(id)
    this._demand.forgetMember(id)
    this._syncSubs()
  }
  /** Mirror only the sequence-accepted projection into the local facade. */
  private _syncLocalMemberMeta(id: string): void {
    const local = this._localParticipants.get(id)
    const accepted = this._state.getRemote(id)
    if (local && accepted) local._meta = accepted.meta
  }
  /** The room closed — runs once, after the `closed` event has been applied and relayed. */
  private _teardown(): void {
    this._rejectDmAcks({ ok: false, err: 'Room is closed' }) // no recipient will reply now
    this._teardownTail()
    for (const local of this._localParticipants.values()) local._onLeft({ type: 'closed' })
    this._localParticipants.clear()
    for (const stub of this._stubs) void stub.close().catch(() => {})
    this._syncSubs()
  }

  /** Recover a still-wanted terminal lane inside Room's one policy horizon. */
  private _onTerminalSubscription(slot: LaneSubscription, failure?: unknown): void {
    if (failure !== undefined) reportRoomError(failure)
    if (this._recoveringSubscriptions.has(slot)) return
    this._recoveringSubscriptions.add(slot)
    void this._recoverTerminalSubscription(slot)
      .catch(reportRoomError)
      .finally(() => this._recoveringSubscriptions.delete(slot))
  }
  private async _recoverTerminalSubscription(slot: LaneSubscription): Promise<void> {
    const deadline = Date.now() + ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS
    for (let attempt = 0; attempt <= ROOM_REPLAN_LIMIT && slot.wanted && Date.now() < deadline; attempt++) {
      const outcome = await this._attemptRecovery(slot, deadline).catch((error: unknown) => reportRoomError(error))
      if (outcome === 'closed') return this._closeFromAuthority()
      // Catch up on what the outage dropped; the lane itself is healthy.
      if (outcome === 'ready') return await this._reconcileAuthority()
    }
    if (!slot.wanted) return
    reportRoomError(new Error(`Room subscription recovery exhausted: ${this.id}`))
    slot.markLost()
  }
  /** One replacement attempt, within its share of the horizon. */
  private async _attemptRecovery(slot: LaneSubscription, deadline: number): Promise<'closed' | 'ready'> {
    if ((await withinRoomHorizon(this._readOpenConfig(), deadline - Date.now())) === null) return 'closed'
    slot.retry()
    const attemptMs = ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS / (ROOM_REPLAN_LIMIT + 1)
    await withinRoomHorizon(slot.attemptReady, Math.min(attemptMs, deadline - Date.now()))
    return 'ready'
  }
  /** The authority says the room closed; the lane that would have carried `closed` failed, so relay it here. */
  private _closeFromAuthority(): void {
    if (this._state.closed) return
    this._state.applyClosed()
    for (const stub of this._stubs) stub._relayEvent({ __r: 'closed' })
    this._teardown()
  }
  private async _reconcileAuthority(): Promise<void> {
    if (this._state.closed) return
    const config = await this._readOpenConfig()
    if (config === null) return this._closeFromAuthority()
    this._state.applyRoomUpdate(config.meta, config.at, config.by)
    await this._refreshMembers()
  }
  private _suppress(from: string): boolean {
    return this._localParticipants.get(from)?.selfDelivery === false
  }
  _startTail(): void {
    this._tail = new TailHold(() => this._teardownTail())
    this._syncSubs() // bring up text ingestion before any stub exists
  }
  private _teardownTail(): void {
    if (this._tail === null) return // already handed off to a stub
    this._tail.end()
    this._tail = null
    this._syncSubs() // drop the text ingestion nothing is consuming
  }
  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    if (this._tail !== null) {
      stub._beginTail(this._tail.take(), () => this._syncSubs())
      this._tail = null
    }
    stub.onOpen(() => {
      void this._ensureRoster()
        .then(() => {
          if (this._stubs.has(stub) && !this._state.closed)
            stub._relayEvent({
              __r: 'roster',
              members: this._state.snapshotMembers().filter((member) => !member.hidden),
            })
        })
        .catch((error) => {
          reportRoomError(error)
          if (this._stubs.has(stub) && !this._state.closed) stub._relayEvent({ __r: 'roster-error' })
        })
    })
    stub.onClose(() => {
      this._stubs.delete(stub)
      stub._endTail() // clear any pending tail hold/timer so a closed stub leaves nothing behind
      for (const id of stub._heldMembers()) {
        if (this._pendingAdmissions.has(id)) continue // the admission rolls itself back
        void this._removeDepartedMember(id).catch(reportRoomError)
      }
      this._syncSubs()
    })
    this._syncSubs()
  }
  async _handleStubRequest(stub: RoomStubChannel, req: RoomRequest): Promise<unknown> {
    switch (req.__r) {
      case 'req-join':
        return await this._joinStubMember(stub, req)
      case 'req-leave':
        await this._removeMember(stub._requireMember(req.id), { type: 'left' })
        stub._forgetMember(req.id)
        return undefined
      case 'req-set-meta':
        return await this._setMemberMeta(stub._requireMember(req.id), req.meta)
      case 'req-set-attrs':
        return await this._mergeMemberMeta(stub._requireMember(req.id), req.attrs)
      case 'req-dm':
        return await this._sendDm(stub._requireMember(req.id), req.to, req.data, req.ack === true)
    }
  }
  private async _joinStubMember(stub: RoomStubChannel, req: Extract<RoomRequest, { __r: 'req-join' }>) {
    await this._assertOpen()
    const admission = { id: crypto.randomUUID(), meta: req.meta, identity: null, joinedAt: Date.now(), hidden: false }
    await this._guardAdmission(admission)
    stub._addMember(admission.id, req.selfDelivery)
    await this._commitAdmission(admission)
    return { id: admission.id, joinedAt: admission.joinedAt }
  }
  async _replayRetainedText(holder: LaneHolder, prevWantedFrom: (member: string) => boolean): Promise<void> {
    // Read retained only after subscription readiness: a racing commit is then retained or live, never lost in the gap.
    await withinRoomHorizon(this._textSub.ready, ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS)
    const stored = await getRoomBackend().readRetained(this.id, this._inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const event = parse(serialized) as RoomDataEnvelope
    if (prevWantedFrom(event.from) || !holder._wantsTextFrom(event.from)) return
    // Replay the stored order as-is; the holder dedupes a same-or-newer live winner.
    holder._emitRetainedText(serialized, event, { seq: stored.seq, timestamp: stored.timestamp })
  }
  async _replayRetainedBinary(holder: LaneHolder, prevWants: BinaryWants): Promise<void> {
    if (!wantsAnyBinary(holder._binaryWants)) return
    const roomWide = holder._binaryWants.everyMember
    if (roomWide.all || roomWide.tracks.length > 0) await this._ensureRoster()
    this._syncSubs()
    // Binary uses the same readiness handoff and stored receipt; its holder dedupes the live/retained race per lane.
    await this._binaryReady()
    const backend = getRoomBackend()
    const lanes = (await backend.listRetained(this.id, this._inc)).filter(
      (lane): lane is Extract<LaneId, { kind: 'binary' }> => lane.kind === 'binary',
    )
    for (const lane of lanes) {
      const stored = await backend.readRetained(this.id, this._inc, lane)
      if (stored === null) continue
      const framed = stored.payload
      const frame = unframeMemberId(framed)
      if (!frame) continue
      const track = frame.track ?? DEFAULT_TRACK
      if (binaryWantsCovers(prevWants, frame.from, track) || !holder._wantsBinary(frame.from, track)) continue
      holder._emitRetainedBinary(framed, frame, { seq: stored.seq, timestamp: stored.timestamp })
    }
  }

  private _onLocalListenersChanged(): void {
    const { prevText, prevBinary } = this._local.refreshWants()
    this._syncSubs()
    if (prevText)
      void this._replayRetainedText(this._local, (member) => prevText.all || prevText.members.includes(member)).catch(
        reportRoomError,
      )
    if (prevBinary) void this._replayRetainedBinary(this._local, prevBinary).catch(reportRoomError)
  }

  _syncSubs(): void {
    const plan = this.deriveSubscriptionPlan(this._state, this._stubs, this._localParticipants)
    this._syncControlAndSemantic(plan)
    this._syncRosterAndBinary(plan)
    this._syncInbox(plan)
    this._syncHeartbeat()
  }

  private deriveSubscriptionPlan(
    state: RoomState,
    stubs: ReadonlySet<RoomStubChannel>,
    locals: ReadonlyMap<string, ServerLocalParticipant>,
  ): SubscriptionPlan {
    const backend = getRoomBackend()
    const open = !state.closed
    const observed = stubs.size > 0 || locals.size > 0 || state.listenerCount > 0
    const textWants = this._aggregateTextWants()
    const wantAnyText = open && (textWants.all || textWants.members.size > 0)
    const wantAnnounce = state.wantsAnnounce || [...stubs].some((stub) => stub._wantsAnnounce)
    const binaryWants = this._aggregateBinaryWants()
    const wantAnyBinary = open && wantsAnyBinary(binaryWants)
    const memberIds = open ? state.listMemberIds() : []
    const binaryPairs = open ? this._binaryPairs(binaryWants, memberIds) : []
    return {
      backend,
      open,
      observed,
      becomesObserved: open && observed && !this._ctrlSub.active,
      wantSemantic: open && (wantAnyText || wantAnnounce),
      wantAnyBinary,
      needsRoster: state.listenerCount > 0 || wantAnyBinary,
      binaryPairs,
    }
  }

  private _syncControlAndSemantic(plan: SubscriptionPlan): void {
    this._ctrlSub.sync(plan.open && plan.observed, () =>
      plan.backend.subscribeLane(this.id, this._inc, CONTROL_LANE, (payload, info) =>
        this._onCtrlMessage(decodeRoomText(payload), info),
      ),
    )
    this._textSub.sync(plan.wantSemantic, () =>
      plan.backend.subscribeLane(this.id, this._inc, SEMANTIC_LANE, (payload, info) =>
        this._onTextData(decodeRoomText(payload), info),
      ),
    )
  }

  private _syncRosterAndBinary(plan: SubscriptionPlan): void {
    const state = this._state
    if ((plan.becomesObserved && state.rosterKnown) || (plan.open && !state.rosterKnown && plan.needsRoster))
      void this._refreshMembers().catch(reportRoomError)
    this._syncKeyedSubs(this._binarySubs, plan.wantAnyBinary ? this._binaryLanes(plan.binaryPairs) : [], (lane) =>
      plan.backend.subscribeLane(this.id, this._inc, lane, (framed, info) => this._onBinary(framed, info)),
    )
    if (plan.binaryPairs.length === 0) this._demand.sync([])
    else {
      void this._binaryReady()
        .then(() => {
          const currentWants = this._aggregateBinaryWants()
          this._demand.sync(this._state.closed ? [] : this._binaryPairs(currentWants, this._state.listMemberIds()))
        })
        .catch(reportRoomError)
    }
  }

  private _syncInbox(plan: SubscriptionPlan): void {
    this._syncKeyedSubs(
      this._inboxSubs,
      plan.open
        ? this._ownedMemberIds().map((member) => ({ key: member, value: { kind: 'inbox', member } as const }))
        : [],
      (lane) =>
        plan.backend.subscribeLane(this.id, this._inc, lane, (payload, info) =>
          this._onDm(decodeRoomText(payload), info),
        ),
    )
  }
  private _aggregateBinaryWants(): BinaryWants {
    const local = this._state.binaryWants()
    let everyMember = local.everyMember
    const members = new Map<string, TrackWants>(Object.entries(local.members))
    for (const stub of this._stubs) {
      everyMember = mergeTrackWants(everyMember, stub._binaryWants.everyMember)
      for (const [id, wants] of Object.entries(stub._binaryWants.members)) {
        members.set(id, mergeTrackWants(members.get(id) ?? emptyTrackWants(), wants))
      }
    }
    return { everyMember, members: Object.fromEntries(members) }
  }
  private _binaryLanes(
    pairs: Array<[string, string]>,
  ): Array<{ key: string; value: Extract<LaneId, { kind: 'binary' }> }> {
    return pairs.map(([member, track]) => ({
      key: `${member}\u0000${track}`,
      value: { kind: 'binary', member, track },
    }))
  }
  /** Declared wants filter the room's members; a want naming anyone else takes effect on their `join`. */
  private _binaryPairs(wants: BinaryWants, memberIds: string[]): Array<[string, string]> {
    const pairs: Array<[string, string]> = []
    for (const memberId of memberIds) {
      const memberWants = wants.members[memberId]
      const eff = memberWants ? mergeTrackWants(wants.everyMember, memberWants) : wants.everyMember
      const tracks = eff.all ? [DEFAULT_TRACK, ...this._state.memberTracks(memberId)] : eff.tracks
      for (const track of tracks) pairs.push([memberId, track])
    }
    return pairs
  }
  private _holderOf(id: string): ServerLocalParticipant | RoomStubChannel | undefined {
    return this._localParticipants.get(id) ?? [...this._stubs].find((stub) => stub._holds(id))
  }
  private _deliverDemand(member: string, track: string, wanted: boolean): void {
    const trackOut = track === DEFAULT_TRACK ? null : track
    const holder = this._holderOf(member)
    if (holder instanceof ServerLocalParticipant) holder._onDemand(trackOut, wanted)
    else holder?._relayEvent({ __r: 'demand', member, track: trackOut, wanted })
  }
  private _aggregateTextWants(): { all: boolean; members: Set<string> } {
    if (this._tail !== null) return { all: true, members: new Set() } // pre-attach tail: ingest everything now
    const local: MemberWants = this._state.textWants()
    if (local.all) return { all: true, members: new Set() }
    const members = new Set(local.members)
    for (const stub of this._stubs) {
      const demand = stub._textDemand()
      if (demand === 'all') return { all: true, members: new Set() }
      for (const id of demand) members.add(id)
    }
    return { all: false, members }
  }
  private _syncKeyedSubs<T>(
    subs: Map<string, LaneSubscription>,
    wantedEntries: Array<{ key: string; value: T }>,
    subscribe: (value: T) => BackendSubscription,
  ) {
    const wanted = new Map(wantedEntries.map(({ key, value }) => [key, value]))
    for (const [key, slot] of [...subs]) {
      if (!wanted.has(key)) {
        subs.delete(key)
        slot.stop()
      }
    }
    for (const [key, value] of wanted) {
      let slot = subs.get(key)
      if (!slot) subs.set(key, (slot = this._newLaneSubscription()))
      slot.sync(true, () => subscribe(value))
    }
  }
  private _newLaneSubscription(): LaneSubscription {
    return new LaneSubscription(
      (slot, error) => this._onTerminalSubscription(slot, error),
      () => void this._reconcileAuthority().catch(reportRoomError),
    )
  }
  private _binaryReady(): Promise<void> {
    const pending: Promise<void>[] = []
    for (const subscription of this._binarySubs.values()) pending.push(subscription.ready)
    return pending.length === 0
      ? Promise.resolve()
      : withinRoomHorizon(Promise.all(pending), ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS).then(() => undefined)
  }
  private _ensureRoster(): Promise<void> {
    if (this._pendingRefresh !== null) return this._pendingRefresh
    if (this._state.closed || (this._state.rosterKnown && this._ctrlSub.established)) return Promise.resolve()
    return this._refreshMembers()
  }
  /** Unknown-sender traffic heals an at-most-once roster drift through one single-flight snapshot. */
  private _healUnknownSender(from: string): void {
    if (!this._state.rosterKnown || this._state.getRemote(from) !== null) return
    void this._refreshMembers().catch(reportRoomError)
  }
  /** Roster refresh replans on membership-version drift and re-seeds streamed views from the committed snapshot. */
  private _refreshMembers(): Promise<void> {
    this._pendingRefresh ??= this._runMemberRefresh().finally(() => {
      this._pendingRefresh = null
    })
    return this._pendingRefresh
  }

  private async _runMemberRefresh(): Promise<void> {
    for (let attempt = 0; !this._state.closed; attempt++) {
      const version = this._state.membershipVersion
      const members = await readAllMembers(this.id, this._inc)
      if (this._commitRefreshedRoster(version, members) === 'done') return
      if (attempt === ROOM_REPLAN_LIMIT) throw new RoomError(`Room roster refresh contention: ${this.id}`)
    }
  }
  private _commitRefreshedRoster(version: number, members: MemberSnapshot[]): 'retry' | 'done' {
    if (this._state.membershipVersion !== version) return 'retry'
    const drifted = this._state.reconcileCompleteRoster(members)
    this._syncSubs()
    if (drifted) this._relayVisibleRoster()
    return 'done'
  }
  private _relayVisibleRoster(): void {
    const members = this._state.snapshotMembers().filter((member) => !member.hidden)
    for (const stub of this._stubs) stub._relayEvent({ __r: 'roster', members })
  }
  // Graceful departures use events; heartbeats refresh owner `seenAt` and reap records orphaned by hard crashes.
  private _ownedMemberIds(): string[] {
    const owned = [...this._localParticipants.keys()]
    for (const stub of this._stubs) owned.push(...stub._heldMembers())
    return owned
  }
  private _syncHeartbeat(): void {
    const want =
      !this._state.closed && (this._ctrlSub.active || this._ownedMemberIds().length > 0 || this._demand.isActive())
    if (want && !this._heartbeatTimer) {
      this._heartbeatTimer = unrefTimer(
        setInterval(() => void this._heartbeatTick().catch(reportRoomError), ROOM_HEARTBEAT_INTERVAL_MS),
      )
    } else if (!want && this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer)
      this._heartbeatTimer = null
    }
  }
  private async _heartbeatTick(): Promise<void> {
    if (this._heartbeatBusy) return // a slow backend must not pile up overlapping ticks
    this._heartbeatBusy = true
    try {
      // Renew this node's binary-demand lease on every owner and sweep any crashed reporter's demand.
      // No cell I/O — runs first so member-cell latency never delays it (the demand TTL has slack for skips).
      this._demand.heartbeat()
      let renewalFailure: { error: unknown } | null = null
      for (const id of this._ownedMemberIds().filter((id) => !this._pendingAdmissions.has(id))) {
        try {
          if (!(await renewMemberLease(this.id, this._inc, id))) this._applyLeave(id)
        } catch (error) {
          renewalFailure ??= { error }
        }
      }
      this._syncSubs() // bounded retry trigger for still-wanted terminal lanes
      await this._reconcileAuthority() // the roster read reaps crashed nodes' expired members
      if (renewalFailure) throw renewalFailure.error
    } finally {
      this._heartbeatBusy = false
    }
  }
}
const SERVER_PARTICIPANT_BRAND: unique symbol = Symbol.for('telefunc.ServerRoomParticipant')
/** Server-side `LocalParticipant`, returned by `ServerRoom.join()`. */
class ServerLocalParticipant extends ParticipantBase {
  readonly [SERVER_PARTICIPANT_BRAND] = true
  /** @internal */ readonly _room: ServerRoom
  constructor(
    serverRoom: ServerRoom,
    id: string,
    meta: ParticipantMeta,
    selfDelivery: boolean,
    identity: string | null,
  ) {
    super(id, meta, selfDelivery, identity)
    this._room = serverRoom
  }
  static isServerLocalParticipant(value: unknown): value is ServerLocalParticipant {
    return value !== null && typeof value === 'object' && SERVER_PARTICIPANT_BRAND in value
  }
  async publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    // Server publish has no uplink to coalesce, but retain semantics remain identical.
    this._assertActive()
    return await this._room._publishText(this.id, data, options?.retain)
  }
  async publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishBinaryFramed(this.id, frameWithMemberId(this.id, data, options))
  }
  _publishFramed(framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return this._room._publishBinaryFramed(this.id, framed)
  }
  async send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    this._assertActive()
    return await this._room._sendDm(this.id, recipientId(to), data, options?.ack === true)
  }
  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._setMemberMeta(this.id, meta)
  }
  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._mergeMemberMeta(this.id, attrs)
  }
  async leave(): Promise<void> {
    if (this._left) return
    await this._room._removeMember(this.id, { type: 'left' })
  }
  protected override _resolveSender(id: string): Sender | null {
    return this._room._state.getRemote(id) // sync view read — delivery must not wait on I/O
  }

  protected _reportError(err: unknown): void {
    reportCallbackError(err)
  }
}

async function runAfterHook(hook: () => unknown): Promise<void> {
  try {
    await hook()
  } catch (error) {
    reportCallbackError(error)
  }
}
