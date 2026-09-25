export { ServerRoom, ServerLocalParticipant }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shield-key.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { markHandled } from '../../../utils/markHandled.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import type { ChannelPublishAck } from '../../channel.js'
import { ROOM_DM_ACK_TIMEOUT_MS, ROOM_HORIZON_MS } from '../constants.js'
import { getRoomBackend } from '../../backend/install.js'
import type { CommitAccepted, LaneId } from '../../backend/room/contract.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../../shared-ws.js'
import {
  encodeBinaryFrame,
  decodeBinaryFrame,
  emptyTrackWants,
  laneTrack,
  publicTrack,
  mergeTrackWants,
  binaryWantsCovers,
  wantsAnyBinary,
  type BinaryFrame,
  type BinaryWants,
  type TrackWants,
} from '../binary.js'
import { DM_FAILURE, participantGoneError, participantLeftError, roomClosedError, roomFailureError } from '../errors.js'
import {
  assertKnownOptions,
  leaveCauseFromWire,
  leaveCauseToWire,
  mergeAttributes,
  normalizeJoinOptions,
  ownMetaArgument,
  ownMessage,
  ownMetadata,
  recipientId,
  senderOf,
} from '../model.js'
import {
  hasRoomTag,
  inboxMessageFromWire,
  joinedMember,
  type MemberWants,
  type MemberSnapshot,
  type RoomConfigRecord,
  type RoomSnapshotMetadata,
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
import { ParticipantBase } from '../participant.js'
import { RoomStubChannel } from './stub.js'
import type { RoomRequest } from './requests.js'
import { LocalHolder, binaryLaneKey, type LaneHolder, type WantsChange } from './replay.js'
import { TailHold } from './tail.js'
import { RoomSubscriptions, type HolderWants } from './subscriptions.js'
import {
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
import { reportRoomError } from './errors.js'
import { reportServerChannelError } from '../../server/channel.js'
import { createMember, evictMember, readMembersById, updateMemberRecord } from './membership.js'
import { memberCellKey } from './cells.js'
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

const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')

/** Lanes carry only Room's own encodings. */
function decodeLaneEnvelope(serialized: string): { __r: string } {
  const envelope: unknown = parse(serialized)
  assert(hasRoomTag(envelope))
  return envelope
}
/** The owner that wrote the event flags a hidden member; room-level events always reach clients. */
function relayedAheadKey(event: Extract<RoomCtrlEnvelope, { __r: 'join' | 'p-meta' }>): string {
  return event.__r === 'join' ? 'join' : String(event.seq)
}

function hiddenMemberOf(event: RoomCtrlEnvelope): string | null {
  if (event.__r === 'join' || event.__r === 'leave' || event.__r === 'p-meta' || event.__r === 'track')
    return event.hidden === true ? event.id : null
  return null
}

type Admission = { id: string; meta: ParticipantMeta; identity: string | null; joinedAt: number; hidden: boolean }

/** One instance's view of a room. Each serialization opens a fresh stub; every event, the origin's echo included, applies idempotently. */
class ServerRoom extends RoomStateView implements Room {
  readonly [SERVER_ROOM_BRAND] = true
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }

  /** Every authority check and member write carries this incarnation, rejecting stale handles after recreate. */
  readonly _inc: string
  /** `Room.get({ tail: true })`'s hold until a stub attaches and takes it. */
  private _tail: TailHold | null = null
  /** In-flight `send(…, { ack: true })`s by `ackId`; `to` lets a leave or close fail the ones it strands. */
  private readonly _pendingDmAcks = new Map<string, { to: string; settle: (reply: DmReply) => void }>()
  private _guards: RoomGuards | null = null
  /** @internal */ readonly _state: RoomState
  private readonly _local: LocalHolder
  private readonly _stubs = new Set<RoomStubChannel>()
  /** Stubs whose first roster read failed: the next successful refresh sends them one. */
  private readonly _rosterOwed = new Set<RoomStubChannel>()
  /** Per member, this instance's own events its clients already got ('join', or a meta revision): their echo relays
   *  nothing. */
  private readonly _relayedAhead = new Map<string, Set<string>>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()
  /** Members whose inbox is establishing before their cell commits; the heartbeat leaves them alone. */
  private readonly _pendingAdmissions = new Set<string>()

  /** (member, track) pairs this instance announced, so only a track's first frame pays for the announcement. */
  private readonly _announcedTracks = new Map<string, Set<string>>()
  private readonly _publishTurns = new Map<string, Promise<void>>()
  /** Binary demand across instances (`onDemand`). */
  private readonly _demand: RoomDemand
  private readonly _subs: RoomSubscriptions

  constructor(roomId: string, config: RoomConfigRecord, seed: { members: MemberSnapshot[] } | { count: number }) {
    super()
    this._inc = config.inc
    this._state = new RoomState({
      roomId,
      meta: config.meta,
      seed,
      updateStamp: { at: config.at, by: config.by },
      onListenersChanged: () => this._onHolderWantsChanged(this._local, this._local.refreshWants()),
      onCallbackError: reportServerChannelError,
      onLeave: (id, cause, hidden) => this._onLeave(id, cause, hidden),
    })
    this._state._owner = this
    this._local = new LocalHolder(this._state, (member) => this._suppress(member))
    this._demand = new RoomDemand(
      (event) => void publishCtrl(roomId, config.inc, { __r: 'want', ...event }).catch(reportRoomError),
      (id) => this._holderOf(id) !== undefined,
      (member, track, wanted) => this._deliverDemand(member, track, wanted),
    )
    this._subs = new RoomSubscriptions(this, this._demand)
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
  }

  /** @internal See `Room.guard()`. One declaration per instance keeps the grant declarative. */
  _setGuards(guards: RoomGuards): void {
    assertUsage(
      this._guards === null,
      'Room.guard() was already called for this room instance: declare all guards in one call',
    )
    this._guards = guards
  }

  async join(options?: JoinOptions): Promise<LocalParticipant> {
    const { meta, selfDelivery, identity, hidden } = normalizeJoinOptions(options)
    const admission = { id: crypto.randomUUID(), meta, identity, joinedAt: Date.now(), hidden }
    const participant = new ServerLocalParticipant(this, admission.id, meta, selfDelivery, identity)
    await this._admit(admission, () => this._localParticipants.set(admission.id, participant))
    return participant
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    await this._subs.ensureRoster()
    return options?.hidden ? this._state.listHidden() : this._state.listVisible()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this._subs.ensureRoster()
    return this._state.getRemote(id)
  }

  snapshot(): RoomSnapshotView {
    // Snapshot consumers want the member view, so load it (need-driven, single-flight); the arrival lands as an onChange, and the next snapshot() is complete.
    if (!this._state.rosterKnown) void this._subs.ensureRoster().catch(reportRoomError)
    return this._state.snapshot()
  }

  /** Both join paths: the room is open, the guard passes, the holder takes the member, then the member commits. */
  private async _admit(admission: Admission, hold: () => void): Promise<void> {
    await this._assertOpen()
    const { id, meta, identity, joinedAt, hidden } = admission
    const onBeforeJoin = this._guards?.onBeforeJoin
    if (!hidden && onBeforeJoin) await onBeforeJoin({ id, meta, identity })
    hold()
    await this._commitAdmission(admission)
    if (hidden) return // a hidden participant has no post-join hook
    const onAfterJoin = this._guards?.onAfterJoin
    if (onAfterJoin) await runAfterHook(() => onAfterJoin({ id, meta, identity }, { joinedAt }))
  }

  /** The member's inbox is ready before its record is durable, and the join is announced after, so no DM or event is lost. */
  private async _commitAdmission(admission: Admission): Promise<void> {
    const { id, meta, identity, joinedAt, hidden } = admission
    this._pendingAdmissions.add(id)
    this._subs.replan()
    try {
      await this._inboxReady(id)
      await createMember(this.id, this._inc, id, {
        meta,
        joinedAt,
        seenAt: joinedAt,
        metaSeq: 0,
        ...(identity === null ? {} : { identity }),
        ...(hidden ? { hidden: true } : {}),
      })
      this._assertAdmitted(id)
      this._pendingAdmissions.delete(id)
      this._state.applyJoin({ id, meta, joinedAt, metaSeq: 0, identity, ...(hidden ? { hidden: true } : {}) })
      const join = {
        __r: 'join',
        id,
        meta,
        joinedAt,
        ...(identity === null ? {} : { identity }),
        ...(hidden ? { hidden: true } : {}),
      } as const
      this._relayOwn(join)
      // A member removed meanwhile gets no join after its leave.
      await publishCtrl(this.id, this._inc, join, { requiredCellKeys: [memberCellKey(id)] })
    } catch (error) {
      // A member write that rejected may still have committed, its reply lost; evicting an absent member only reads.
      await evictMember(this.id, this._inc, id, identity, { type: 'left' }).catch(reportRoomError)
      this._abandonAdmission(id)
      throw error
    }
  }
  /** The member's inbox delivers (within the horizon) before its join is visible, and the admission still holds. */
  private async _inboxReady(id: string): Promise<void> {
    const inbox = this._subs.inboxOf(id)
    if (inbox !== undefined) await withinRoomHorizon(inbox.ready, ROOM_HORIZON_MS)
    this._assertAdmitted(id)
  }
  /** A closed room or a departed member drops the admission's inbox. */
  private _assertAdmitted(id: string): void {
    if (this._subs.inboxOf(id) === undefined)
      throw this._state.closed ? roomClosedError(this.id) : participantLeftError()
  }
  private _abandonAdmission(id: string): void {
    this._pendingAdmissions.delete(id)
    this._applyLeave(id, { type: 'left' })
  }

  /** @internal */
  async _removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this._state.closed) return // close() already removed everyone
    const identity = this._state.getRemote(id)?.identity ?? null
    await evictMember(this.id, this._inc, id, identity, cause)
    this._applyLeave(id, cause)
  }

  /** @internal The member's holder is gone and nothing will retry: ownership ends even if eviction fails, leaving a record whose lease expires. */
  async _removeDepartedMember(id: string): Promise<void> {
    const cause = { type: 'disconnected' } as const
    try {
      await this._removeMember(id, cause)
    } catch (error) {
      this._applyLeave(id, cause)
      throw error
    }
  }

  /** @internal Full replace (`setMeta`). */
  async _setMemberMeta(id: string, meta: ParticipantMeta): Promise<AcceptedMeta> {
    const owned = ownMetaArgument(meta, 'setMeta() meta')
    return await this._writeMemberMeta(id, () => owned)
  }

  /** @internal Per-key merge (`setAttributes`); an `undefined` value deletes the key. */
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
    this._localParticipants.get(id)?._acceptMeta({ meta, seq })
    const accepted = { __r: 'p-meta', id, meta, seq, ...(hidden ? { hidden: true } : {}) } as const
    this._relayOwn(accepted)
    await publishCtrl(this.id, this._inc, accepted)
    return { meta, seq }
  }

  async _publishText(from: string, data: unknown, retain = false): Promise<ChannelPublishAck> {
    const { sender, commit } = await this._inCallOrder(from, async (sent) => {
      const sender = await this._admitPublish(from, data)
      const envelope: RoomDataEnvelope = {
        __r: 'data',
        from,
        fromMeta: sender.meta,
        ...(sender.identity === null ? {} : { fromIdentity: sender.identity }),
        data,
      }
      const record = encodeRoomRecord(envelope)
      const opts = { retain, requiredCellKeys: [memberCellKey(from)] }
      const committing = commitRoomLaneOrThrow(this.id, this._inc, SEMANTIC_LANE, record, opts)
      sent()
      return { sender, commit: await committing }
    })
    return this._finishPublish(sender, data, commit)
  }

  /** `frame` is `framed` decoded; receivers trust its sender id, which every caller checked is the publisher's. */
  async _publishBinaryFrame(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    const { from } = frame
    const lane = { kind: 'binary', member: from, track: laneTrack(frame.track) } as const
    const { sender, commit } = await this._inCallOrder(binaryLaneKey(from, lane.track), async (sent) => {
      const sender = await this._admitPublish(from, frame.payload)
      if (frame.track !== null) await this._ensureTrackAnnounced(from, frame.track)
      const opts = { retain: frame.retain, requiredCellKeys: [memberCellKey(from)] }
      const committing = commitRoomLaneOrThrow(this.id, this._inc, lane, framed, opts)
      sent()
      return { sender, commit: await committing }
    })
    return await this._finishPublish(sender, frame.payload, commit)
  }

  /** A member's publishes on one lane leave in call order: each starts once the one before it was sent to the backend
   *  (its guard ran and its commit went out) or failed, never waiting on that commit's answer, which the backend
   *  keeps in sending order. */
  private async _inCallOrder<T>(key: string, publish: (sent: () => void) => Promise<T>): Promise<T> {
    const previous = this._publishTurns.get(key)
    const turn = createDeferred()
    this._publishTurns.set(key, turn.promise)
    try {
      await previous
      return await publish(turn.resolve)
    } finally {
      turn.resolve()
      if (this._publishTurns.get(key) === turn.promise) this._publishTurns.delete(key)
    }
  }

  private async _admitPublish(from: string, payload: unknown): Promise<Sender> {
    if (this._state.closed) throw roomClosedError(this.id)
    const sender = this._memberSender(from)
    const onBeforePublish = this._guards?.onBeforePublish
    if (onBeforePublish) await onBeforePublish(sender, payload)
    return sender
  }

  private async _finishPublish(sender: Sender, payload: unknown, commit: CommitAccepted): Promise<ChannelPublishAck> {
    const receipt = {
      seq: commit.seq,
      timestamp: commit.timestamp,
      ...(commit.receivers === undefined ? {} : { receivers: commit.receivers }),
    }
    const onAfterPublish = this._guards?.onAfterPublish
    if (onAfterPublish) await runAfterHook(() => onAfterPublish(sender, payload, receipt))
    return { key: this.id, ...receipt }
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
    const known = this._state.getRemote(from) ?? this._localParticipants.get(from)
    return senderOf(from, known?.meta ?? {}, known?.identity ?? null)
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
      // The recipient replying/leaving/overflowing settles this promptly; this bounds the one case none of those cover: a recipient that joined but never listens and never leaves.
      timer = unrefTimer(
        setTimeout(() => {
          if (this._pendingDmAcks.delete(ackId)) settle(DM_FAILURE.timeout)
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
    if (remote) return senderOf(id, remote.meta, remote.identity)
    const [member] = await readMembersById(this.id, this._inc, [id])
    return member === undefined ? null : senderOf(id, member.meta, member.identity ?? null)
  }
  /** @internal */
  async _readOpenConfig(): Promise<RoomConfigRecord | null> {
    return openConfig(await getRoomBackend().readHead(this.id), this._inc)
  }
  private async _assertOpen(): Promise<void> {
    if (this._state.closed || (await this._readOpenConfig()) === null) throw roomClosedError(this.id)
  }
  /** @internal */
  _onCtrlMessage(serialized: string, rawInfo: WirePublishInfo): void {
    const event = decodeLaneEnvelope(serialized) as RoomCtrlEnvelope
    if (event.__r === 'want') {
      this._demand.applyWant(event) // between instances only, never relayed to clients
      return
    }
    const wasClosed = this._state.closed
    this._applyCtrl(event)
    // A leave reaches clients from `_onLeave`, like every leave this view applies.
    if (event.__r !== 'leave' && !this._takeRelayedAhead(event))
      this._relayControl(event, () => encodePublishText(serialized, rawInfo))
    if (this._state.closed && !wasClosed) this._teardown()
  }

  /** This instance's own join or meta change reaches its clients at once, not through its echo, which a lost frame can
   *  drop with no reconcile seeing drift here. */
  private _relayOwn(event: Extract<RoomCtrlEnvelope, { __r: 'join' | 'p-meta' }>): void {
    let ahead = this._relayedAhead.get(event.id)
    if (!ahead) this._relayedAhead.set(event.id, (ahead = new Set()))
    ahead.add(relayedAheadKey(event))
    this._relayApplied(event)
  }
  private _takeRelayedAhead(event: RoomCtrlEnvelope): boolean {
    if (event.__r !== 'join' && event.__r !== 'p-meta') return false
    return this._relayedAhead.get(event.id)?.delete(relayedAheadKey(event)) ?? false
  }
  /** An event this view applied, relayed as is rather than as its lane frame. */
  private _relayApplied(event: RoomCtrlEnvelope): void {
    this._relayControl(event, () => encodePublishText(stringify(event), { seq: 0, timestamp: Date.now() }))
  }

  private _relayControl(event: RoomCtrlEnvelope, wireText: () => string): void {
    if (this._stubs.size === 0) return
    const text = wireText()
    const hiddenMember = hiddenMemberOf(event)
    for (const stub of this._stubs) stub._relayControl(text, hiddenMember)
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
  }

  private _relayMemberData(serialized: string, event: RoomDataEnvelope, rawInfo: WirePublishInfo): void {
    if (this._stubs.size === 0) {
      this._tail?.push({ serialized, ord: rawInfo, from: event.from })
      return
    }
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) stub._relayText(serialized, wireText, event.from, rawInfo)
  }
  /** @internal */
  _onTextData(serialized: string, rawInfo: WirePublishInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDataEnvelope | Extract<RoomEnvelope, { __r: 'announce' }>
    if (envelope.__r === 'announce') return this._applyAnnouncement(envelope, serialized, rawInfo)
    this._applyMemberData(envelope, rawInfo)
    this._relayMemberData(serialized, envelope, rawInfo)
  }
  /** @internal */
  _onBinary(framed: Uint8Array, rawInfo: WirePublishInfo): void {
    const unframed = decodeBinaryFrame(framed)
    assert(unframed)
    this._local.relayBinary(unframed, rawInfo)
    if (this._stubs.size > 0) {
      const wireData = encodePublishBinary(framed, rawInfo)
      const track = laneTrack(unframed.track)
      for (const stub of this._stubs) stub._relayBinary(wireData, unframed.from, track, rawInfo)
    }
  }
  /** @internal A DM for a member this instance owns, routed to its holder: a server participant or its client stub. */
  _onDm(serialized: string, rawInfo: WirePublishInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDmEnvelope | RoomDmAckEnvelope
    // A reply to one of our own `send(…, { ack: true })`s, riding our inbox back home.
    if (envelope.__r === 'dm-ack') return this._resolveDmAck(envelope)
    const holder = this._holderOf(envelope.to)
    // A client that joined through a stub gets the DM relayed (its `ackId` rides along) and answers with `dm-reply`.
    if (holder instanceof RoomStubChannel) return holder._relayDm(encodePublishText(serialized, rawInfo), envelope)
    if (holder === undefined) return
    const msg = inboxMessageFromWire(envelope)
    const { ackId } = envelope
    if (ackId === undefined) return holder._deliverMessage(msg)
    // A server participant, or one a client holds (its forwarder answers): the reply goes to the sender's inbox.
    void holder
      ._deliverMessageAck(msg)
      .then((reply) => this._publishDmAck(envelope.from, ackId, reply))
      .catch(reportRoomError)
  }
  private _applyCtrl(event: RoomCtrlEnvelope): void {
    switch (event.__r) {
      case 'join':
        this._state.applyJoin(joinedMember(event))
        this._subs.replan() // a new member means a new per-member key candidate
        return
      case 'track':
        this._state.applyTrack(event.id, event.track)
        this._subs.replan() // all-track subscribers need the new (member, track) key
        return
      case 'leave':
        this._applyLeave(event.id, leaveCauseFromWire(event))
        return
      case 'p-meta': {
        this._state.applyParticipantMeta(event.id, event.meta, event.seq)
        this._localParticipants.get(event.id)?._acceptMeta(event)
        return
      }
      case 'update':
        this._state.applyRoomUpdate(event.meta, event.at, event.by)
        return
      case 'closed':
        this._state.applyClosed()
    }
  }
  private _applyLeave(id: string, cause: LeaveCause): void {
    this._state.applyLeave(id, cause)
  }
  /** Every leave the state applies, event or reconcile, runs the member's cleanup. */
  private _onLeave(id: string, cause: LeaveCause | undefined, hidden: boolean | null): void {
    this._announcedTracks.delete(id)
    this._relayedAhead.delete(id)
    this._rejectDmAcks(DM_FAILURE.left, id) // strand no waiter on a gone member
    const local = this._localParticipants.get(id)
    if (local) {
      this._localParticipants.delete(id)
      // A live-heartbeating owner can't be reaped (heartbeats outpace the TTL by 4x), so a vanished record with no observed event means the member was removed.
      local._onLeft(cause ?? { type: 'removed' })
    }
    // Every leave of a member this view knew reaches its clients here, once: from an event, from this instance's own
    // removal, whose echo a lost frame can drop, or from a roster read, where no event means a removal.
    if (hidden !== null)
      this._relayApplied({
        __r: 'leave',
        id,
        ...leaveCauseToWire(cause ?? { type: 'removed' }),
        ...(hidden ? { hidden: true } : {}),
      })
    for (const stub of this._stubs) stub._forgetMember(id)
    this._local.forgetMember(id)
    this._demand.forgetMember(id)
    this._subs.replan()
  }
  /** The room closed. Runs once, after the `closed` event has been applied and relayed. */
  private _teardown(): void {
    this._rejectDmAcks(DM_FAILURE.roomClosed) // no recipient will reply now
    this._teardownTail()
    for (const local of this._localParticipants.values()) local._onLeft({ type: 'closed' })
    this._localParticipants.clear()
    for (const stub of this._stubs) void stub.close().catch(() => {})
    this._subs.replan()
  }

  /** @internal A newer config reaching this view through the authority never reached its clients as an event. */
  _applyAuthorityConfig(config: RoomConfigRecord): void {
    if (!this._state.applyRoomUpdate(config.meta, config.at, config.by)) return
    const update = { __r: 'update', meta: config.meta, at: config.at, by: config.by } as const
    for (const stub of this._stubs) stub._relayEvent(update)
  }
  /** @internal */
  _applyAuthorityRoster(members: MemberSnapshot[], departing: ReadonlySet<string>): boolean {
    return this._state.reconcileRoster(members, departing)
  }
  /** @internal The authority says the room closed; the lane that would have carried `closed` failed. */
  _closeFromAuthority(): void {
    if (this._state.closed) return
    this._state.applyClosed()
    for (const stub of this._stubs) stub._relayEvent({ __r: 'closed' })
    this._teardown()
  }
  private _suppress(from: string): boolean {
    return this._localParticipants.get(from)?.selfDelivery === false
  }
  /** Resolves once the tail receives, so it holds everything committed after it returns. */
  async _startTail(): Promise<void> {
    this._tail = new TailHold(() => this._teardownTail())
    this._subs.replan() // bring up text ingestion before any stub exists
    await withinRoomHorizon(this._subs.semanticReady, ROOM_HORIZON_MS)
  }
  private _teardownTail(): void {
    if (this._tail === null) return // already handed off to a stub
    this._tail.end()
    this._tail = null
    this._subs.replan() // drop the text ingestion nothing is consuming
  }
  /** @internal A client's view of this room. It attaches before the snapshot, so every later event relays and every earlier one is in the snapshot. */
  _openStub(options: ConstructorParameters<typeof RoomStubChannel>[1]): {
    stub: RoomStubChannel
    metadata: RoomSnapshotMetadata
  } {
    const stub = new RoomStubChannel(this, options)
    this._attachStub(stub)
    return {
      stub,
      metadata: {
        channelId: stub.id,
        roomId: this.id,
        meta: this.meta,
        closed: this.isClosed,
        stamp: this._state.updateStamp,
        // Scalars only: the roster streams over the stub once its peer attaches.
        count: this.count,
      },
    }
  }
  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    if (this._tail !== null) {
      stub._beginTail(this._tail.take(), () => this._subs.replan())
      this._tail = null
    }
    stub.onClose(() => this._detachStub(stub))
    this._subs.replan()
  }
  _onStubAttached(stub: RoomStubChannel, reattach: boolean): void {
    this._sendRosterTo(stub)
    if (reattach && this._stubs.has(stub))
      stub._relayEvent({ __r: 'update', meta: this.meta, ...this._state.updateStamp })
  }
  private _sendRosterTo(stub: RoomStubChannel): void {
    void this._subs
      .ensureRoster()
      .then(() => {
        if (this._stubs.has(stub) && !this._state.closed) stub._relayRoster(this._state.snapshotMembers())
      })
      .catch((error) => {
        reportRoomError(error)
        if (!this._stubs.has(stub) || this._state.closed) return
        stub._relayEvent({ __r: 'roster-error' })
        this._rosterOwed.add(stub)
      })
  }
  private _detachStub(stub: RoomStubChannel): void {
    this._stubs.delete(stub)
    this._rosterOwed.delete(stub)
    stub._endTail()
    for (const id of stub._heldMembers()) {
      if (this._pendingAdmissions.has(id)) continue // the admission rolls itself back
      void this._removeDepartedMember(id).catch(reportRoomError)
    }
    this._subs.replan()
  }
  async _joinStubMember(stub: RoomStubChannel, req: Extract<RoomRequest, { __r: 'req-join' }>) {
    const admission = {
      id: crypto.randomUUID(),
      meta: ownMetadata(req.meta),
      identity: null,
      joinedAt: Date.now(),
      hidden: false,
    }
    await this._admit(admission, () => stub._addMember(admission.id, req.selfDelivery))
    return { id: admission.id, joinedAt: admission.joinedAt }
  }
  async _replayRetainedText(holder: LaneHolder, previous: MemberWants): Promise<void> {
    if (previous.all) return
    // Read retained only after subscription readiness: a racing commit is then retained or live, never lost in the gap.
    await withinRoomHorizon(this._subs.semanticReady, ROOM_HORIZON_MS)
    const stored = await getRoomBackend().readRetained(this.id, this._inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const event = parse(serialized) as RoomDataEnvelope
    if (previous.members.includes(event.from) || !holder._wantsTextFrom(event.from)) return
    // Replay the stored order as-is; the holder admits it only if it is newer than what it has.
    holder._emitRetainedText(serialized, event, { seq: stored.seq, timestamp: stored.timestamp })
  }
  async _replayRetainedBinary(holder: LaneHolder, prevWants: BinaryWants): Promise<void> {
    if (!wantsAnyBinary(holder._binaryWants)) return
    const roomWide = holder._binaryWants.everyMember
    if (roomWide.all || roomWide.tracks.length > 0) await this._subs.ensureRoster()
    // Binary uses the same readiness handoff and stored receipt; its holder dedupes the live/retained race per lane.
    await this._subs.binaryReady()
    const backend = getRoomBackend()
    // A binary lane is keyed by its frames' sender and track, so only newly wanted lanes are read.
    const lanes = (await backend.listRetained(this.id, this._inc)).filter(
      (lane): lane is Extract<LaneId, { kind: 'binary' }> =>
        lane.kind === 'binary' &&
        !binaryWantsCovers(prevWants, lane.member, lane.track) &&
        holder._wantsBinary(lane.member, lane.track),
    )
    for (const lane of lanes) {
      const stored = await backend.readRetained(this.id, this._inc, lane)
      if (stored === null) continue
      const framed = stored.payload
      const frame = decodeBinaryFrame(framed)
      assert(frame)
      holder._emitRetainedBinary(framed, frame, { seq: stored.seq, timestamp: stored.timestamp })
    }
  }

  /** @internal A holder's wants changed: replan, then replay the retained frames it now wants. */
  _onHolderWantsChanged(holder: LaneHolder, previous: WantsChange): void {
    this._subs.replan()
    if (previous.text) void this._replayRetainedText(holder, previous.text).catch(reportRoomError)
    if (previous.binary) void this._replayRetainedBinary(holder, previous.binary).catch(reportRoomError)
  }

  /** @internal */
  _holderWants(): HolderWants {
    const holders: LaneHolder[] = [this._local, ...this._stubs]
    let everyMember = emptyTrackWants()
    const members: Record<string, TrackWants> = Object.create(null)
    for (const { _binaryWants: wants } of holders) {
      everyMember = mergeTrackWants(everyMember, wants.everyMember)
      for (const [id, trackWants] of Object.entries(wants.members))
        members[id] = mergeTrackWants(members[id] ?? emptyTrackWants(), trackWants)
    }
    return {
      observed: this._stubs.size > 0 || this._localParticipants.size > 0 || this._state.listenerCount > 0,
      text: this._textWants(holders),
      announce: holders.some((holder) => holder._wantsAnnounce),
      binary: { everyMember, members },
    }
  }

  /** @internal */
  _wantsBinary(member: string, track: string): boolean {
    return this._local._wantsBinary(member, track) || [...this._stubs].some((stub) => stub._wantsBinary(member, track))
  }

  private _textWants(holders: LaneHolder[]): { all: boolean; members: Set<string> } {
    if (this._tail !== null) return { all: true, members: new Set() } // pre-attach tail: ingest everything now
    const members = new Set<string>()
    for (const holder of holders) {
      const demand = holder._textDemand()
      if (demand === 'all') return { all: true, members: new Set() }
      for (const id of demand) members.add(id)
    }
    return { all: false, members }
  }

  /** @internal */
  _ownedMembers(): { all: string[]; renewable: string[] } {
    const all = [...this._localParticipants.keys()]
    for (const stub of this._stubs) all.push(...stub._heldMembers())
    return { all, renewable: all.filter((id) => !this._pendingAdmissions.has(id)) }
  }

  /** @internal */
  _onRosterRefreshed(drifted: boolean): void {
    const recipients = drifted ? [...this._stubs] : [...this._rosterOwed]
    this._rosterOwed.clear()
    if (recipients.length === 0) return
    const members = this._state.snapshotMembers()
    for (const stub of recipients) stub._relayRoster(members)
  }

  private _holderOf(id: string): ServerLocalParticipant | RoomStubChannel | undefined {
    return this._localParticipants.get(id) ?? [...this._stubs].find((stub) => stub._holds(id))
  }
  private _deliverDemand(member: string, track: string, wanted: boolean): void {
    const trackOut = publicTrack(track)
    const holder = this._holderOf(member)
    if (holder instanceof ServerLocalParticipant) holder._onDemand(trackOut, wanted)
    else holder?._relayEvent({ __r: 'demand', member, track: trackOut, wanted })
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
  // Messaging is often fire-and-forget: a usage error throws, and a failure is a rejection left handled.
  publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    assertKnownOptions(options, ['coalesce', 'retain'], 'publish()')
    // Server publish has no uplink to coalesce, but retain semantics remain identical.
    return markHandled(this._publishText(ownMessage(data), options?.retain))
  }
  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    const framed = encodeBinaryFrame(this.id, data, options)
    // Local holders get exactly what remote ones decode, not the caller's objects.
    const frame = decodeBinaryFrame(framed)
    assert(frame !== null)
    return markHandled(this._publishFrame(frame, framed))
  }
  private async _publishText(data: unknown, retain: boolean | undefined): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishText(this.id, data, retain)
  }
  /** @internal */
  async _publishFrame(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishBinaryFrame(frame, framed)
  }
  /** @internal Each meta write this room accepts for the participant, with its revision. */
  _onAcceptedMeta(callback: (accepted: AcceptedMeta) => void): () => void {
    const state = this._room._state
    const unlisten = state.getRemote(this.id)?.onUpdate(() => {
      const accepted = state.acceptedMeta(this.id)
      if (accepted) callback(accepted)
    })
    return unlisten ?? (() => {})
  }
  /** @internal The client holding this participant went away without leaving. */
  _releaseHolder(): Promise<void> {
    return this._room._removeDepartedMember(this.id)
  }
  send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    assertKnownOptions(options, ['ack'], 'send()')
    return markHandled(this._sendDm(recipientId(to), ownMessage(data), options?.ack === true))
  }
  private async _sendDm(to: string, data: unknown, ack: boolean): Promise<unknown> {
    this._assertActive()
    return await this._room._sendDm(this.id, to, data, ack)
  }
  async setMeta(meta: ParticipantMeta): Promise<void> {
    await this._setMeta(meta)
  }
  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    await this._setAttributes(attrs)
  }
  /** @internal The accepted write, which a client holder mirrors in revision order. */
  async _setMeta(meta: ParticipantMeta): Promise<AcceptedMeta> {
    this._assertActive()
    return await this._room._setMemberMeta(this.id, meta)
  }
  /** @internal */
  async _setAttributes(attrs: ParticipantMeta): Promise<AcceptedMeta> {
    this._assertActive()
    return await this._room._mergeMemberMeta(this.id, attrs)
  }
  async leave(): Promise<void> {
    if (this._left) return
    await this._room._removeMember(this.id, { type: 'left' })
  }
  protected override _resolveSender(id: string): Sender | null {
    return this._room._state.getRemote(id) // sync view read (delivery must not wait on I/O)
  }

  protected _reportError(err: unknown): void {
    reportServerChannelError(err)
  }
}

async function runAfterHook(hook: () => unknown): Promise<void> {
  try {
    await hook()
  } catch (error) {
    reportServerChannelError(error)
  }
}
