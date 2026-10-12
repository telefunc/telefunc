export { ServerRoom, ServerLocalParticipant }

import { parse } from '@brillout/json-serializer/parse'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shield-key.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { markHandled } from '../../../utils/markHandled.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import type { ChannelPublishAck } from '../../channel.js'
import { getRoomBackend } from '../../backend/install.js'
import type { LaneId } from '../../backend/room/contract.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../../shared-ws.js'
import {
  encodeBinaryFrame,
  decodeBinaryFrame,
  laneTrack,
  publicTrack,
  type BinaryFrame,
  type BinaryWantChange,
} from '../binary.js'
import { DM_FAILURE, participantLeftError, roomClosedError } from '../errors.js'
import {
  assertKnownOptions,
  mergeAttributes,
  normalizeJoinOptions,
  ownMessage,
  ownMetadata,
  recipientId,
} from '../model.js'
import {
  inboxMessageFromWire,
  joinedMember,
  leaveCauseFromWire,
  leaveCauseToWire,
  type MemberSnapshot,
  type RoomConfigRecord,
  type RoomSnapshotMetadata,
  type RoomDataEnvelope,
  type SenderStamp,
  type RoomDmEnvelope,
  type RoomDmAckEnvelope,
  type AcceptedMeta,
  type RoomEnvelope,
} from '../protocol.js'
import { RoomState, RoomStateView } from '../state.js'
import { DemandReport } from '../demand.js'
import { ParticipantBase } from '../participant.js'
import { MemberMessageTexts, RoomStubChannel, unorderedEventText } from './stub.js'
import type { RoomRequest } from './requests.js'
import { LocalHolder, type LaneHolder, type TextWantsChange } from './replay.js'
import { TailHold } from './tail.js'
import { PendingDmAcks } from './dm-acks.js'
import { RoomSubscriptions } from './subscriptions.js'
import { AuthoritySync } from './authority-sync.js'
import { RoomReplica, type PresenceEvent } from './replica.js'
import { RosterStream } from './roster-stream.js'
import { RoomWrites, runAfterHook } from './writes.js'
import {
  SEMANTIC_LANE,
  decodeRoomText,
  publishCtrl,
  openConfig,
  withinRoomHorizon,
  decodeLaneEnvelope,
} from './lanes.js'
import { reportRoomError } from './errors.js'
import { reconnectWindow, reportServerChannelError } from '../../server/channel.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'
import { createMember, evictMember, holdViews, removeView } from './membership.js'
import { memberCellKey } from './cells.js'
import type { StoredRoomMeta } from './meta.js'
import type { RoomIncarnation } from './lifecycle.js'
import type {
  BinaryPublishOptions,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  RoomSnapshotView,
  Sender,
} from '../types.js'
import type { Room, RoomGuardHooks } from './statics.js'
assertIsNotBrowser()

const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')

/** The owner that wrote the event flags a hidden member; room-level events always reach clients. */
function hiddenMemberOf(event: PresenceEvent): string | null {
  if (event.__r === 'join' || event.__r === 'leave' || event.__r === 'p-meta')
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
  /** @internal */ readonly _incarnation: RoomIncarnation
  /** `Room.get({ tail: true })`'s hold until a stub attaches and takes it. */
  private _tail: TailHold | null = null
  /** In-flight `send(…, { ack: true })`s; a leave of either end, or a close, fails the ones it strands. */
  private readonly _pendingDmAcks = new PendingDmAcks()
  private _guards: Partial<RoomGuardHooks> | null = null
  /** @internal */ readonly _state: RoomState
  private readonly _local: LocalHolder
  private readonly _stubs = new Set<RoomStubChannel>()
  /** The attached stub holding each member a client joined through. */
  private readonly _stubOf = new Map<string, RoomStubChannel>()
  /** Each stub's latest view write, from its attach or a renewal; its removal comes after it. */
  private readonly _views = new Map<RoomStubChannel, Promise<void>>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()
  /** The local participants that joined with `selfDelivery: false`. */
  private readonly _localSuppressed = new Set<string>()
  /** Members whose inbox is establishing before their cell commits; the heartbeat leaves them alone. */
  private readonly _pendingAdmissions = new Set<string>()

  /** What this handle's holders receive, told to the replica's demand (`onDemand`). */
  private readonly _report = new DemandReport(() => this._replica.demand)
  private _replicaOf: RoomReplica
  private readonly _subs: RoomSubscriptions
  /** @internal */ readonly _sync: AuthoritySync
  /** @internal */ readonly _roster: RosterStream
  /** @internal */ readonly _writes: RoomWrites

  constructor(
    roomId: string,
    { inc, hold }: RoomConfigRecord,
    stored: StoredRoomMeta,
    seed: { members: MemberSnapshot[] } | { count: number },
  ) {
    super()
    this._inc = inc
    this._incarnation = { id: roomId, inc, hold }
    this._state = new RoomState({
      owner: this,
      roomId,
      meta: stored.entries,
      seed,
      metaSeq: stored.seq,
      onMetaWaiting: () => this._sync.metaWaiting(),
      onMemberMetaGap: () => this._sync.memberMetaGap(),
      onListenersChanged: (member, binary) => {
        this._onHolderTextChanged(this._local, this._local.refreshText(member))
        if (binary !== null && this._local.applyBinary(binary)) this._onHolderBinaryChanged(this._local, binary)
      },
      onCallbackError: reportServerChannelError,
      onLeave: (id, cause, hidden) => this._onLeave(id, cause, hidden),
    })
    this._local = new LocalHolder(this._state, this._localSuppressed)
    this._replicaOf = RoomReplica.of(roomId, inc)
    this._subs = new RoomSubscriptions(this, this._report)
    this._sync = new AuthoritySync(this, this._subs)
    this._roster = new RosterStream(this._state, this._stubs, () => this._sync.ensureRoster())
    this._writes = new RoomWrites(this, {
      guards: () => this._guards,
      local: (id) => this._localParticipants.get(id),
      lanePlanned: (member, track) => this._subs.lanePlanned(member, track),
      applyOwn: (event) => {
        if (this._applyCtrl(event)) this._relayApplied(event)
      },
      acks: this._pendingDmAcks,
    })
  }

  /** @internal What this process shares of the incarnation; a dropped one is replaced. */
  get _replica(): RoomReplica {
    if (!this._replicaOf.live) this._replicaOf = RoomReplica.of(this.id, this._inc)
    return this._replicaOf
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
  }

  /** @internal See `Room.guard()`. One declaration per instance keeps the grant declarative. */
  _setGuards(guards: Partial<RoomGuardHooks>): void {
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
    await this._admit(admission, () => {
      this._localParticipants.set(admission.id, participant)
      this._replica.own(admission.id, this)
      if (!selfDelivery) this._localSuppressed.add(admission.id)
    })
    return participant
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    await this._sync.ensureRoster()
    return options?.hidden ? this._state.listHidden() : this._state.listVisible()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this._sync.ensureRoster()
    return this._state.getRemote(id)
  }

  snapshot(): RoomSnapshotView {
    // Loads the members for the next snapshot; their arrival fires onChange.
    if (!this._state.rosterKnown) void this._sync.ensureRoster().catch(reportRoomError)
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
    const optional = { ...(identity === null ? {} : { identity }), ...(hidden ? { hidden: true } : {}) } as const
    this._pendingAdmissions.add(id)
    this._subs.memberOwned(id)
    try {
      await this._inboxReady(id)
      await createMember(
        this._incarnation,
        id,
        { joinedAt, seenAt: joinedAt, metaSeq: 0, metaGen: 0, ...optional },
        meta,
      )
      this._assertAdmitted(id)
      this._pendingAdmissions.delete(id)
      const join = { __r: 'join', id, meta, joinedAt, ...optional } as const
      // A member removed meanwhile gets no join after its leave, here or anywhere.
      await publishCtrl(this.id, this._inc, join, { requiredCellKeys: [memberCellKey(id)] })
      this._assertAdmitted(id)
      if (this._applyJoin(joinedMember(join))) this._relayApplied(join)
    } catch (error) {
      // A member write that rejected may still have committed, its reply lost; evicting an absent member only reads.
      // In an open room the inbox closes only when the holder detached, or a removal already evicted the member.
      const lost = this._subs.inboxOf(id) === undefined && !this._state.closed
      const cause = lost ? ({ type: 'disconnected' } as const) : ({ type: 'left' } as const)
      await evictMember(this._incarnation, id, identity, cause).catch(reportRoomError)
      this._abandonAdmission(id, cause)
      throw error
    }
  }
  /** The member's inbox delivers (within the horizon) before its join is visible, and the admission still holds. */
  private async _inboxReady(id: string): Promise<void> {
    const inbox = this._subs.inboxOf(id)
    if (inbox !== undefined) await withinRoomHorizon(inbox.ready)
    this._assertAdmitted(id)
  }
  /** A closed room or a departed member drops the admission's inbox. */
  private _assertAdmitted(id: string): void {
    if (this._subs.inboxOf(id) === undefined)
      throw this._state.closed ? roomClosedError(this.id) : participantLeftError()
  }
  private _abandonAdmission(id: string, cause: LeaveCause): void {
    this._pendingAdmissions.delete(id)
    this._state.applyLeave(id, cause)
  }

  /** @internal */
  async _removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this._state.closed) return // close() already removed everyone
    const identity = this._state.getRemote(id)?.identity ?? null
    await evictMember(this._incarnation, id, identity, cause)
    this._state.applyLeave(id, cause)
  }

  /** @internal The member's holder is gone and nothing will retry: ownership ends even if eviction fails, leaving a record whose lease expires. */
  async _removeDepartedMember(id: string): Promise<void> {
    const cause = { type: 'disconnected' } as const
    try {
      await this._removeMember(id, cause)
    } catch (error) {
      this._state.applyLeave(id, cause)
      throw error
    }
  }

  /** @internal */
  async _readOpenConfig(): Promise<RoomConfigRecord | null> {
    return openConfig(await getRoomBackend().readHead(this.id), this._inc)
  }
  private async _assertOpen(): Promise<void> {
    if (this._state.closed || (await this._readOpenConfig()) === null) throw roomClosedError(this.id)
  }
  /** @internal */
  _onCtrlEvent(event: PresenceEvent, serialized: string, rawInfo: WirePublishInfo): void {
    if (!this._applyCtrl(event)) return
    this._relayControl(event, () => encodePublishText(serialized, rawInfo))
    if (event.__r === 'closed') this._teardown()
  }

  /** Relayed as this view applied it, not as a lane frame: an own event's echo may be lost with no drift to reconcile. */
  private _relayApplied(event: PresenceEvent): void {
    this._relayControl(event, () => unorderedEventText(event))
  }

  private _relayControl(event: PresenceEvent, wireText: () => string): void {
    if (this._stubs.size === 0) return
    const text = wireText()
    const hiddenMember = hiddenMemberOf(event)
    // Each attach sends the roster and the room's meta again, which carry every event but the close.
    const resentOnAttach = event.__r !== 'closed'
    for (const stub of this._stubs) stub._relayControl(text, hiddenMember, resentOnAttach)
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

  private _relayMemberData(serialized: string, event: RoomDataEnvelope, rawInfo: WirePublishInfo): void {
    if (this._stubs.size === 0) {
      this._tail?.push({ serialized, ord: rawInfo, from: event.from })
      return
    }
    const texts = new MemberMessageTexts(serialized, rawInfo, () => ({
      __r: 'data',
      from: event.from,
      data: event.data,
    }))
    for (const stub of this._stubs) stub._relayText(serialized, texts, event.from, rawInfo)
  }
  /** @internal */
  _onTextData(serialized: string, rawInfo: WirePublishInfo): void {
    const envelope = decodeLaneEnvelope(serialized) as RoomDataEnvelope | Extract<RoomEnvelope, { __r: 'announce' }>
    if (envelope.__r === 'announce') return this._applyAnnouncement(envelope, serialized, rawInfo)
    this._applyStamp(envelope.from, envelope)
    this._local.relayText(envelope, rawInfo)
    this._relayMemberData(serialized, envelope, rawInfo)
  }
  /** A stamp newer than the sender's meta here is the update this view missed so far: it applies and relays first, so
   *  every client here names the sender as of the send. */
  private _applyStamp(id: string, { fromMeta, fromMetaSeq }: SenderStamp): void {
    const hidden = this._state.memberHidden(id)
    if (hidden === null || this._state.memberMetaSeq(id)! >= fromMetaSeq) return
    const update = { __r: 'p-meta', id, meta: fromMeta, seq: fromMetaSeq, ...(hidden ? { hidden: true } : {}) } as const
    if (this._applyCtrl(update)) this._relayApplied(update)
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
    if (envelope.__r === 'dm-ack') return this._pendingDmAcks.settle(envelope.ackId, envelope)
    const holder = this._holderOf(envelope.to)
    // A client that joined through a stub gets the DM relayed (its `ackId` rides along) and answers with `dm-reply`.
    if (holder instanceof RoomStubChannel) {
      if (envelope.fromMetaSeq !== undefined) this._applyStamp(envelope.from, envelope as RoomDmEnvelope & SenderStamp)
      const { fromMeta, fromMetaSeq, fromIdentity, ...unstamped } = envelope
      const texts = new MemberMessageTexts(serialized, rawInfo, () => unstamped)
      return holder._relayDm(texts, envelope)
    }
    if (holder === undefined) return
    const msg = inboxMessageFromWire(envelope)
    const { ackId } = envelope
    if (ackId === undefined) return holder._deliverMessage(msg)
    // A server participant, or one a client holds (its forwarder answers): the reply goes to the sender's inbox.
    void holder
      ._deliverMessageAck(msg)
      .then((reply) => this._writes.publishDmAck(envelope.from, ackId, reply))
      .catch(reportRoomError)
  }
  /** Whether the event changed this view, which is what its clients get; a leave reaches them from `_onLeave`. */
  private _applyCtrl(event: PresenceEvent): boolean {
    switch (event.__r) {
      case 'join':
        return this._applyJoin(joinedMember(event))
      case 'leave':
        this._state.applyLeave(event.id, leaveCauseFromWire(event))
        return false
      case 'p-meta': {
        const applied = this._state.applyParticipantMeta(event.id, event)
        this._localParticipants.get(event.id)?._acceptMeta(event)
        return applied
      }
      case 'update':
        // Relayed here: each update this one let apply, in seq order.
        for (const applied of this._state.applyRoomUpdate(event)) this._relayApplied(applied)
        return false
      case 'closed':
        return this._state.applyClosed()
    }
  }
  private _applyJoin(member: MemberSnapshot): boolean {
    if (!this._state.applyJoin(member)) return false
    this._subs.memberChanged(member.id)
    return true
  }
  /** Every leave the state applies, event or reconcile, runs the member's cleanup. */
  private _onLeave(id: string, cause: LeaveCause, hidden: boolean | null): void {
    this._pendingDmAcks.settleMember(id, DM_FAILURE.left) // strand no waiter on a gone sender or recipient
    const local = this._localParticipants.get(id)
    if (this._holderOf(id) !== undefined) this._replica.disown(id, this)
    if (local) {
      this._localParticipants.delete(id)
      this._localSuppressed.delete(id)
      local._onLeft(cause)
    }
    // Every leave of a known member reaches the clients here, once, whatever applied it.
    if (hidden !== null)
      this._relayApplied({
        __r: 'leave',
        id,
        ...leaveCauseToWire(cause),
        ...(hidden ? { hidden: true } : {}),
      })
    this._stubOf.delete(id)
    this._local.forgetMember(id)
    for (const stub of this._stubs) {
      stub._forgetMember(id)
      if (stub._forgetWantsOf(id)) this._subs.holderChanged(stub, [id])
    }
    this._subs.memberReleased(id)
  }
  /** The room closed. Runs once, after the `closed` event has been applied and relayed. */
  private _teardown(): void {
    this._pendingDmAcks.settleAll(DM_FAILURE.roomClosed) // no recipient will reply now
    this._teardownTail()
    this._roster.clear()
    const replica = this._replica
    for (const id of this._ownedMembers()) replica.disown(id, this)
    for (const local of this._localParticipants.values()) local._onLeft({ type: 'closed' })
    this._localParticipants.clear()
    this._localSuppressed.clear()
    // A client back while its channel still holds it gets `closed`.
    const timeout = reconnectWindow()
    for (const stub of this._stubs) void stub.close({ timeout }).catch(() => {})
    replica.closed()
    void this._subs.replan()
  }

  /** @internal A newer meta reaching this view through the authority never reached its clients as an event. */
  _applyAuthorityMeta({ entries, seq }: StoredRoomMeta): void {
    for (const applied of this._state.applyRoomUpdate({ __r: 'update', seq, meta: entries }))
      this._relayApplied(applied)
  }
  /** @internal The authority says the room closed; the lane that would have carried `closed` failed. */
  _closeFromAuthority(): void {
    if (!this._state.applyClosed()) return
    this._relayApplied({ __r: 'closed' })
    this._teardown()
  }
  /** Resolves once the tail receives, so it holds everything committed after it returns. */
  async _startTail(): Promise<void> {
    this._tail = new TailHold(() => this._teardownTail())
    this._subs.syncLanes() // bring up text ingestion before any stub exists
    await withinRoomHorizon(this._subs.semanticReady)
  }
  private _teardownTail(): void {
    if (this._tail === null) return // already handed off to a stub
    this._tail.end()
    this._tail = null
    this._subs.syncLanes() // drop the text ingestion nothing is consuming
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
        meta: this._state.metaEntries(),
        closed: this.isClosed,
        metaSeq: this._state.metaSeq,
        bufferLimit: getServerConfig().channel.bufferLimit,
        count: this.count,
      },
    }
  }
  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    this._views.set(stub, holdViews(this._incarnation, [stub.id]).catch(reportRoomError))
    if (this._tail !== null) {
      // Its expiry ends only the stub's want of all text.
      stub._beginTail(this._tail.take(), () => this._subs.textChanged(stub, []))
      this._tail = null
    }
    stub.onClose(() => this._detachStub(stub))
    this._subs.holderChanged(stub)
  }
  _onStubAttached(stub: RoomStubChannel): void {
    if (!this._stubs.has(stub)) return
    this._roster.sendTo(stub)
    stub._relayEvent({ __r: 'update', seq: this._state.metaSeq, meta: this._state.metaEntries() })
    for (const member of stub._heldMembers())
      stub._relayEvent({ __r: 'demand-state', member, tracks: this._demandOf(member) })
  }
  private _detachStub(stub: RoomStubChannel): void {
    this._stubs.delete(stub)
    const view = this._views.get(stub)
    this._views.delete(stub)
    void view?.then(() => removeView(this._incarnation, stub.id)).catch(reportRoomError)
    this._roster.forget(stub)
    stub._endTail()
    this._subs.holderGone(stub)
    for (const id of stub._heldMembers()) {
      this._stubOf.delete(id)
      this._replica.disown(id, this)
      this._subs.memberReleased(id)
      if (this._pendingAdmissions.has(id)) continue // the admission rolls itself back
      void this._removeDepartedMember(id).catch(reportRoomError)
    }
  }
  /** @internal A response serialized a participant whose own frames the stub's client doesn't get back. */
  _onStubSuppressed(stub: RoomStubChannel, member: string): void {
    if (this._stubs.has(stub)) this._subs.memberChanged(member)
  }
  async _joinStubMember(stub: RoomStubChannel, req: Extract<RoomRequest, { __r: 'req-join' }>) {
    const admission = {
      id: crypto.randomUUID(),
      meta: ownMetadata(req.meta),
      identity: null,
      joinedAt: Date.now(),
      hidden: false,
    }
    await this._admit(admission, () => {
      stub._addMember(admission.id, req.selfDelivery)
      if (this._stubs.has(stub)) {
        this._stubOf.set(admission.id, stub)
        this._replica.own(admission.id, this)
      }
    })
    return { id: admission.id, joinedAt: admission.joinedAt }
  }
  async _replayRetainedText(holder: LaneHolder, before: TextWantsChange): Promise<void> {
    if (before.all) return
    await this._sync.ensureRoster()
    // Read retained only after subscription readiness: a racing commit is then retained or live, never lost in the gap.
    await withinRoomHorizon(this._subs.semanticReady)
    const stored = await getRoomBackend().readRetained(this.id, this._inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const event = parse(serialized) as RoomDataEnvelope
    // A member whose own want didn't change is wanted now only by the new room-wide want, if at all.
    const had = before.members.get(event.from) ?? holder._textDemand().members.has(event.from)
    if (had || !holder._wantsTextFrom(event.from) || !this._state.hasMember(event.from)) return
    // Replay the stored order as-is; the holder admits it only if it is newer than what it has.
    holder._emitRetainedText(event, { seq: stored.seq, timestamp: stored.timestamp }, serialized)
  }
  /** Binary uses the same readiness handoff and stored receipt; its holder dedupes the live/retained race per lane, so a
   *  frame it already has is not delivered again. A want for one member's track reads that lane; any other lists the
   *  retained lanes it covers. As with text, a frame replays only while its member is in the roster: a leave clears the
   *  member's retained frames, and one read just before that is gone with it. */
  async _replayRetainedBinary(holder: LaneHolder, { member, track }: BinaryWantChange): Promise<void> {
    await this._sync.ensureRoster()
    await (member === null ? this._subs.binaryReady() : this._subs.pairsReady(member, track))
    const backend = getRoomBackend()
    const lanes: LaneId[] =
      member !== null && track !== null
        ? [{ kind: 'binary', member, track }]
        : await backend.listRetained(
            this.id,
            this._inc,
            member !== null ? { member } : track !== null ? { track } : undefined,
          )
    for (const lane of lanes) {
      if (lane.kind !== 'binary' || !holder._wantsBinary(lane.member, lane.track)) continue
      const stored = await backend.readRetained(this.id, this._inc, lane)
      if (stored === null || !this._state.hasMember(lane.member)) continue
      const framed = stored.payload
      const frame = decodeBinaryFrame(framed)
      assert(frame)
      holder._emitRetainedBinary(frame, { seq: stored.seq, timestamp: stored.timestamp }, framed)
    }
  }

  /** @internal A holder's text wants changed (`before` is `null` when none did): replan, then replay the retained
   *  frame it now wants. */
  _onHolderTextChanged(holder: LaneHolder, before: TextWantsChange | null): void {
    if (this._holdsLanes(holder)) this._subs.textChanged(holder, [...(before?.members.keys() ?? [])])
    if (before) void this._replayRetainedText(holder, before).catch(reportRoomError)
  }

  /** @internal One of a holder's binary wants changed: replan the pairs it covers, then replay what it now wants. */
  _onHolderBinaryChanged(holder: LaneHolder, change: BinaryWantChange): void {
    if (this._holdsLanes(holder)) this._subs.binaryChanged(holder, change)
    if (change.on) void this._replayRetainedBinary(holder, change).catch(reportRoomError)
  }

  /** A stub that is not attached holds no lanes. */
  private _holdsLanes(holder: LaneHolder): boolean {
    return holder === this._local || this._stubs.has(holder as RoomStubChannel)
  }

  /** @internal */
  _observed(): boolean {
    return this._stubs.size > 0 || this._localParticipants.size > 0 || this._state.listenerCount > 0
  }

  /** @internal */
  _holdsTail(): boolean {
    return this._tail !== null
  }

  /** @internal Renews every stub's view in one write, after each one's earlier write; the caller reports a failure. */
  _renewViews(): Promise<void> {
    const stubs = [...this._views.keys()]
    if (stubs.length === 0) return Promise.resolve()
    const previous = Promise.all(this._views.values())
    const renewed = previous.then(() =>
      holdViews(
        this._incarnation,
        stubs.map((stub) => stub.id),
      ),
    )
    const settled = renewed.catch(() => {})
    for (const stub of stubs) this._views.set(stub, settled)
    return renewed
  }

  /** @internal */
  *_ownedMembers(): Generator<string> {
    yield* this._localParticipants.keys()
    yield* this._stubOf.keys()
  }
  *_renewableMembers(): Generator<string> {
    for (const id of this._ownedMembers()) if (!this._pendingAdmissions.has(id)) yield id
  }

  /** @internal */
  _ownsMember(id: string): boolean {
    return this._holderOf(id) !== undefined
  }

  private _holderOf(id: string): ServerLocalParticipant | RoomStubChannel | undefined {
    return this._localParticipants.get(id) ?? this._stubOf.get(id)
  }
  /** The owned member's tracks someone wants. */
  _demandOf(member: string): Array<string | null> {
    return this._replica.demand.wanted(member).map(publicTrack)
  }
  /** @internal */
  _deliverDemand(member: string, track: string, wanted: boolean): void {
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
  private _metaMirror: ((accepted: AcceptedMeta) => void) | null = null
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
  /** @internal Read from the room's demand, which holds it once. */
  get _demandedTracks(): Array<string | null> {
    return this._left ? [] : this._room._demandOf(this.id)
  }
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
    return await this._room._writes.publishText(this.id, data, retain)
  }
  /** @internal */
  async _publishFrame(frame: BinaryFrame, framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._writes.publishBinaryFrame(frame, framed)
  }
  /** @internal Each meta update this participant applies, which a client holding it mirrors. */
  _onAcceptedMeta(callback: (accepted: AcceptedMeta) => void): () => void {
    this._metaMirror = callback
    return () => {
      if (this._metaMirror === callback) this._metaMirror = null
    }
  }
  override _acceptMeta(update: AcceptedMeta): boolean {
    if (!super._acceptMeta(update)) return false
    this._metaMirror?.(update)
    return true
  }
  /** @internal The client holding this participant went away without leaving. */
  _releaseHolder(): Promise<void> {
    return this._room._removeDepartedMember(this.id)
  }
  send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    assertKnownOptions(options, ['ack'], 'send()')
    assertUsage(
      options?.ack === undefined || typeof options.ack === 'boolean',
      'send() options.ack should be a boolean',
    )
    return markHandled(this._sendDm(recipientId(to), ownMessage(data), options?.ack === true))
  }
  private async _sendDm(to: string, data: unknown, ack: boolean): Promise<unknown> {
    this._assertActive()
    return await this._room._writes.sendDm(this.id, to, data, ack)
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
    return await this._room._writes.setMemberMeta(this.id, meta)
  }
  /** @internal */
  async _setAttributes(attrs: ParticipantMeta): Promise<AcceptedMeta> {
    this._assertActive()
    return await this._room._writes.mergeMemberMeta(this.id, attrs)
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
  protected _inboxBufferLimit(): number {
    return getServerConfig().channel.bufferLimit
  }
}
