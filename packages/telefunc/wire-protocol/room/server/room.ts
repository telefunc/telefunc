export { ServerRoom, ServerLocalParticipant }
export type { MetaMirror }

import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shield-key.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { markHandled } from '../../../utils/markHandled.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import type { ChannelPublishAck } from '../../channel.js'
import type { WirePublishInfo } from '../../shared-ws.js'
import {
  encodeBinaryFrame,
  decodeBinaryFrame,
  type BinaryFrame,
  type BinaryWantChange,
  type TrackKey,
} from '../binary.js'
import { participantLeftError, roomClosedError, ROOM_BUG_MESSAGE } from '../errors.js'
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
  type SenderStamp,
  type RoomDmEnvelope,
  type RoomCtrlEnvelope,
  type AcceptedMeta,
  type RoomSnapshotMetadata,
} from '../protocol.js'
import { type RoomDataEnvelope, type RoomMemberRecord } from './protocol.js'
import { type RoomModel, RoomState, RoomStateView } from '../state.js'
import { ParticipantBase } from '../participant.js'
import { MemberMessageTexts, RoomStubChannel } from './stub.js'
import type { RoomRequest } from './requests.js'
import { LocalHolder, type LaneHolder, type TextWantsChange } from './replay.js'
import { TailHold } from './tail.js'
import type { RoomReplica } from './replica.js'
import { RoomWrites, runAfterHook } from './writes.js'
import { commitControlOrThrow, withinRoomHorizon } from './lanes.js'
import { reportRoomError } from './errors.js'
import { reportServerChannelError } from '../../server/channel.js'
import { getServerConfig, reconnectWindow } from '../../../node/server/serverConfig.js'
import { createMember, evictMember } from './membership.js'
import { memberCellKey } from './cells.js'
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

type Admission = { id: string; meta: ParticipantMeta; identity: string | null; joinedAt: number; hidden: boolean }
/** An admission through this handle once its holder took the member: its inbox opening (`owned`), its record durable
 *  (`committed`), or given up because the member left, its holder detached or the room closed (`abandoned`). Before
 *  that it is a guard running; once its join applies, a member like any other. */
type AdmissionState = 'owned' | 'committed' | 'abandoned'

/** One `Room.get()`'s handle on a room: its guards, its tail, the stubs it opened, its participants and its listeners,
 *  over this process's one view of the room, which every handle to it shares. Each serialization opens a fresh stub. */
class ServerRoom extends RoomStateView implements Room {
  readonly [SERVER_ROOM_BRAND] = true
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }

  /** @internal Every authority check and member write carries this incarnation, rejecting stale handles after recreate. */
  readonly _incarnation: RoomIncarnation
  /** @internal What this process shares of the incarnation. */ readonly _replica: RoomReplica
  /** `Room.get({ tail: true })`'s hold until a stub attaches and takes it. */
  private _tail: TailHold | null = null
  private _guards: Partial<RoomGuardHooks> | null = null
  /** @internal This handle's listeners over the replica's model. */ readonly _state: RoomState
  /** @internal */ readonly _local: LocalHolder
  private readonly _stubs = new Set<RoomStubChannel>()
  /** The attached stub holding each member a client joined through. */
  private readonly _stubOf = new Map<string, RoomStubChannel>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()
  /** The local participants that joined with `selfDelivery: false`. */
  private readonly _localSuppressed = new Set<string>()
  /** The admissions under way; an `owned` one the heartbeat leaves alone, and its holder's detach doesn't evict. */
  private readonly _admissions = new Map<string, AdmissionState>()
  /** @internal */ readonly _writes: RoomWrites

  constructor(replica: RoomReplica) {
    super()
    this._replica = replica
    this._incarnation = replica._incarnation
    this._state = new RoomState({
      owner: this,
      model: replica.model,
      onListenersChanged: (member, binary) => {
        this._replica.track(this)
        this._onHolderTextChanged(this._local, this._local.refreshText(member))
        if (binary !== null && this._local.applyBinary(binary)) this._onHolderBinaryChanged(this._local, binary)
      },
      onCallbackError: reportServerChannelError,
    })
    this._local = new LocalHolder(this._state, this._localSuppressed)
    this._writes = new RoomWrites(this, {
      guards: () => this._guards,
      local: (id) => this._localParticipants.get(id),
    })
  }

  /** @internal */
  get _model(): RoomModel {
    return this._replica.model
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
      this._replica.track(this)
    })
    return participant
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    assertKnownOptions(options, ['hidden'], 'getParticipants()')
    await this._replica.sync.ensureRoster()
    return options?.hidden ? this._state.listHidden() : this._state.listVisible()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this._replica.sync.ensureRoster()
    return this._state.getRemote(id)
  }

  snapshot(): RoomSnapshotView {
    // Loads the members for the next snapshot; their arrival fires onChange.
    if (!this._model.rosterKnown) void this._replica.sync.ensureRoster().catch(reportRoomError)
    return this._model.snapshot()
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
    const record: Omit<RoomMemberRecord, 'session'> = { joinedAt, metaSeq: 0, metaGen: 0 }
    const join: Extract<RoomCtrlEnvelope, { __r: 'join' }> = { __r: 'join', id, meta, joinedAt }
    if (identity !== null) record.identity = join.identity = identity
    if (hidden) record.hidden = join.hidden = true
    // A holder that detached during the guard took nothing, and a room that closed meanwhile takes no one.
    this._admissions.set(id, this._holderOf(id) !== undefined && !this._model.closed ? 'owned' : 'abandoned')
    this._replica.plan.memberOwned(id)
    try {
      await this._inboxReady(id)
      await createMember(this._incarnation, this._replica.session, id, record, meta)
      this._assertAdmitted(id)
      this._admissions.set(id, 'committed')
      // A member removed meanwhile gets no join after its leave, here or anywhere.
      await commitControlOrThrow(this.id, this._incarnation.inc, join, { requiredCellKeys: [memberCellKey(id)] })
      this._assertAdmitted(id)
      this._admissions.delete(id)
      this._replica.applyControl(join)
    } catch (error) {
      // A member write that rejected may still have committed, its reply lost; evicting an absent member only reads.
      // In an open room only a detach of the holder, or a removal that already evicted the member, abandons it.
      const lost = this._admissions.get(id) === 'abandoned' && !this._model.closed
      const cause = lost ? ({ type: 'disconnected' } as const) : ({ type: 'left' } as const)
      await evictMember(this._incarnation, id, identity, cause).catch((error: unknown) => {
        reportRoomError(error)
        this._replica.evictLater(id, identity)
      })
      this._abandonAdmission(id, cause)
      throw error
    }
  }
  /** The member's inbox delivers (within the horizon) before its join is visible, and the admission still holds. */
  private async _inboxReady(id: string): Promise<void> {
    const inbox = this._replica.plan.inboxOf(id)
    if (inbox !== undefined) await withinRoomHorizon(inbox.ready)
    this._assertAdmitted(id)
  }
  private _assertAdmitted(id: string): void {
    if (this._admissions.get(id) === 'abandoned')
      throw this._model.closed ? roomClosedError(this.id) : participantLeftError()
  }
  private _abandonAdmission(id: string, cause: LeaveCause): void {
    this._admissions.delete(id)
    this._model.applyLeave(id, cause)
  }

  /** @internal */
  async _removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this._model.closed) return // close() already removed everyone
    const identity = this._state.getRemote(id)?.identity ?? null
    await evictMember(this._incarnation, id, identity, cause)
    this._model.applyLeave(id, cause)
  }

  /** @internal The member's holder is gone: ownership ends even if eviction fails, which the heartbeat then retries. */
  async _removeDepartedMember(id: string): Promise<void> {
    const cause = { type: 'disconnected' } as const
    const identity = this._state.getRemote(id)?.identity ?? null
    try {
      await this._removeMember(id, cause)
    } catch (error) {
      this._replica.evictLater(id, identity)
      this._model.applyLeave(id, cause)
      throw error
    }
  }

  private async _assertOpen(): Promise<void> {
    if (this._model.closed || (await this._replica._readOpenConfig()) === null) throw roomClosedError(this.id)
  }

  /** @internal What the lane carried for this handle's own listeners, or for its tail while no stub took it. */
  _relayData(serialized: string, event: RoomDataEnvelope, info: WirePublishInfo): void {
    this._local.relayText(event, info)
    if (this._stubs.size === 0) this._tail?.push({ serialized, ord: info, from: event.from })
  }

  /** @internal A meta update of a member this handle owns, which its participant mirrors. */
  _acceptMeta(id: string, accepted: AcceptedMeta): void {
    this._localParticipants.get(id)?._acceptMeta(accepted)
  }

  /** @internal A member this handle owns left: its participant ends, and its admission, if under way, is abandoned. */
  _memberLeft(id: string, cause: LeaveCause): void {
    const local = this._localParticipants.get(id)
    if (local) {
      this._localParticipants.delete(id)
      this._localSuppressed.delete(id)
      local._onLeft(cause)
    }
    this._stubOf.delete(id)
    if (this._admissions.has(id)) this._admissions.set(id, 'abandoned')
    this._replica.track(this)
  }

  /** @internal The room closed: the replica already applied and relayed `closed`. */
  _teardown(): void {
    this._teardownTail()
    for (const local of this._localParticipants.values()) local._onLeft({ type: 'closed' })
    this._localParticipants.clear()
    this._localSuppressed.clear()
    for (const id of this._admissions.keys()) this._admissions.set(id, 'abandoned')
    // A client back while its channel still holds it gets `closed`.
    const timeout = reconnectWindow()
    for (const stub of this._stubs) void stub.close({ timeout }).catch(() => {})
    this._replica.track(this)
  }

  /** @internal A DM for a member this handle owns, routed to its holder: a server participant or its client stub. */
  _onDm(envelope: RoomDmEnvelope, serialized: string, info: WirePublishInfo): void {
    const holder = this._holderOf(envelope.to)
    // A client that joined through a stub gets the DM relayed (its `ackId` rides along) and answers with `dm-reply`.
    if (holder instanceof RoomStubChannel) {
      if (envelope.fromMetaSeq !== undefined)
        this._replica.applyStamp(envelope.from, envelope as RoomDmEnvelope & SenderStamp)
      const texts = new MemberMessageTexts(serialized, info, () => {
        const { fromMeta, fromMetaSeq, fromIdentity, ...unstamped } = envelope
        return unstamped
      })
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

  /** Resolves once the tail receives, so it holds everything committed after it returns. */
  async _startTail(): Promise<void> {
    this._tail = new TailHold(() => this._teardownTail())
    this._replica.track(this)
    this._replica.plan.syncLanes() // bring up text ingestion before any stub exists
    await withinRoomHorizon(this._replica.plan.semanticReady)
  }
  private _teardownTail(): void {
    if (this._tail === null) return // already handed off to a stub
    this._tail.end()
    this._tail = null
    this._replica.track(this)
    this._replica.plan.syncLanes() // drop the text ingestion nothing is consuming
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
        meta: this._model.metaEntries(),
        closed: this.isClosed,
        metaSeq: this._model.metaSeq,
        bufferLimit: getServerConfig().channel.bufferLimit,
        count: this.count,
      },
    }
  }
  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    this._replica.attachStub(stub)
    if (this._tail !== null) {
      // Its expiry ends only the stub's want of all text.
      stub._beginTail(this._tail.take(), () => this._replica.plan.textChanged(stub, []))
      this._tail = null
    }
    stub.onClose(() => this._detachStub(stub))
    this._replica.track(this)
    this._replica.plan.holderChanged(stub)
  }
  _onStubAttached(stub: RoomStubChannel): void {
    if (!this._stubs.has(stub)) return
    this._replica.roster.sendTo(stub)
    stub._relayEvent({ __r: 'update', seq: this._model.metaSeq, meta: this._model.metaEntries() })
    for (const member of stub._heldMembers())
      stub._relayEvent({ __r: 'demand-state', member, tracks: this._replica.demand.wanted(member) })
  }
  private _detachStub(stub: RoomStubChannel): void {
    this._stubs.delete(stub)
    this._replica.detachStub(stub)
    stub._endTail()
    this._replica.track(this)
    this._replica.plan.holderGone(stub)
    for (const id of stub._heldMembers()) {
      this._stubOf.delete(id)
      this._replica.disown(id, this)
      this._replica.plan.memberReleased(id)
      const admission = this._admissions.get(id)
      if (admission !== undefined) this._admissions.set(id, 'abandoned')
      if (admission === 'owned') continue // the admission rolls itself back
      void this._removeDepartedMember(id).catch(reportRoomError)
    }
  }
  /** @internal A response serialized a participant whose own frames the stub's client doesn't get back. */
  _onStubSuppressed(stub: RoomStubChannel, member: string): void {
    if (this._stubs.has(stub)) this._replica.plan.memberChanged(member)
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
      stub._addMember(req.reqId, admission.id, req.selfDelivery)
      if (this._stubs.has(stub)) {
        this._stubOf.set(admission.id, stub)
        this._replica.own(admission.id, this)
      }
    })
    return { id: admission.id, joinedAt: admission.joinedAt }
  }

  /** @internal A holder's text wants changed (`before` is `null` when none did): replan, then replay the retained
   *  frame it now wants. */
  _onHolderTextChanged(holder: LaneHolder, before: TextWantsChange | null): void {
    if (this._holdsLanes(holder)) this._replica.plan.textChanged(holder, [...(before?.members.keys() ?? [])])
    if (before) void this._replica.replayText(holder, before).catch(reportRoomError)
  }

  /** @internal One of a holder's binary wants changed: replan the pairs it covers, then replay what it now wants. */
  _onHolderBinaryChanged(holder: LaneHolder, change: BinaryWantChange): void {
    if (this._holdsLanes(holder)) this._replica.plan.binaryChanged(holder, change)
    if (change.on) void this._replica.replayBinary(holder, change).catch(reportRoomError)
  }

  /** A stub that is not attached holds no lanes. */
  private _holdsLanes(holder: LaneHolder): boolean {
    return holder === this._local || this._stubs.has(holder as RoomStubChannel)
  }

  /** @internal Whether this handle observes the room: a client stub, a participant, or a listener. */
  _observed(): boolean {
    return this._stubs.size > 0 || this._localParticipants.size > 0 || this._state.listenerCount > 0
  }

  /** @internal */
  _holdsTail(): boolean {
    return this._tail !== null
  }

  private _holderOf(id: string): ServerLocalParticipant | RoomStubChannel | undefined {
    return this._localParticipants.get(id) ?? this._stubOf.get(id)
  }
  /** @internal */
  _deliverDemand(member: string, track: string, wanted: boolean): void {
    const holder = this._holderOf(member)
    if (holder instanceof ServerLocalParticipant) holder._onDemand(track, wanted)
    else holder?._relayEvent({ __r: 'demand', member, track, wanted })
  }
}
const SERVER_PARTICIPANT_BRAND: unique symbol = Symbol.for('telefunc.ServerRoomParticipant')
type MetaMirror = { accepted(update: AcceptedMeta): void; rewound(): void }
/** Server-side `LocalParticipant`, returned by `ServerRoom.join()`. */
class ServerLocalParticipant extends ParticipantBase {
  readonly [SERVER_PARTICIPANT_BRAND] = true
  /** @internal */ readonly _room: ServerRoom
  private _metaMirror: MetaMirror | null = null
  constructor(
    serverRoom: ServerRoom,
    id: string,
    meta: ParticipantMeta,
    selfDelivery: boolean,
    identity: string | null,
  ) {
    super({ id, meta, selfDelivery, identity }, serverRoom._model.metaRewinds)
    this._room = serverRoom
  }
  static isServerLocalParticipant(value: unknown): value is ServerLocalParticipant {
    return value !== null && typeof value === 'object' && SERVER_PARTICIPANT_BRAND in value
  }
  /** @internal Read from the room's demand, which holds it once. */
  get _demandedTracks(): TrackKey[] {
    return this._left ? [] : this._room._replica.demand.wanted(this.id)
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
  /** @internal Each meta update this participant applies, and each failover rewind of its seq, which a client holding it
   *  mirrors. */
  _onMeta(mirror: MetaMirror): () => void {
    this._metaMirror = mirror
    this._room._replica.metaMirrors.add(mirror)
    return () => {
      this._room._replica.metaMirrors.delete(mirror)
      if (this._metaMirror === mirror) this._metaMirror = null
    }
  }
  override _acceptMeta(update: AcceptedMeta): boolean {
    if (!super._acceptMeta(update)) return false
    this._metaMirror?.accepted(update)
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
  protected readonly _bugMessage = ROOM_BUG_MESSAGE
  protected _inboxBufferLimit(): number {
    return getServerConfig().channel.bufferLimit
  }
}
