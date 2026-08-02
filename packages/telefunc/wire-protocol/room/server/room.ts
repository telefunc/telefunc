export { ServerRoom, ServerLocalParticipant }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { ShieldValidationError } from '../../../shared/ShieldValidationError.js'
import type { ShieldValidator } from '../../../node/server/shield.js'
import type { TELEFUNC_SHIELDS } from '../../../node/shared/transformer/generateShield/shield-key.js'
import { assert, assertUsage } from '../../../utils/assert.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { isObject } from '../../../utils/isObject.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { makePublishInfo, type ChannelPublishAck } from '../../channel.js'
import {
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
} from '../constants.js'
import { getRoomBackend } from '../../backend/install.js'
import type { LaneId } from '../../backend/room/contract.js'
import type { BackendSubscription } from '../../backend/subscription.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../../shared-ws.js'
import {
  frameWithMemberId,
  binaryFrameSender,
  unframeMemberId,
  DEFAULT_TRACK,
  emptyTrackWants,
  mergeTrackWants,
  binaryWantsCovers,
  wantsAnyBinary,
  sanitizeBinaryWants,
  type BinaryWants,
  type TrackWants,
} from '../binary.js'
import { RoomError, roomFailureError } from '../errors.js'
import { roomIdentityMemberKvKey, roomMemberKvKey } from '../keys.js'
import { leaveCauseFromWire, mergeAttributes, normalizeJoinOptions, ownMetadata } from '../model.js'
import {
  hasRoomTag,
  pushBoundedTail,
  type MemberWants,
  type MemberSnapshot,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
  type RoomOrder,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type RoomDmAckEnvelope,
  type DmReply,
  type RoomEnvelope,
  type RoomMemberRecord,
  type RoomStubRequest,
} from '../protocol.js'
import { RoomState, RoomStateView } from '../state.js'
import { RoomDemand } from '../demand.js'
import { ParticipantBase, type InboxMessage } from '../participant.js'
import type { RoomStubChannel } from '../stubs.js'
import {
  CONTROL_LANE,
  SEMANTIC_LANE,
  SubSlot,
  commitRoomLane,
  configFromHead,
  decodeRoomText,
  encodeRoomText,
  publishCtrl,
  withinRoomHorizon,
} from './lanes.js'
import { reportRoomError } from './errors.js'
import { evictMember, mutateCells, readCell, readMembers } from './membership.js'
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

function decodeSemanticEnvelope(serialized: string): RoomEnvelope | undefined {
  try {
    const envelope: unknown = parse(serialized)
    return hasRoomTag(envelope) ? (envelope as RoomEnvelope) : undefined
  } catch {
    return undefined
  }
}
function shouldRelayMemberData(stub: RoomStubChannel, from: string): boolean {
  return stub._wantsTextFrom(from) && !stub._selfSuppressed.has(from)
}
function requireStubMember(stub: RoomStubChannel, id: unknown): string {
  if (typeof id !== 'string' || !stub._stubMembers.has(id))
    throw new RoomError('Not a participant of this room (joined through this connection)')
  return id
}

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
  /** Tail mode ingests from `Room.get` until stub attach or close, closing the pre-serialization gap. */
  _tail = false
  /** Pre-attach tail text is bounded drop-oldest, then handed to the attaching stub. */
  private readonly _tailHold: Array<{ serialized: string; ord: RoomOrder; from: string }> = []
  private _tailTimer: ReturnType<typeof setTimeout> | null = null
  /** In-flight `send(…, { ack: true })`s awaiting the recipient's reply, keyed by `ackId`. `to` is the recipient, so a leave/close can fail the ones it strands. Empty at steady state. */
  private readonly _pendingDmAcks = new Map<string, { to: string; settle: (reply: DmReply) => void }>()
  private _guards: RoomGuards | null = null
  /** @internal */ readonly _state: RoomState
  private readonly _stubs = new Set<RoomStubChannel>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()
  /** Members registered with their holder so their inbox can establish, but not yet durable. They own inbox routes; heartbeat must not renew/reap them until the member cell commits. */
  private readonly _pendingAdmissions = new Set<string>()

  private readonly _ctrlSub = new SubSlot(
    (slot, error) => this._onTerminalSubscription(slot, error),
    () => void this._reconcileAuthority().catch(reportRoomError),
  )
  private readonly _textSub = new SubSlot(
    (slot, error) => this._onTerminalSubscription(slot, error),
    () => void this._reconcileAuthority().catch(reportRoomError),
  )
  /** Upstream subscriptions keyed by their policy identity. */
  private readonly _binaryKeyUnsubs = new Map<string, SubSlot>()
  private readonly _dmUnsubs = new Map<string, SubSlot>()
  /** (member, track) pairs this instance has already announced — first publish pays the KV append + ctrl event, every further frame is a Set lookup. */
  private readonly _announcedTracks = new Map<string, Set<string>>()
  /** Cross-node binary-demand aggregation (`onDemand`) — constructed once `roomId` and the ownership/delivery callbacks are available (see the constructor). */
  private readonly _demand: RoomDemand
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _heartbeatBusy = false
  private _pendingRefresh: Promise<void> | null = null
  private readonly _recoveringSubscriptions = new Set<SubSlot>()
  private _controlSeq = 0

  constructor(roomId: string, config: RoomConfigRecord, seed: { members: MemberSnapshot[] } | { count: number }) {
    super()
    this._inc = config.inc
    this._state = new RoomState({
      roomId,
      meta: config.meta,
      seed,
      updateStamp: { at: config.at, by: config.by },
      onListenersChanged: () => this._syncSubs(),
      onCallbackError: reportRoomError,
    })
    this._state._owner = this
    this._demand = new RoomDemand(
      (event) => void publishCtrl(roomId, config.inc, { __r: 'want', ...event }).catch(reportRoomError),
      (id) => this._ownsMember(id),
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
    let participant!: ServerLocalParticipant
    await this._admitMember(
      meta,
      identity,
      (id) => {
        participant = new ServerLocalParticipant(this, id, meta, selfDelivery, identity)
        this._localParticipants.set(id, participant)
      },
      hidden,
    )
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

  snapshot = (): RoomSnapshotView => {
    // Snapshot consumers want the member view — load it (need-driven, single-flight); the arrival lands as an onChange, and the next snapshot() is complete.
    if (!this._state.rosterKnown) void this._ensureRoster().catch(reportRoomError)
    return this._state.snapshot()
  }

  /** Both join paths register ownership before inbox/heartbeat sync and join announcement. */
  private async _admitMember(
    meta: ParticipantMeta,
    identity: string | null,
    track: (id: string) => void,
    hidden = false,
  ): Promise<{ id: string; joinedAt: number }> {
    await this._assertOpen()
    const id = crypto.randomUUID()
    const joinedAt = Date.now()
    // Admission policy runs first, on the definitive member ID — a rejected join writes nothing.
    const onBeforeJoin = this._guards?.onBeforeJoin
    if (!hidden && onBeforeJoin) await onBeforeJoin({ id, meta, identity })
    this._pendingAdmissions.add(id)
    track(id)
    this._syncSubs()
    let created = false
    try {
      const inbox = this._dmUnsubs.get(id)
      assert(inbox)
      await withinRoomHorizon(inbox.ready, ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS)
      await this._createMember(id, meta, identity, joinedAt, hidden)
      created = true
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
          await evictMember(this.id, this._inc, id, identity ?? undefined, { type: 'left' })
        } catch (rollbackError) {
          reportRoomError(rollbackError)
        }
      }
      this._applyLeave(id, { type: 'left' })
      throw error
    }
    if (hidden) return { id, joinedAt } // announced above; a hidden participant has no post-join hook
    const onAfterJoin = this._guards?.onAfterJoin
    if (onAfterJoin) await runAfterHook(() => onAfterJoin({ id, meta, identity }, { joinedAt }))
    return { id, joinedAt }
  }

  /** Persist the member cells for a join, guarding against a concurrent `Room.close()`. */
  private async _createMember(
    id: string,
    meta: ParticipantMeta,
    identity: string | null,
    joinedAt: number,
    hidden = false,
  ): Promise<void> {
    const record: RoomMemberRecord = {
      meta,
      joinedAt,
      seenAt: joinedAt,
      metaSeq: 0,
      ...(identity === null ? {} : { identity }),
      ...(hidden ? { hidden: true } : {}),
    }
    const memberKey = roomMemberKvKey(this.id, id)
    const keys = [memberKey]
    if (identity !== null) keys.push(roomIdentityMemberKvKey(this.id, identity, id))
    await mutateCells(this.id, this._inc, { keys }, () => ({
      value: undefined,
      mutations: keys.map((key) => ({
        key,
        set: {
          bytes: key === memberKey ? encodeRoomText(stringify(record)) : new Uint8Array(),
        },
      })),
    }))
  }

  /** @internal */
  async _removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this._state.closed) return // close() already removed everyone
    const identity = this._state.getRemote(id)?.identity ?? null
    await evictMember(this.id, this._inc, id, identity ?? undefined, cause)
    this._applyLeave(id, cause)
  }

  /** @internal — full replace (`setMeta`). */
  async _setMemberMeta(id: string, meta: ParticipantMeta): Promise<void> {
    assertUsage(isObject(meta), 'setMeta() meta should be an object')
    const owned = ownMetadata(meta)
    await this._writeMemberMeta(id, () => owned)
  }

  /** @internal — per-key merge (`setAttributes`); an `undefined` value deletes the key. */
  async _mergeMemberMeta(id: string, attrs: ParticipantMeta): Promise<void> {
    assertUsage(isObject(attrs), 'setAttributes() attributes should be an object')
    const owned = ownMetadata(attrs)
    await this._writeMemberMeta(id, (current) => mergeAttributes(current, owned))
  }

  private async _writeMemberMeta(
    id: string,
    computeMeta: (current: ParticipantMeta) => ParticipantMeta,
  ): Promise<void> {
    const key = roomMemberKvKey(this.id, id)
    const { meta, seq } = await mutateCells(this.id, this._inc, { keys: [key] }, (cells) => {
      const raw = cells.get(key)
      if (raw === undefined) throw new RoomError(`Participant not found (left?): ${id}`)
      const record = parse(decodeRoomText(raw)) as RoomMemberRecord
      const meta = computeMeta(record.meta)
      const seq = record.metaSeq + 1
      const next = { ...record, meta, metaSeq: seq, seenAt: Date.now() } satisfies RoomMemberRecord
      return {
        value: { meta, seq },
        mutations: [{ key, set: { bytes: encodeRoomText(stringify(next)) } }],
      }
    })
    this._state.applyParticipantMeta(id, meta, seq)
    this._syncLocalMemberMeta(id)
    await publishCtrl(this.id, this._inc, { __r: 'p-meta', id, meta, seq })
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
    const commit = await commitRoomLane(this.id, this._inc, SEMANTIC_LANE, encodeRoomText(stringify(envelope)), {
      retain,
      requiredCellKeys: [roomMemberKvKey(this.id, from)],
    })
    if (commit === null) return await this._throwStaleMembers(from)
    return this._finishPublish(sender, data, commit)
  }

  async _publishBinaryFramed(from: string, framed: Uint8Array): Promise<ChannelPublishAck> {
    const frame = unframeMemberId(framed)
    if (!frame) throw new RoomError('Malformed binary frame')
    const sender = await this._admitPublish(from, frame.payload)
    if (frame.track !== null) await this._ensureTrackAnnounced(from, frame.track)
    const result = await commitRoomLane(
      this.id,
      this._inc,
      { kind: 'binary', member: from, track: frame.track ?? DEFAULT_TRACK },
      framed,
      { retain: frame.retain, requiredCellKeys: [roomMemberKvKey(this.id, from)] },
    )
    if (result === null) return await this._throwStaleMembers(from)
    const ack = await this._finishPublish(sender, frame.payload, result)
    return ack
  }

  private async _admitPublish(from: string, payload: unknown): Promise<Sender> {
    if (this._state.closed) throw new RoomError(`Room is closed: ${this.id}`)
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
    const key = roomMemberKvKey(this.id, from)
    const appended = await mutateCells(this.id, this._inc, { keys: [key] }, (cells) => {
      const raw = cells.get(key)
      if (raw === undefined) throw new RoomError(`Participant not found (left?): ${from}`)
      const record = parse(decodeRoomText(raw)) as RoomMemberRecord
      const tracks = record.tracks ?? []
      if (tracks.includes(track)) {
        return { value: false, mutations: [] }
      }
      const next = { ...record, tracks: [...tracks, track], seenAt: Date.now() } satisfies RoomMemberRecord
      return {
        value: true,
        mutations: [{ key, set: { bytes: encodeRoomText(stringify(next)) } }],
      }
    })
    if (appended) await publishCtrl(this.id, this._inc, { __r: 'track', id: from, track })
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

  async _sendDmAck(from: string, to: string, data: unknown): Promise<{ receipt: RoomSendReceipt; reply: DmReply }> {
    const ackId = crypto.randomUUID()
    let timer: ReturnType<typeof setTimeout> | undefined
    const reply = new Promise<DmReply>((settle) => {
      this._pendingDmAcks.set(ackId, { to, settle })
      // The recipient replying/leaving/overflowing settles this promptly; this bounds the one case none of those cover — a recipient that joined but never listens and never leaves.
      timer = setTimeout(() => {
        if (this._pendingDmAcks.delete(ackId))
          settle({ ok: false, err: 'send({ ack: true }) timed out — the recipient never handled the message' })
      }, ROOM_DM_ACK_TIMEOUT_MS)
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
    if (this._state.closed) throw new RoomError(`Room is closed: ${this.id}`)
    const target = await this._resolveMember(to)
    if (!target) throw new RoomError(`Participant not found: ${to}`)
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
    const receipt = await commitRoomLane(
      this.id,
      this._inc,
      { kind: 'inbox', member: to },
      encodeRoomText(stringify(envelope)),
      { requiredCellKeys: [roomMemberKvKey(this.id, from), roomMemberKvKey(this.id, to)] },
    )
    if (receipt === null) return await this._throwStaleMembers(from, to)
    const info: RoomSendReceipt = { seq: receipt.seq, timestamp: receipt.timestamp }
    const onAfterSend = this._guards?.onAfterSend
    if (onAfterSend) await runAfterHook(() => onAfterSend(sender, target, data, info))
    return info
  }

  private async _publishDmAck(to: string, ackId: string, reply: DmReply): Promise<void> {
    const envelope: RoomDmAckEnvelope = { __r: 'dm-ack', to, ackId, ...reply }
    const committed = await commitRoomLane(
      this.id,
      this._inc,
      { kind: 'inbox', member: to },
      encodeRoomText(stringify(envelope)),
      { requiredCellKeys: [roomMemberKvKey(this.id, to)] },
    )
    if (committed === null) throw new RoomError(`Room is closed: ${this.id}`)
  }

  private _resolveDmAck(envelope: RoomDmAckEnvelope): void {
    const pending = this._pendingDmAcks.get(envelope.ackId)
    if (!pending) return
    this._pendingDmAcks.delete(envelope.ackId)
    pending.settle(envelope)
  }

  private _rejectDmAcks(message: string, to?: string): void {
    for (const [ackId, pending] of this._pendingDmAcks) {
      if (to !== undefined && pending.to !== to) continue
      this._pendingDmAcks.delete(ackId)
      pending.settle({ ok: false, err: message })
    }
  }

  private async _resolveMember(id: string): Promise<Sender | null> {
    const remote = this._state.getRemote(id)
    if (remote) return remote
    const raw = await readCell(this.id, this._inc, roomMemberKvKey(this.id, id))
    if (raw === null) return null
    const record = parse(decodeRoomText(raw)) as RoomMemberRecord
    return { id, meta: record.meta, identity: record.identity ?? null }
  }
  private async _openConfig(): Promise<RoomConfigRecord | null> {
    const current = await getRoomBackend().readHead(this.id)
    if (current === null || current.head.state !== 'open' || current.head.currentInc !== this._inc) return null
    return configFromHead(current.head)
  }
  private async _assertOpen(): Promise<void> {
    if (this._state.closed || (await this._openConfig()) === null) {
      throw new RoomError(`Room is closed: ${this.id}`)
    }
  }
  private async _throwStaleMembers(...ids: string[]): Promise<never> {
    await this._assertOpen()
    for (const id of ids) {
      if ((await readCell(this.id, this._inc, roomMemberKvKey(this.id, id))) === null) {
        throw new RoomError(`Participant not found (left?): ${id}`)
      }
    }
    throw new RoomError(`Room is closed: ${this.id}`)
  }
  private _onCtrlMessage(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the control lane
    }
    if (!hasRoomTag(envelope)) return
    const event = envelope as RoomEnvelope
    if (event.__r === 'data' || event.__r === 'announce') return // semantic messages never travel here
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
    const serverOnly = this._hidesFromClients(event)
    this._applyCtrl(event)
    if (this._stubs.size > 0 && !serverOnly) {
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) stub._relayPublishText(wireText)
    }
    if (this._state.closed && !wasClosed) this._teardown()
  }
  private _hidesFromClients(event: RoomEnvelope): boolean {
    switch (event.__r) {
      case 'join':
        return event.hidden === true
      case 'leave':
      case 'p-meta':
      case 'track':
        return this._state.isHidden(event.id)
      default:
        return false // room-level events (update/closed) always reach clients
    }
  }

  private _applyAnnouncement(announce: Extract<RoomEnvelope, { __r: 'announce' }>, serialized: string, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    this._state.applyAnnounce(announce.data, info)
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) if (stub._wantsAnnounce) stub._relayTextLive(wireText, '', rawInfo)
  }

  private _applyMemberData(event: RoomDataEnvelope, rawInfo: WirePublishInfo): void {
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    if (!this._suppress(event.from))
      this._state.applyData(event.from, event.fromMeta, event.fromIdentity ?? null, event.data, info)
    this._healUnknownSender(event.from)
  }

  private _relayMemberData(serialized: string, event: RoomDataEnvelope, rawInfo: WirePublishInfo): void {
    if (this._stubs.size === 0) {
      if (!this._tail) return
      pushBoundedTail(this._tailHold, { serialized, ord: rawInfo, from: event.from })
      return
    }
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) {
      if (stub._tailPending !== null) stub._holdTail(serialized, rawInfo, event.from)
      else if (shouldRelayMemberData(stub, event.from)) stub._relayTextLive(wireText, event.from, rawInfo)
    }
  }
  private _onTextData(serialized: string, rawInfo: WirePublishInfo): void {
    const envelope = decodeSemanticEnvelope(serialized)
    if (!envelope) return
    if (envelope.__r === 'announce') return this._applyAnnouncement(envelope, serialized, rawInfo)
    if (envelope.__r !== 'data') return
    this._applyMemberData(envelope, rawInfo)
    this._relayMemberData(serialized, envelope, rawInfo)
  }
  private _onBinary(framed: Uint8Array, rawInfo: WirePublishInfo): void {
    const unframed = unframeMemberId(framed)
    if (!unframed) return // junk on the binary lane
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    if (!this._suppress(unframed.from))
      this._state.applyBinary(unframed.from, unframed.payload, unframed.track, unframed.meta, info)
    this._healUnknownSender(unframed.from)
    if (this._stubs.size > 0) {
      const wireData = encodePublishBinary(framed, rawInfo)
      const track = unframed.track ?? DEFAULT_TRACK
      for (const stub of this._stubs) {
        if (!stub._wantsBinary(unframed.from, track)) continue
        if (stub._selfSuppressed.has(unframed.from)) continue
        stub._relayBinaryLive(wireData, unframed.from, track, rawInfo)
      }
    }
  }
  /** A message on the inbox key of a member this instance owns — route it to the holder: a server-side participant's listeners, or the one client stub the member joined through. */
  private _onDm(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the inbox lane
    }
    if (!hasRoomTag(envelope)) return
    // A reply to one of our own `send(…, { ack: true })`s, riding our inbox back home.
    if (envelope.__r === 'dm-ack') return this._resolveDmAck(envelope as RoomDmAckEnvelope)
    if (envelope.__r !== 'dm') return
    const dm = envelope as RoomDmEnvelope
    const msg: InboxMessage = {
      from: dm.from,
      fromMeta: dm.fromMeta,
      fromIdentity: dm.fromIdentity ?? null,
      data: dm.data,
      ...(dm.ackId ? { ackId: dm.ackId } : {}),
    }
    const local = this._localParticipants.get(dm.to)
    if (local) {
      // A server-side participant, or one a client holds (its forwarder replies). Either way, for an ack DM we route the handler's reply back to the sender's inbox.
      if (dm.ackId) {
        void local
          ._deliverMessageAck(msg)
          .then((reply) => this._publishDmAck(dm.from, dm.ackId!, reply))
          .catch(reportRoomError)
      } else {
        local._deliverMessage(msg)
      }
      return
    }
    // A client held through a room stub — relay the DM (its `ackId` rides along); the client replies with `dm-reply`, which `_handleStubRequest` turns into the `dm-ack` above.
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) {
      if (!stub._stubMembers.has(dm.to)) continue
      if (dm.ackId) stub._recordAckDm(dm.ackId, dm.from, dm.to)
      stub._relayPublishText(wireText)
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
    this._rejectDmAcks('Recipient left the room before replying', id) // strand no waiter on a gone member
    const local = this._localParticipants.get(id)
    if (local) {
      this._localParticipants.delete(id)
      // A live-heartbeating owner can't be reaped (heartbeats outpace the TTL by 4x), so a vanished record with no observed event means the member was removed.
      local._onLeft(cause ?? { type: 'removed' })
    }
    for (const stub of this._stubs) {
      stub._stubMembers.delete(id)
      stub._selfSuppressed.delete(id)
      stub._forgetMember(id) // drop the departed member's retained-replay watermarks (bounded state)
    }
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
    this._rejectDmAcks('Room is closed') // no recipient will reply now
    this._teardownTail()
    for (const local of this._localParticipants.values()) local._onLeft({ type: 'closed' })
    this._localParticipants.clear()
    for (const stub of this._stubs) void stub.close().catch(() => {})
    this._syncSubs()
  }

  /** Recover a still-wanted terminal lane inside Room's one policy horizon. */
  private _onTerminalSubscription(slot: SubSlot, failure?: unknown): void {
    if (failure !== undefined) reportRoomError(failure)
    if (this._recoveringSubscriptions.has(slot)) return
    this._recoveringSubscriptions.add(slot)
    void this._recoverTerminalSubscription(slot)
      .catch(reportRoomError)
      .finally(() => this._recoveringSubscriptions.delete(slot))
  }
  private async _authorityStillOwnsSubscription(deadline: number): Promise<boolean> { const current = await withinRoomHorizon(getRoomBackend().readHead(this.id), deadline - Date.now()); return current !== null && current.head.state === 'open' && current.head.currentInc === this._inc }
  private async _retryTerminalSubscription(slot: SubSlot, deadline: number, attemptMs: number): Promise<'recovered' | 'retry' | 'exhausted'> {
    slot.retry()
    try {
      await withinRoomHorizon(slot.attemptReady, Math.min(attemptMs, deadline - Date.now()))
      await this._reconcileAuthority()
      return 'recovered'
    } catch (error) {
      reportRoomError(error)
      return Date.now() >= deadline ? 'exhausted' : 'retry'
    }
  }
  private async _recoverTerminalSubscription(slot: SubSlot): Promise<void> {
    const deadline = Date.now() + ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS
    const attemptMs = ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS / (ROOM_REPLAN_LIMIT + 1)
    for (let attempt = 0; attempt <= ROOM_REPLAN_LIMIT && slot.wanted; attempt++) {
      try {
        if (!(await this._authorityStillOwnsSubscription(deadline))) return this._settleTerminalSubscription()
      } catch (error) {
        reportRoomError(error)
        if (Date.now() >= deadline) break
      }
      const outcome = await this._retryTerminalSubscription(slot, deadline, attemptMs)
      if (outcome === 'recovered') return
      if (outcome === 'exhausted') break
    }
    if (!slot.wanted) return
    reportRoomError(new RoomError(`Room subscription recovery exhausted: ${this.id}`))
    slot.markLost()
  }
  private _settleTerminalSubscription(): void {
    if (this._state.closed) return
    this._state.applyClosed()
    this._teardown()
  }
  private async _reconcileAuthority(): Promise<void> {
    if (this._state.closed) return
    const current = await getRoomBackend().readHead(this.id)
    if (current === null || current.head.state !== 'open' || current.head.currentInc !== this._inc) {
      this._settleTerminalSubscription()
      return
    }
    const config = configFromHead(current.head)
    this._state.applyRoomUpdate(config.meta, config.at, config.by)
    await this._refreshMembers()
  }
  private _suppress(from: string): boolean {
    return this._localParticipants.get(from)?.selfDelivery === false
  }
  _startTail(): void {
    this._tail = true
    this._syncSubs() // bring up text ingestion before any stub exists
    this._tailTimer = unrefTimer(setTimeout(() => this._teardownTail(), ROOM_TAIL_ATTACH_TIMEOUT_MS))
  }
  private _teardownTail(): void {
    if (!this._tail) return // already handed off to a stub
    this._tail = false
    this._tailHold.length = 0
    if (this._tailTimer !== null) {
      clearTimeout(this._tailTimer)
      this._tailTimer = null
    }
    this._syncSubs() // drop the text ingestion nothing is consuming
  }
  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    if (this._tail) {
      if (this._tailTimer !== null) {
        clearTimeout(this._tailTimer)
        this._tailTimer = null
      }
      stub._beginTail(this._tailHold.slice(), () => this._syncSubs())
      this._tailHold.length = 0
      this._tail = false
    }
    stub.onOpen(() => {
      void this._ensureRoster()
        .then(() => {
          if (this._stubs.has(stub) && !this._state.closed)
            stub._relayRoster(this._state.snapshotMembers().filter((member) => !member.hidden))
        })
        .catch((error) => {
          reportRoomError(error)
          if (this._stubs.has(stub) && !this._state.closed) stub._relayRosterError()
        })
    })
    stub.onClose(() => {
      this._stubs.delete(stub)
      stub._endTail() // clear any pending tail hold/timer so a closed stub leaves nothing behind
      for (const id of [...stub._stubMembers.keys()])
        void this._removeMember(id, { type: 'disconnected' }).catch(reportRoomError)
      stub._stubMembers.clear()
      this._syncSubs()
    })
    this._syncSubs()
  }
  async _handleStubRequest(stub: RoomStubChannel, msg: unknown): Promise<unknown> {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as RoomStubRequest
    switch (req.__r) {
      case 'req-join':
        return await this._joinStubMember(stub, req)
      case 'req-leave':
        await this._removeMember(requireStubMember(stub, req.id), { type: 'left' })
        stub._stubMembers.delete(req.id as string)
        return undefined
      case 'req-set-meta':
        await this._setMemberMeta(requireStubMember(stub, req.id), isObject(req.meta) ? req.meta : {})
        return undefined
      case 'req-set-attrs':
        await this._mergeMemberMeta(requireStubMember(stub, req.id), isObject(req.attrs) ? req.attrs : {})
        return undefined
      case 'req-dm':
        return await this._sendStubDm(stub, req)
      case 'dm-reply':
        this._applyStubDmReply(stub, req)
        return undefined
      case 'sub-binary':
        this._applyStubBinaryWants(stub, req)
        return undefined
      case 'sub-text':
        this._applyStubTextWants(stub, req)
        return undefined
      default:
        return undefined
    }
  }
  private async _joinStubMember(stub: RoomStubChannel, req: Extract<RoomStubRequest, { __r: 'req-join' }>) {
    const meta = isObject(req.meta) ? req.meta : {}
    const { id, joinedAt } = await this._admitMember(meta, null, (id) => {
      stub._stubMembers.add(id)
      if (req.selfDelivery === false) stub._selfSuppressed.add(id)
    })
    return { id, joinedAt }
  }
  private async _sendStubDm(stub: RoomStubChannel, req: Extract<RoomStubRequest, { __r: 'req-dm' }>) {
    const id = requireStubMember(stub, req.id)
    if (!req.ack) return await this._publishDm(id, req.to, req.data)
    const { receipt, reply } = await this._sendDmAck(id, req.to, req.data)
    if (!reply.ok) throw roomFailureError(reply)
    return { ...receipt, response: reply.result }
  }
  private _applyStubDmReply(stub: RoomStubChannel, req: Extract<RoomStubRequest, { __r: 'dm-reply' }>): void {
    const sender = stub._takeAckDm(req.ackId, req.id)
    if (sender === undefined) return
    const reply: DmReply = req.ok
      ? { ok: true, result: req.result }
      : 'abort' in req ? { ok: false, abort: true, abortValue: req.abortValue } : { ok: false, err: req.err }
    void this._publishDmAck(sender, req.ackId, reply).catch(reportRoomError)
  }
  private _applyStubBinaryWants(stub: RoomStubChannel, req: Extract<RoomStubRequest, { __r: 'sub-binary' }>): void {
    const wants = sanitizeBinaryWants(req.wants)
    if (!wants) return reportRoomError(new RoomError('Malformed sub-binary declaration'))
    const prev = stub._binaryWants
    stub._binaryWants = wants
    this._syncSubs()
    void this._replayRetainedBinary(stub, prev).catch(reportRoomError)
  }
  private _applyStubTextWants(stub: RoomStubChannel, req: Extract<RoomStubRequest, { __r: 'sub-text' }>): void {
    const members = Array.isArray(req.members) ? req.members.filter((member) => typeof member === 'string') : []
    const prev = stub._textMemberWants
    stub._textMemberWants = new Set(members)
    stub._wantsAnnounce = req.announce === true
    stub._flushTail()
    this._syncSubs()
    void this._replayRetainedText(stub, stub._wantsText, prev).catch(reportRoomError)
  }
  _shieldPublishData(validate: ShieldValidator | undefined, data: unknown): void {
    if (!validate) return
    const result = validate(data)
    if (result !== true) throw new ShieldValidationError(result)
  }
  async _publishFromStub(
    stub: RoomStubChannel,
    payload: { text: string } | { binary: Uint8Array },
  ): Promise<ChannelPublishAck> {
    if ('text' in payload) {
      const envelope = parse(payload.text) as unknown
      if (!hasRoomTag(envelope) || envelope.__r !== 'data') throw new RoomError('Malformed room publish')
      const publish = envelope as RoomDataPublish
      requireStubMember(stub, publish.from)
      this._shieldPublishData(stub._publishShield, publish.data)
      return await this._publishText(publish.from, publish.data, publish.retain)
    }
    const from = requireStubMember(stub, binaryFrameSender(payload.binary))
    return await this._publishBinaryFramed(from, payload.binary)
  }

  async _replayRetainedText(
    stub: RoomStubChannel,
    prevWantsText: boolean,
    prevMemberWants: ReadonlySet<string>,
  ): Promise<void> {
    // Read retained only after subscription readiness: a racing commit is then retained or live, never lost in the gap.
    await withinRoomHorizon(this._textSub.ready, ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS)
    const stored = await getRoomBackend().readRetained(this.id, this._inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const info = { seq: stored.seq, timestamp: stored.timestamp }
    const envelope = parse(serialized) as RoomDataEnvelope
    if ((await readMembers(this.id, this._inc, [envelope.from])).length === 0) {
      await getRoomBackend().deleteRetained(this.id, this._inc, SEMANTIC_LANE, { ifSeq: stored.seq })
      return
    }
    if (prevWantsText || prevMemberWants.has(envelope.from) || !stub._wantsTextFrom(envelope.from)) return
    // Replay the stored order as-is; the stub dedupes a same-or-newer live winner.
    stub._emitRetainedText(encodePublishText(serialized, info), envelope.from, info)
  }

  async _replayRetainedBinary(stub: RoomStubChannel, prevWants: BinaryWants): Promise<void> {
    if (!wantsAnyBinary(stub._binaryWants)) return
    const roomWide = stub._binaryWants.everyMember
    if (roomWide.all || roomWide.tracks.length > 0) await this._ensureRoster()
    this._syncSubs()
    // Binary uses the same readiness handoff and stored receipt; its stub dedupes the live/retained race per lane.
    await this._binaryReady()
    const backend = getRoomBackend()
    const lanes = (await backend.listRetained(this.id, this._inc)).filter(
      (lane): lane is Extract<LaneId, { kind: 'binary' }> => lane.kind === 'binary',
    )
    const owners = new Set(
      (await readMembers(this.id, this._inc, [...new Set(lanes.map((lane) => lane.member))])).map(({ id }) => id),
    )
    for (const lane of lanes) {
      const stored = await backend.readRetained(this.id, this._inc, lane)
      if (stored === null) continue
      if (!owners.has(lane.member)) {
        await backend.deleteRetained(this.id, this._inc, lane, { ifSeq: stored.seq })
        continue
      }
      const framed = stored.payload
      const frame = unframeMemberId(framed)
      if (!frame) continue
      const track = frame.track ?? DEFAULT_TRACK
      if (binaryWantsCovers(prevWants, frame.from, track) || !stub._wantsBinary(frame.from, track)) continue
      const info: WirePublishInfo = { seq: stored.seq, timestamp: stored.timestamp }
      stub._emitRetainedBinary(encodePublishBinary(framed, info), frame.from, track, info)
    }
  }

  _syncSubs(): void {
    const backend = getRoomBackend()
    const state = this._state
    const open = !state.closed
    const observed = this._stubs.size > 0 || this._localParticipants.size > 0 || state.listenerCount > 0

    const becomesObserved = open && observed && !this._ctrlSub.active
    this._ctrlSub.sync(open && observed, () =>
      backend.subscribeLane(this.id, this._inc, CONTROL_LANE, (payload, info) =>
        this._onCtrlMessage(decodeRoomText(payload), info),
      ),
    )

    const textWants = this._aggregateTextWants()
    const wantAnyText = open && (textWants.all || textWants.members.size > 0)
    const wantAnnounce = state.wantsAnnounce || [...this._stubs].some((stub) => stub._wantsAnnounce)
    const wantSemantic = open && (wantAnyText || wantAnnounce)
    const memberIds = open ? state.listMemberIds() : []

    // Rosters refresh when observation resumes or a listener/member lane needs them; join alone stays lazy.
    const binaryWants = this._aggregateBinaryWants()
    const wantAnyBinary = open && wantsAnyBinary(binaryWants)
    const needsRoster = state.listenerCount > 0 || wantAnyBinary
    if ((becomesObserved && state.rosterKnown) || (open && !state.rosterKnown && needsRoster)) {
      void this._refreshMembers().catch(reportRoomError)
    }
    this._textSub.sync(wantSemantic, () =>
      backend.subscribeLane(this.id, this._inc, SEMANTIC_LANE, (payload, info) =>
        this._onTextData(decodeRoomText(payload), info),
      ),
    )

    const binaryPairs = open ? this._binaryPairs(binaryWants, memberIds) : []
    this._syncKeyedSubs(this._binaryKeyUnsubs, wantAnyBinary ? this._binaryLanes(binaryPairs) : [], (lane) =>
      backend.subscribeLane(this.id, this._inc, lane, (framed, info) => this._onBinary(framed, info)),
    )

    if (binaryPairs.length === 0) this._demand.sync([])
    else {
      void this._binaryReady()
        .then(() => {
          const currentWants = this._aggregateBinaryWants()
          this._demand.sync(this._state.closed ? [] : this._binaryPairs(currentWants, this._state.listMemberIds()))
        })
        .catch(reportRoomError)
    }

    this._syncKeyedSubs(
      this._dmUnsubs,
      open ? this._ownedMemberIds().map((member) => ({ key: member, value: { kind: 'inbox', member } as const })) : [],
      (lane) =>
        backend.subscribeLane(this.id, this._inc, lane, (payload, info) => this._onDm(decodeRoomText(payload), info)),
    )

    this._syncHeartbeat()
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

  private _binaryPairs(wants: BinaryWants, memberIds: string[]): Array<[string, string]> {
    const pairs: Array<[string, string]> = []
    for (const memberId of new Set([...memberIds, ...Object.keys(wants.members)])) {
      const memberWants = wants.members[memberId]
      const eff = memberWants ? mergeTrackWants(wants.everyMember, memberWants) : wants.everyMember
      const tracks = eff.all ? [DEFAULT_TRACK, ...this._state.memberTracks(memberId)] : eff.tracks
      for (const track of tracks) pairs.push([memberId, track])
    }
    return pairs
  }

  private _ownsMember(id: string): boolean {
    if (this._localParticipants.has(id)) return true
    for (const stub of this._stubs) if (stub._stubMembers.has(id)) return true
    return false
  }

  private _deliverDemand(member: string, track: string, wanted: boolean): void {
    const trackOut = track === DEFAULT_TRACK ? null : track
    const local = this._localParticipants.get(member)
    if (local) {
      local._onDemand(trackOut, wanted)
      return
    }
    for (const stub of this._stubs) {
      if (stub._stubMembers.has(member)) {
        stub._relayDemand({ __r: 'demand', member, track: trackOut, wanted })
        return
      }
    }
  }

  private _aggregateTextWants(): { all: boolean; members: Set<string> } {
    if (this._tail) return { all: true, members: new Set() } // pre-attach tail: ingest everything now
    const local: MemberWants = this._state.textWants()
    if (local.all) return { all: true, members: new Set() }
    const members = new Set(local.members)
    for (const stub of this._stubs) {
      // A tail-pending stub ingests everything so its hold captures the whole recent tail; the selector is applied at flush, not here (the want isn't known until the client subscribes).
      if (stub._wantsText || stub._tailPending !== null) return { all: true, members: new Set() }
      for (const id of stub._textMemberWants) members.add(id)
    }
    return { all: false, members }
  }

  private _syncKeyedSubs<T>(
    subs: Map<string, SubSlot>,
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
      if (!slot) {
        slot = new SubSlot(
          (terminal, error) => this._onTerminalSubscription(terminal, error),
          () => void this._reconcileAuthority().catch(reportRoomError),
        )
        subs.set(key, slot)
      }
      slot.sync(true, () => subscribe(value))
    }
  }

  private _binaryReady(): Promise<void> {
    const pending: Promise<void>[] = []
    for (const subscription of this._binaryKeyUnsubs.values()) pending.push(subscription.ready)
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
    this._pendingRefresh ??= (async () => {
      try {
        for (let attempt = 0; !this._state.closed; attempt++) {
          const version = this._state.membershipVersion
          const members = await readMembers(this.id, this._inc)
          if (this._state.membershipVersion !== version) {
            if (attempt === ROOM_REPLAN_LIMIT) throw new RoomError(`Room roster refresh contention: ${this.id}`)
            continue
          }
          const drifted = this._state.reconcileCompleteRoster(members)
          this._syncSubs() // per-member lanes may need subscriptions for the members just learned
          if (drifted) {
            for (const stub of this._stubs) stub._relayRoster(this._state.snapshotMembers().filter((m) => !m.hidden))
          }
          return
        }
      } finally {
        this._pendingRefresh = null
      }
    })()
    return this._pendingRefresh
  }

  // Graceful departures use events; heartbeats refresh owner `seenAt` and reap records orphaned by hard crashes.

  private _ownedMemberIds(): string[] {
    const owned = [...this._localParticipants.keys()]
    for (const stub of this._stubs) owned.push(...stub._stubMembers.keys())
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
      try {
        // Renew this node's binary-demand lease on every owner and sweep any crashed reporter's demand.
        // No cell I/O — runs first so member-cell latency never delays it (the demand TTL has slack for skips).
        this._demand.heartbeat()
        let renewalFailure: { error: unknown } | null = null
        for (const id of this._ownedMemberIds().filter((id) => !this._pendingAdmissions.has(id))) {
          try {
            const key = roomMemberKvKey(this.id, id)
            const present = await mutateCells(this.id, this._inc, { keys: [key] }, (cells) => {
              const raw = cells.get(key)
              if (raw === undefined) return { value: false, mutations: [] }
              const record = { ...(parse(decodeRoomText(raw)) as RoomMemberRecord), seenAt: Date.now() }
              return {
                value: true,
                mutations: [{ key, set: { bytes: encodeRoomText(stringify(record)) } }],
              }
            })
            if (!present) this._applyLeave(id)
          } catch (error) {
            if (error instanceof RoomError && error.message === `Room is closed: ${this.id}`) throw error
            renewalFailure ??= { error }
          }
        }
        if (renewalFailure) throw renewalFailure.error
      } finally {
        // Authority diagnosis owns convergence after a missed close. Renewal failure cannot bypass it.
        await this._reconcileAuthority()
      }
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
    const toId = typeof to === 'string' ? to : to.id
    if (!options?.ack) return this._room._publishDm(this.id, toId, data)
    const { receipt, reply } = await this._room._sendDmAck(this.id, toId, data)
    if (!reply.ok) throw roomFailureError(reply)
    return { ...receipt, response: reply.result } satisfies RoomAckReceipt
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
    reportRoomError(err)
  }
}

async function runAfterHook(hook: () => unknown): Promise<void> {
  try {
    await hook()
  } catch (error) {
    reportRoomError(error)
  }
}
