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
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../../channel.js'
import {
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
} from '../../constants.js'
import { getBackend } from '../../backend/install.js'
import type { LaneId, BackendSubscription } from '../../backend/spi.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../../shared-ws.js'
import {
  RoomError,
  roomFailureError,
  leaveCauseFromWire,
  leaveCauseToWire,
  frameWithMemberId,
  binaryFrameSender,
  hasRoomTag,
  mergeAttributes,
  normalizeJoinOptions,
  roomMemberKvKey,
  roomHiddenMemberKvKey,
  roomIdentityMemberKvKey,
  unframeMemberId,
  DEFAULT_TRACK,
  emptyTrackWants,
  mergeTrackWants,
  binaryWantsCovers,
  sanitizeBinaryWants,
  pushBoundedTail,
  wantsAnyBinary,
  type BinaryWants,
  type MemberWants,
  type TrackWants,
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
import { RoomState } from '../state.js'
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
  reportRoomError,
  withinRoomHorizon,
} from './lanes.js'
import { dropRetainedOwnedBy, evictMember, mutateCells, readCell, readMembers } from './membership.js'
import type {
  BinaryFrameInfo,
  BinaryPublishOptions,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  RoomMeta,
  RoomSendReceipt,
  RoomAckReceipt,
  RoomSnapshotView,
  Sender,
} from '../types.js'
import type { Room, RoomGuards } from './statics.js'
assertIsNotBrowser()

const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')

/**
 * Server-side `Room`.
 *
 * Not a channel itself: serializing a `ServerRoom` attaches a fresh `RoomStubChannel` per
 * response (see `roomReplacer`), so the same instance can be serialized any number of times
 * and `room.id` stays the room ID (wire channel IDs must be globally unique).
 *
 * State changes flow through backend lanes: the node causing a change applies it locally and commits
 * it; every observing node — including the committer's own echo — applies the subscribed payload.
 * Application is idempotent, so the overlap is harmless.
 */
class ServerRoom implements Room {
  readonly [SERVER_ROOM_BRAND] = true
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }

  /** @internal — the room incarnation id (`RoomConfigRecord.inc`) this handle is bound to. Every
   *  authority-side legality check (join, mutate, close) and every member record this handle writes
   *  carries it, so an operation from a previous incarnation is rejected after a close/recreate. */
  readonly _inc: string
  /** @internal — tail mode (`Room.get(id, { tail: true })`): this node ingests and holds the room's
   *  text from the moment of `Room.get`, so a history read done before the room is serialized misses
   *  no live message. Cleared when a stub attaches (the hold moves onto the stub, which keeps it until
   *  the client's first `subscribe()`) or when the safety timer tears an unserialized tail down. */
  _tail = false
  /** Text held between `Room.get({ tail })` and the stub attaching; handed to the stub on attach (see
   *  `_attachStub`). Bounded drop-oldest, so a fetched-with-tail room that is never serialized (misuse)
   *  can't grow it without limit. */
  private readonly _tailHold: Array<{ serialized: string; ord: RoomOrder; from: string }> = []
  private _tailTimer: ReturnType<typeof setTimeout> | null = null
  /** In-flight `send(…, { ack: true })`s awaiting the recipient's reply, keyed by `ackId`. `to` is
   *  the recipient, so a leave/close can fail the ones it strands. Empty at steady state. */
  private readonly _pendingDmAcks = new Map<string, { to: string; settle: (reply: DmReply) => void }>()
  private _guards: RoomGuards | null = null
  /** @internal */ readonly _state: RoomState
  private readonly _stubs = new Set<RoomStubChannel>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()

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
  /** (member, track) pairs this instance has already announced — first publish pays the
   *  KV append + ctrl event, every further frame is a Set lookup. */
  private readonly _announcedTracks = new Map<string, Set<string>>()
  /** Cross-node binary-demand aggregation (`onDemand`) — constructed once `roomId` and the
   *  ownership/delivery callbacks are available (see the constructor). */
  private readonly _demand: RoomDemand
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _heartbeatBusy = false
  private _pendingRefresh: Promise<void> | null = null
  private readonly _recoveringSubscriptions = new Set<SubSlot>()
  private _controlSeq = 0

  constructor(roomId: string, config: RoomConfigRecord, seed: { members: MemberSnapshot[] } | { count: number }) {
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

  get id(): string {
    return this._state.roomId
  }
  get meta(): RoomMeta {
    return this._state.meta
  }
  get count(): number {
    return this._state.count
  }
  get isEmpty(): boolean {
    return this._state.count === 0
  }
  get isClosed(): boolean {
    return this._state.closed
  }

  async join(options?: JoinOptions): Promise<LocalParticipant> {
    const { meta, selfDelivery } = normalizeJoinOptions(options)
    const identity = normalizeIdentity(options)
    const hidden = normalizeHidden(options)
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

  subscribe(callback: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._state.subscribe(callback)
  }
  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    options?: { track?: string },
  ): () => void {
    return this._state.subscribeBinary(callback, options)
  }
  onJoin(callback: (member: RemoteParticipant) => void): () => void {
    return this._state.onJoin(callback)
  }
  onLeave(callback: (member: RemoteParticipant, cause?: LeaveCause) => void): () => void {
    return this._state.onLeave(callback)
  }
  onParticipantUpdate(
    callback: (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void,
  ): () => void {
    return this._state.onParticipantUpdate(callback)
  }
  onUpdate(callback: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this._state.onUpdate(callback)
  }
  onEmpty(callback: () => void): () => void {
    return this._state.onEmpty(callback)
  }
  onClose(callback: () => void): () => void {
    return this._state.onClose(callback)
  }
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._state.onAnnounce(callback)
  }

  // Arrow-valued, not prototype methods: the documented `useSyncExternalStore(room.onChange, room.snapshot)`
  // passes both detached (React calls them with no receiver), so they must stay bound to survive it.
  onChange = (callback: () => void): (() => void) => this._state.onChange(callback)

  snapshot = (): RoomSnapshotView => {
    // Snapshot consumers want the member view — load it (need-driven, single-flight); the
    // arrival lands as an onChange, and the next snapshot() is complete.
    if (!this._state.rosterKnown) void this._ensureRoster().catch(reportRoomError)
    return this._state.snapshot()
  }

  /** Join choreography shared by local `join()` and stub `req-join`. `track` registers the
   *  holder first — the member must count as owned before `_syncSubs()` brings up its inbox
   *  subscription and heartbeat, and before its join is announced. */
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
    track(id)
    this._syncSubs()
    let created = false
    try {
      const inbox = this._dmUnsubs.get(id)
      assert(inbox)
      await Promise.all([this._textSub.ready, inbox.ready])
      await this._createMember(id, meta, identity, joinedAt, hidden)
      created = true
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
      inc: this._inc,
      metaSeq: 0,
      ...(identity === null ? {} : { identity }),
      ...(hidden ? { hidden: true } : {}),
    }
    const memberKey = roomMemberKvKey(this.id, id)
    const keys = [memberKey]
    if (identity !== null) keys.push(roomIdentityMemberKvKey(this.id, identity, id))
    if (hidden) keys.push(roomHiddenMemberKvKey(this.id, id))
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
    const hidden = this._state.isHidden(id)
    const keys = [roomMemberKvKey(this.id, id)]
    if (identity !== null) keys.push(roomIdentityMemberKvKey(this.id, identity, id))
    if (hidden) keys.push(roomHiddenMemberKvKey(this.id, id))
    await mutateCells(this.id, this._inc, { keys }, () => ({
      value: undefined,
      mutations: keys.map((key) => ({ key })),
    }))
    await dropRetainedOwnedBy(this.id, this._inc, id)
    this._applyLeave(id, cause)
    await publishCtrl(this.id, this._inc, { __r: 'leave', id, ...leaveCauseToWire(cause) })
  }

  /** @internal — full replace (`setMeta`). */
  async _setMemberMeta(id: string, meta: ParticipantMeta): Promise<void> {
    assertUsage(isObject(meta), 'setMeta() meta should be an object')
    await this._writeMemberMeta(id, () => meta)
  }

  /** @internal — per-key merge (`setAttributes`); an `undefined` value deletes the key. */
  async _mergeMemberMeta(id: string, attrs: ParticipantMeta): Promise<void> {
    assertUsage(isObject(attrs), 'setAttributes() attributes should be an object')
    await this._writeMemberMeta(id, (current) => mergeAttributes(current, attrs))
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
    await publishCtrl(this.id, this._inc, { __r: 'p-meta', id, meta, seq })
  }

  /** @internal — publish a member's text message. The sender's verified meta/identity are stamped
   *  into the envelope here — never client-supplied. Text rides the room's semantic lane. `retain`
   *  stores the message as the room's one retained-text slot (MQTT-style), replayed to any later
   *  text subscriber (see `_replayRetainedText`). */
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

  /** @internal — publish a member's binary frame (`[16-byte member ID][flags][…]`, validated at
   *  its entry point — the unframe cannot fail). Binary rides per-publisher lanes — per
   *  (publisher, track) for named tracks: that's what makes delivery track-selective at the
   *  source. When the backend can count globally, `receivers: 0` truthfully means "nobody anywhere
   *  wants this track"; backends that cannot know omit `receivers`. */
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

  /** Shared publish prologue: open check + `onBeforePublish` guard, on the verified sender. */
  private async _admitPublish(from: string, payload: unknown): Promise<Sender> {
    if (this._state.closed) throw new RoomError(`Room is closed: ${this.id}`)
    const sender = this._memberSender(from)
    const onBeforePublish = this._guards?.onBeforePublish
    if (onBeforePublish) await onBeforePublish(sender, payload)
    return sender
  }

  /** Shared publish epilogue: the receipt (with `receivers`) plus the `onAfterPublish` hook. */
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

  /** First frame on a new (member, track): record the track on the member's cell (late observers
   *  discover it from the roster) and announce it on the control lane (live all-track subscribers
   *  bring the lane subscription up) — both strictly before the frame. Idempotent across owner
   *  incarnations via the cell record; O(1) per further frame via `_announcedTracks`. */
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

  /** The verified sender as this node knows it — own participants first (freshest), then the
   *  view. The one place sender identity is assembled; guards and envelopes both consume it. */
  private _memberSender(from: string): Sender {
    const local = this._localParticipants.get(from)
    if (local) return { id: from, meta: local.meta, identity: local.identity }
    const remote = this._state.getRemote(from)
    return { id: from, meta: remote?.meta ?? {}, identity: remote?.identity ?? null }
  }

  /** @internal — send a private message: published on the target's inbox key, which only
   *  the target's owning node subscribes to (see `_onDm`). The sender's verified meta rides
   *  the envelope so every receiver can surface a rich sender. */
  async _sendDm(from: string, to: string, data: unknown): Promise<RoomSendReceipt> {
    return this._publishDm(from, to, data)
  }

  /** @internal — `send(…, { ack: true })`: publish the DM tagged with an `ackId`, then wait for the
   *  recipient's node to route the handler's reply back on our own inbox (`_onDm` → `_resolveDmAck`).
   *  Returns the send receipt plus the recipient's `DmReply` — a success value, or a failure the
   *  caller surfaces (the handler's `Abort`, the recipient leaving, the room closing). The outbound
   *  publish itself still throws (an invalid send never reaches a recipient to reply). */
  async _sendDmAck(from: string, to: string, data: unknown): Promise<{ receipt: RoomSendReceipt; reply: DmReply }> {
    const ackId = crypto.randomUUID()
    let timer: ReturnType<typeof setTimeout> | undefined
    const reply = new Promise<DmReply>((settle) => {
      this._pendingDmAcks.set(ackId, { to, settle })
      // The recipient replying/leaving/overflowing settles this promptly; this bounds the one case
      // none of those cover — a recipient that joined but never listens and never leaves.
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

  /** Shared DM publish: validate the target, run the send guards, stamp the verified sender, and
   *  publish on the target's inbox key. `ackId` rides the envelope when a reply is awaited. */
  private async _publishDm(from: string, to: string, data: unknown, ackId?: string): Promise<RoomSendReceipt> {
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

  /** @internal — publish an `{ ack: true }` reply back to the sender's inbox (`to` is the sender). */
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

  /** @internal — the recipient replied: settle the matching pending `send(…, { ack: true })` with
   *  its `DmReply`, verbatim — the sender's caller reconstructs success or failure from it. */
  private _resolveDmAck(envelope: RoomDmAckEnvelope): void {
    const pending = this._pendingDmAcks.get(envelope.ackId)
    if (!pending) return
    this._pendingDmAcks.delete(envelope.ackId)
    pending.settle(envelope)
  }

  /** @internal — fail any pending ack whose recipient just left (or, on close, all of them). This
   *  is an expected operational outcome, not a bug, so it carries a visible reason (`err`). */
  private _rejectDmAcks(message: string, to?: string): void {
    for (const [ackId, pending] of this._pendingDmAcks) {
      if (to !== undefined && pending.to !== to) continue
      this._pendingDmAcks.delete(ackId)
      pending.settle({ ok: false, err: message })
    }
  }

  /** The member's live view — falling back to the authoritative backend cell, since the local
   *  view lags while unobserved (and briefly after the observe transition, until the cell
   *  resync lands). */
  private async _resolveMember(id: string): Promise<Sender | null> {
    const remote = this._state.getRemote(id)
    if (remote) return remote
    const raw = await readCell(this.id, this._inc, roomMemberKvKey(this.id, id))
    if (raw === null) return null
    const record = parse(decodeRoomText(raw)) as RoomMemberRecord
    return { id, meta: record.meta, identity: record.identity ?? null }
  }

  /** The room's authoritative config iff it is `open` at this handle's incarnation — the legality
   *  oracle for every mutation. A `null` result means closed/closing/gone or another incarnation. */
  private async _openConfig(): Promise<RoomConfigRecord | null> {
    const current = await getBackend().readHead(this.id)
    if (current === null || current.head.state !== 'open' || current.head.currentInc !== this._inc) return null
    return configFromHead(current.head)
  }

  private async _assertOpen(): Promise<void> {
    if (this._state.closed || (await this._openConfig()) === null) {
      throw new RoomError(`Room is closed: ${this.id}`)
    }
  }

  /** Decode a failed atomic member fence into the caller-facing operational reason. The commit has
   *  already rejected without advancing order or delivering, so these reads are diagnostic only. */
  private async _throwStaleMembers(...ids: string[]): Promise<never> {
    await this._assertOpen()
    for (const id of ids) {
      if ((await readCell(this.id, this._inc, roomMemberKvKey(this.id, id))) === null) {
        throw new RoomError(`Participant not found (left?): ${id}`)
      }
    }
    throw new RoomError(`Room is closed: ${this.id}`)
  }

  /** The control lane: presence and lifecycle events — relayed to every stub
   *  unconditionally, since a client's live view is only correct if it sees every one. */
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

  /** Whether a control event concerns a hidden (server-only) member and so must not be relayed to
   *  clients. The event still rides the control lane between servers — that is how every server's
   *  projection converges on a hidden member — but it stops at the stub boundary, so a hidden member
   *  never enters a client's roster or its presence narration (see `getParticipants({ hidden })`). */
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

  /** The text data lane — relayed per stub, skipping the sender's own holder when it opted out. */
  private _onTextData(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the semantic lane
    }
    if (!hasRoomTag(envelope)) return
    if (envelope.__r === 'announce') {
      const announce = envelope as Extract<RoomEnvelope, { __r: 'announce' }>
      const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
      this._state.applyAnnounce(announce.data, info)
      if (this._stubs.size > 0) {
        const wireText = encodePublishText(serialized, rawInfo)
        for (const stub of this._stubs) {
          if (stub._wantsAnnounce) stub._relayTextLive(wireText, '', rawInfo)
        }
      }
      return
    }
    if (envelope.__r !== 'data') return
    const event = envelope as RoomDataEnvelope
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    this._state.applyData(
      event.from,
      event.fromMeta,
      event.fromIdentity ?? null,
      event.data,
      info,
      this._suppress(event.from),
    )
    this._healUnknownSender(event.from)

    if (this._stubs.size > 0) {
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) {
        // A stub still awaiting its client's first text want holds the message server-side (bounded,
        // drop-oldest) instead of relaying — flushed selectively once the want declares the selector.
        if (stub._tailPending !== null) {
          stub._holdTail(serialized, rawInfo, event.from)
          continue
        }
        if (!stub._wantsTextFrom(event.from)) continue
        if (stub._selfSuppressed.has(event.from)) continue
        stub._relayTextLive(wireText, event.from, rawInfo)
      }
    } else if (this._tail) {
      // Tail, pre-attach: no stub yet, but `Room.get({ tail })` opened ingestion — hold the message so
      // the stub inherits it on attach. Bounded by count and total size; the client dedupes any overlap.
      pushBoundedTail(this._tailHold, { serialized, ord: rawInfo, from: event.from })
    }
  }

  private _onBinary(framed: Uint8Array, rawInfo: WirePublishInfo): void {
    const unframed = unframeMemberId(framed)
    if (!unframed) return // junk on the binary lane
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    this._state.applyBinary(
      unframed.from,
      unframed.payload,
      unframed.track,
      unframed.meta,
      info,
      this._suppress(unframed.from),
    )
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

  /** A message on the inbox key of a member this instance owns — route it to the holder:
   *  a server-side participant's listeners, or the one client stub the member joined through. */
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
      // A server-side participant, or one a client holds (its forwarder replies). Either way,
      // for an ack DM we route the handler's reply back to the sender's inbox.
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
    // A client held through a room stub — relay the DM (its `ackId` rides along); the client
    // replies with `dm-reply`, which `_handleStubRequest` turns into the `dm-ack` above.
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
        const local = this._localParticipants.get(event.id)
        if (local) local._meta = event.meta
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
      // A live-heartbeating owner can't be reaped (heartbeats outpace the TTL by 4x), so a
      // vanished record with no observed event means the member was removed.
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

  /** The room closed — runs once, after the `closed` event has been applied and relayed. */
  private _teardown(): void {
    this._rejectDmAcks('Room is closed') // no recipient will reply now
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
    void (async () => {
      const deadline = Date.now() + ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS
      while (slot.wanted && Date.now() < deadline) {
        try {
          const current = await withinRoomHorizon(getBackend().readHead(this.id), deadline - Date.now())
          if (current === null || current.head.state !== 'open' || current.head.currentInc !== this._inc) {
            this._settleTerminalSubscription()
            return
          }
        } catch (error) {
          reportRoomError(error)
          if (Date.now() >= deadline) break
        }
        slot.retry()
        try {
          await withinRoomHorizon(slot.attemptReady, Math.min(1_000, deadline - Date.now()))
          await this._reconcileAuthority()
          return
        } catch (error) {
          reportRoomError(error)
        }
        const remaining = deadline - Date.now()
        if (remaining > 0) {
          await new Promise<void>((resolve) => {
            unrefTimer(setTimeout(resolve, Math.min(100, remaining)))
          })
        }
      }
      if (slot.wanted) {
        reportRoomError(new RoomError(`Room subscription recovery exhausted: ${this.id}`))
        slot.markLost()
      }
    })()
      .catch(reportRoomError)
      .finally(() => this._recoveringSubscriptions.delete(slot))
  }

  private _settleTerminalSubscription(): void {
    if (this._state.closed) return
    this._state.applyClosed()
    this._teardown()
  }

  private async _reconcileAuthority(): Promise<void> {
    if (this._state.closed) return
    const current = await getBackend().readHead(this.id)
    if (current === null || current.head.state !== 'open' || current.head.currentInc !== this._inc) {
      this._settleTerminalSubscription()
      return
    }
    const config = configFromHead(current.head)
    this._state.applyRoomUpdate(config.meta, config.at, config.by)
    await this._refreshMembers()
  }

  /** A member's own messages are suppressed for this holder when its participant opted out. */
  private _suppress(from: string): boolean {
    return this._localParticipants.get(from)?.selfDelivery === false
  }

  // ── Client stubs ──

  /** @internal — begin tail relay (`Room.get({ tail: true })`): ingest and hold the room's text from
   *  now, so a history read done before this room is serialized misses no live message. A safety timer
   *  tears the ingestion down if the room is fetched-with-tail but never serialized. */
  _startTail(): void {
    this._tail = true
    this._syncSubs() // bring up text ingestion before any stub exists
    this._tailTimer = setTimeout(() => this._teardownTail(), ROOM_TAIL_ATTACH_TIMEOUT_MS)
  }

  private _teardownTail(): void {
    if (!this._tail) return // already handed off to a stub
    this._tail = false
    this._tailHold.length = 0
    this._tailTimer = null
    this._syncSubs() // drop the text ingestion nothing is consuming
  }

  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    // Tail mode (`Room.get(id, { tail: true })`): hand the pre-attach hold to the stub, which keeps
    // holding server-side (still ingesting, gate closed) until the client's first text want declares
    // the selector — then it flushes the selected messages once, in order, ahead of the live stream
    // (see `_flushTail`). Nothing crosses the wire before the client asks for it, and the client needs
    // no buffer of its own. `_syncSubs()` below keeps text ingestion up while the hold is pending.
    if (this._tail) {
      if (this._tailTimer !== null) {
        clearTimeout(this._tailTimer)
        this._tailTimer = null
      }
      stub._beginTail(this._tailHold.slice(), () => this._syncSubs())
      this._tailHold.length = 0
      this._tail = false
    }
    // The snapshot carries only scalars; push the roster once the peer is attached. Committed
    // publish frames enter the stub's ReplayBuffer, so reconnect recovers this without rerunning
    // onOpen. Failure is an explicit replayable event so client roster getters always settle.
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
      // The client is gone — presence says its members leave.
      for (const id of [...stub._stubMembers.keys()])
        void this._removeMember(id, { type: 'disconnected' }).catch(reportRoomError)
      stub._stubMembers.clear()
      this._syncSubs()
    })
    this._syncSubs()
  }

  /** @internal — requests arriving on a room stub channel. The return value is the ack. */
  /** Handle a client request on a `Room` stub. The ack-bearing requests (join/leave/set-meta/
   *  set-attrs/dm) return their raw success value or *throw*: the stub's ack encoder (`roomAckError`,
   *  set in `RoomStubChannel`) maps the throw onto the channel's native `ABORT`/`ERROR` status, so
   *  the client's awaiting request rejects with the reconstructed `AbortError`/`Error` — no envelope.
   *  The fire-and-forget requests (dm-reply, sub-binary, sub-text) ride no ack and must never throw. */
  async _handleStubRequest(stub: RoomStubChannel, msg: unknown): Promise<unknown> {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as RoomStubRequest
    switch (req.__r) {
      case 'req-join': {
        const meta = isObject(req.meta) ? req.meta : {}
        // Identity is trusted and therefore server-assigned — a client join never carries one.
        const { id, joinedAt } = await this._admitMember(meta, null, (id) => {
          stub._stubMembers.add(id)
          if (req.selfDelivery === false) stub._selfSuppressed.add(id)
        })
        return { id, joinedAt }
      }
      case 'req-leave':
        this._assertStubMember(stub, req.id)
        await this._removeMember(req.id, { type: 'left' })
        stub._stubMembers.delete(req.id)
        return undefined
      case 'req-set-meta':
        this._assertStubMember(stub, req.id)
        await this._setMemberMeta(req.id, isObject(req.meta) ? req.meta : {})
        return undefined
      case 'req-set-attrs':
        this._assertStubMember(stub, req.id)
        await this._mergeMemberMeta(req.id, isObject(req.attrs) ? req.attrs : {})
        return undefined
      case 'req-dm': {
        this._assertStubMember(stub, req.id)
        if (!req.ack) return await this._sendDm(req.id, req.to, req.data)
        const { receipt, reply } = await this._sendDmAck(req.id, req.to, req.data)
        // The recipient's failure rides home too — throw it so the ack encoder emits the native
        // ABORT/ERROR (a re-catch here would reclassify a carried Abort/RoomError as an opaque bug).
        if (!reply.ok) throw roomFailureError(reply)
        return { ...receipt, response: reply.result }
      }
      case 'dm-reply': {
        // A client-held member replied to an ack DM we relayed it — route the reply to the sender.
        // Fire-and-forget: only an `ackId` we relayed to this stub, replied to by the very member we
        // relayed it to and not yet expired, is honored (`_takeAckDm` is the guard) — a forged,
        // misattributed, or stale reply matches nothing, so nothing here throws onto the (unacked) wire.
        const sender = stub._takeAckDm(req.ackId, req.id)
        if (sender !== undefined) {
          // Rebuild a clean `DmReply` from the spread fields (dropping `__r`/`id`/`ackId`),
          // preserving whether it's a value, a carried `Abort`, or an operational error.
          const reply: DmReply = req.ok
            ? { ok: true, result: req.result }
            : 'abort' in req
              ? { ok: false, abort: true, abortValue: req.abortValue }
              : { ok: false, err: req.err }
          void this._publishDmAck(sender, req.ackId, reply).catch(reportRoomError)
        }
        return undefined
      }
      case 'sub-binary': {
        const wants = sanitizeBinaryWants(req.wants)
        // Fire-and-forget: a malformed declaration is a client bug — report it, don't act on it.
        if (!wants) {
          reportRoomError(new RoomError('Malformed sub-binary declaration'))
          return undefined
        }
        const prev = stub._binaryWants
        stub._binaryWants = wants
        this._syncSubs()
        void this._replayRetainedBinary(stub, prev).catch(reportRoomError)
        return undefined
      }
      case 'sub-text': {
        // Member-scoped text and announce wants — room-level text rides the broadcast-sub ctrl.
        const members = Array.isArray(req.members) ? req.members.filter((m) => typeof m === 'string') : []
        const prev = stub._textMemberWants
        stub._textMemberWants = new Set(members)
        stub._wantsAnnounce = req.announce === true
        stub._flushTail() // this selector newly covers held tail — flush it before live/retained relay
        this._syncSubs()
        void this._replayRetainedText(stub, stub._wantsText, prev).catch(reportRoomError)
        return undefined
      }
      default:
        return undefined
    }
  }

  /** @internal — validate a client publish payload against the room's declared message type (`Pub`),
   *  auto-generated from the type (see `RoomShield`). `validate` is the room's `data` shield, carried on
   *  the ingress channel's own `_publishShield` slot — deliberately *not* the channel's `_validators` map,
   *  which the base channel runs against every request envelope (`_dispatchAckReq`): a room stub multiplexes
   *  join/leave/dm through that path, so the payload is shielded explicitly here, not as a request side effect.
   *  A fail throws a `ShieldValidationError` the ack encoder turns into a `SHIELD_ERROR`. A no-op when nothing
   *  declared `Pub` (`unknown` generates no verifier). Both publish ingresses (room stub, standalone-participant
   *  stub) run it before the payload is admitted. */
  _shieldPublishData(validate: ShieldValidator | undefined, data: unknown): void {
    if (!validate) return
    const result = validate(data)
    if (result !== true) throw new ShieldValidationError(result)
  }

  /** @internal — a client publish arriving on a room stub. The client's envelope is only a
   *  claim: membership is validated against this stub, and the publish path re-stamps the verified
   *  `fromMeta` — nothing client-supplied reaches the room stream except the payload itself. */
  async _publishFromStub(
    stub: RoomStubChannel,
    payload: { text: string } | { binary: Uint8Array },
  ): Promise<ChannelPublishAck> {
    if ('text' in payload) {
      const envelope = parse(payload.text) as unknown
      if (!hasRoomTag(envelope) || envelope.__r !== 'data') throw new RoomError('Malformed room publish')
      const publish = envelope as RoomDataPublish
      this._assertStubMember(stub, publish.from)
      // Shield the payload against the room's declared message type (`Pub`) — auto-generated, run only
      // on client-originated publishes (a server-side `me.publish()` is trusted and never comes here).
      // A fail rides the publish ack as `SHIELD_ERROR` (see `roomAckError`), rejecting the client's
      // `publish()` with a `ShieldValidationError` before the payload reaches a guard or the room.
      this._shieldPublishData(stub._publishShield, publish.data)
      return await this._publishText(publish.from, publish.data, publish.retain)
    }
    // Read only the sender prefix to check membership; the full validating unframe happens once, in
    // `_publishBinaryFramed`. A frame too short to carry a sender fails the membership check here.
    const from = binaryFrameSender(payload.binary)
    this._assertStubMember(stub, from)
    return await this._publishBinaryFramed(from, payload.binary)
  }

  private _assertStubMember(stub: RoomStubChannel, id: unknown): asserts id is string {
    if (typeof id !== 'string' || !stub._stubMembers.has(id)) {
      throw new RoomError('Not a participant of this room (joined through this connection)')
    }
  }

  /** @internal — MQTT-retained replay for the text lane. Called when a stub's text want grows
   *  (`_onPeerBroadcastSubscribe` for the whole lane, `sub-text` for member-scoped): replay the
   *  room's one retained-text slot iff this change is what newly covers the message's sender —
   *  an already-covered stub received it live, so it's delivered exactly once per subscription. */
  async _replayRetainedText(
    stub: RoomStubChannel,
    prevWantsText: boolean,
    prevMemberWants: ReadonlySet<string>,
  ): Promise<void> {
    // Wait until the shared text subscription is live at the backend before reading the retained slot.
    // A commit stores the retained copy before it publishes, so once the subscription is live, any
    // publish that raced this subscribe is either already in the copy we read or arrives live — never
    // lost in the gap between subscribing and the read. A synchronous backend (in-memory) resolves
    // instantly. See `SubSlot.ready` and `BackendSubscription.ready`.
    await this._textSub.ready
    const stored = await getBackend().readRetained(this.id, this._inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const info = { seq: stored.seq, timestamp: stored.timestamp }
    const envelope = parse(serialized) as RoomDataEnvelope
    if (prevWantsText || prevMemberWants.has(envelope.from) || !stub._wantsTextFrom(envelope.from)) return
    // Replay the stored frame as-is (it already carries its real order), and let the stub drop it if a
    // same-or-newer live frame already reached it (a publish that raced this subscribe) — exactly-once.
    stub._emitRetainedText(encodePublishText(serialized, info), envelope.from, info)
  }

  /** @internal — MQTT-retained replay for the binary lanes. Called when a stub's `sub-binary` want
   *  grows: replay each retained (member, track) frame this change newly covers (in the new want,
   *  not the old), so a subscriber gets the last retained frame of every lane it starts watching. The
   *  frame is self-describing, so the sender/track come from the frame itself, not the key. */
  async _replayRetainedBinary(stub: RoomStubChannel, prevWants: BinaryWants): Promise<void> {
    if (!wantsAnyBinary(stub._binaryWants)) return
    const roomWide = stub._binaryWants.everyMember
    if (roomWide.all || roomWide.tracks.length > 0) await this._ensureRoster()
    this._syncSubs()
    // Same handoff as the text lane (see `_replayRetainedText`): wait for the per-(member, track)
    // subscriptions to be live before reading the retained frames, so a frame racing the subscribe rides
    // the retained copy or the live lane instead of the gap. A synchronous backend resolves instantly.
    await this._binaryReady()
    const backend = getBackend()
    for (const lane of await backend.listRetained(this.id, this._inc)) {
      if (lane.kind !== 'binary') continue
      const stored = await backend.readRetained(this.id, this._inc, lane)
      if (stored === null) continue
      const framed = stored.payload
      const frame = unframeMemberId(framed)
      if (!frame) continue
      const track = frame.track ?? DEFAULT_TRACK
      if (binaryWantsCovers(prevWants, frame.from, track) || !stub._wantsBinary(frame.from, track)) continue
      // Replay with the frame's own stored receipt (never seq:0/Date.now()); the stub drops it if a
      // same-or-newer live frame on this lane already reached it — exactly-once, in order.
      const info: WirePublishInfo = { seq: stored.seq, timestamp: stored.timestamp }
      stub._emitRetainedBinary(encodePublishBinary(framed, info), frame.from, track, info)
    }
  }

  // ── Backend lane subscriptions ──

  /** @internal — recompute which backend lanes this instance needs and (un)subscribe to match.
   *  Idempotent; called after every change that can affect the answer (listeners, members,
   *  stubs, close). */
  _syncSubs(): void {
    const backend = getBackend()
    const state = this._state
    const open = !state.closed
    const observed =
      this._stubs.size > 0 ||
      this._localParticipants.size > 0 ||
      state.eventListenerCount + state.dataListenerCount + state.binaryListenerCount > 0

    // Control: one low-rate lane every observer holds — it's what keeps the live view correct.
    const becomesObserved = open && observed && !this._ctrlSub.active
    this._ctrlSub.sync(open && observed, () =>
      backend.subscribeLane(this.id, this._inc, CONTROL_LANE, (payload, info) =>
        this._onCtrlMessage(decodeRoomText(payload), info),
      ),
    )

    // Text and announcements share one semantic lane. A holder opens it on text/announcement demand,
    // then filters delivery at the per-stub relay.
    // A presence-only observer opens only the control lane.
    const textWants = this._aggregateTextWants()
    const wantAnyText = open && (textWants.all || textWants.members.size > 0)
    const wantAnnounce = state.wantsAnnounce || [...this._stubs].some((stub) => stub._wantsAnnounce)
    const wantSemantic = open && (wantAnyText || wantAnnounce)
    const memberIds = open ? state.listMemberIds() : []

    // Roster loads are need-driven: a resident roster refreshes on the observe transition
    // (events between its cell snapshot and this subscription were missed); a lazy one loads once
    // something actually needs the member view — room-level listeners (onLeave/onEmpty
    // and live senders are only correct against it) or a member-keyed lane. A holder that only
    // joins attaches neither, so `Room.join()` never loads a roster at all —
    // `getParticipants()`/serialization go through `_ensureRoster` on their own.
    const binaryWants = this._aggregateBinaryWants()
    const wantAnyBinary = open && wantsAnyBinary(binaryWants)
    const needsRoster =
      state.eventListenerCount + state.dataListenerCount + state.binaryListenerCount > 0 || wantAnyBinary
    if ((becomesObserved && state.rosterKnown) || (open && !state.rosterKnown && needsRoster)) {
      void this._refreshMembers().catch(reportRoomError)
    }
    // Semantic data: one lane per room. Once wanted, the node ingests the complete lane; text and
    // announcement selectivity is enforced at the per-stub relay (see `_onTextData`), never by
    // narrowing the backend subscription.
    this._textSub.sync(wantSemantic, () =>
      backend.subscribeLane(this.id, this._inc, SEMANTIC_LANE, (payload, info) =>
        this._onTextData(decodeRoomText(payload), info),
      ),
    )

    // Binary: per-(publisher, track) keys in every mode — subscribing want-selectively at the
    // source makes upstream delivery pay-per-want, not filter-after-receive: dropping the last
    // want for a track drops its key, and the publisher's `receivers` hits 0.
    this._syncKeyedSubs(this._binaryKeyUnsubs, wantAnyBinary ? this._binaryLanes(binaryWants, memberIds) : [], (lane) =>
      backend.subscribeLane(this.id, this._inc, lane, (framed, info) => this._onBinary(framed, info)),
    )

    // Demand (`onDemand`): gossip this node's local binary-demand transitions and push the
    // aggregated global count to any of our own members whose demand changed.
    const demandPairs = open ? this._localDemandPairs(binaryWants, memberIds) : []
    if (demandPairs.length === 0) this._demand.sync([])
    else {
      void this._binaryReady()
        .then(() => {
          const currentWants = this._aggregateBinaryWants()
          this._demand.sync(this._state.closed ? [] : this._localDemandPairs(currentWants, this._state.listMemberIds()))
        })
        .catch(reportRoomError)
    }

    // Inbox subscriptions follow ownership, not listeners — a holder must always be
    // able to receive direct messages addressed to its members.
    this._syncKeyedSubs(
      this._dmUnsubs,
      open ? this._ownedMemberIds().map((member) => ({ key: member, value: { kind: 'inbox', member } as const })) : [],
      (lane) =>
        backend.subscribeLane(this.id, this._inc, lane, (payload, info) => this._onDm(decodeRoomText(payload), info)),
    )

    this._syncHeartbeat()
  }

  /** Union of this holder's own binary listeners and every client stub's declared wants. */
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

  /** The backend lanes the aggregated binary wants resolve to — the exact upstream footprint.
   *  Per member: every-track wants take the default lane plus each *known* track's lane (named
   *  tracks are discovered — see `_ensureTrackAnnounced`); exact wants take exactly their lanes,
   *  eagerly (a lane needs no prior existence, so named subscribers never miss a frame). */
  private _binaryLanes(
    wants: BinaryWants,
    memberIds: string[],
  ): Array<{ key: string; value: Extract<LaneId, { kind: 'binary' }> }> {
    const lanes: Array<{ key: string; value: Extract<LaneId, { kind: 'binary' }> }> = []
    for (const memberId of new Set([...memberIds, ...Object.keys(wants.members)])) {
      const memberWants = wants.members[memberId]
      const eff = memberWants ? mergeTrackWants(wants.everyMember, memberWants) : wants.everyMember
      const tracks = eff.all ? [DEFAULT_TRACK, ...this._state.memberTracks(memberId)] : eff.tracks
      for (const track of tracks) {
        lanes.push({ key: `${memberId}\u0000${track}`, value: { kind: 'binary', member: memberId, track } })
      }
    }
    return lanes
  }

  /** The (member, track) pairs this instance has local binary demand for — the demand twin of
   *  `_binaryKeys`, kept in member/track terms so it can be gossiped and aggregated. */
  private _localDemandPairs(wants: BinaryWants, memberIds: string[]): Array<[string, string]> {
    const pairs: Array<[string, string]> = []
    for (const memberId of new Set([...memberIds, ...Object.keys(wants.members)])) {
      const memberWants = wants.members[memberId]
      const eff = memberWants ? mergeTrackWants(wants.everyMember, memberWants) : wants.everyMember
      const tracks = eff.all ? [DEFAULT_TRACK, ...this._state.memberTracks(memberId)] : eff.tracks
      for (const track of tracks) pairs.push([memberId, track])
    }
    return pairs
  }

  /** Whether one of this instance's own members is `id` — the instance that must aggregate and
   *  deliver `id`'s demand. */
  private _ownsMember(id: string): boolean {
    if (this._localParticipants.has(id)) return true
    for (const stub of this._stubs) if (stub._stubMembers.has(id)) return true
    return false
  }

  /** Route a member's freshly-changed track demand (aggregated by `RoomDemand`) to its holder —
   *  the local participant's `onDemand`, or the one client stub it joined through. */
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

  /** The text-lane twin of `_aggregateBinaryWants()` — a stub's broadcast subscription is its
   *  `all`, its `sub-text` set the member-scoped want. */
  private _aggregateTextWants(): { all: boolean; members: Set<string> } {
    if (this._tail) return { all: true, members: new Set() } // pre-attach tail: ingest everything now
    const local: MemberWants = this._state.textWants()
    if (local.all) return { all: true, members: new Set() }
    const members = new Set(local.members)
    for (const stub of this._stubs) {
      // A tail-pending stub ingests everything so its hold captures the whole recent tail; the
      // selector is applied at flush, not here (the want isn't known until the client subscribes).
      if (stub._wantsText || stub._tailPending !== null) return { all: true, members: new Set() }
      for (const id of stub._textMemberWants) members.add(id)
    }
    return { all: false, members }
  }

  /** Reconcile a map of keyed subscriptions (member IDs or lane keys) to the wanted set. */
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

  /** The binary analogue of `SubSlot.ready`: settles once every wanted per-(member, track) binary
   *  subscription is live and remains pending while a lane is internally lost. Awaited before retained
   *  replay so a keyframe racing the subscribe isn't lost in the gap. */
  private _binaryReady(): Promise<void> {
    const pending: Promise<void>[] = []
    for (const subscription of this._binaryKeyUnsubs.values()) pending.push(subscription.ready)
    return pending.length === 0 ? Promise.resolve() : Promise.all(pending).then(() => undefined)
  }

  /** Resolves once the local roster is authoritative: immediately while the live view holds it
   *  (roster known and the event stream attached), else via a backend cell read. */
  private _ensureRoster(): Promise<void> {
    if (this._state.closed || (this._state.rosterKnown && this._ctrlSub.established)) return Promise.resolve()
    return this._refreshMembers()
  }

  /** A message from a sender the loaded roster doesn't know is a drift signal — its join event
   *  was dropped or reordered away (pub/sub is at-most-once between nodes). The message itself
   *  already delivered correctly (identity rides the envelope); this heals the *view*, so the
   *  live participant materializes and long-lived observers can't stay stale forever.
   *  Single-flight, so a burst from the same unknown sender costs one backend snapshot. */
  private _healUnknownSender(from: string): void {
    if (!this._state.rosterKnown || this._state.getRemote(from) !== null) return
    void this._refreshMembers().catch(reportRoomError)
  }

  /** Single-flight roster refresh. A membership event landing mid-read makes the snapshot
   *  ambiguous (its cell write may or may not be in it) — re-read: joins/leaves write cells before
   *  publishing, so the next read includes the event that dirtied this one. */
  private _refreshMembers(): Promise<void> {
    this._pendingRefresh ??= (async () => {
      try {
        while (!this._state.closed) {
          const version = this._state.membershipVersion
          const members = await readMembers(this.id, this._inc)
          if (this._state.membershipVersion !== version) continue
          const drifted = this._state.reconcile(members)
          this._syncSubs() // per-member lanes may need subscriptions for the members just learned
          // Clients seeded from the pre-drift state must be re-synced the same way they were
          // seeded — the streamed roster (position-in-stream consistent, replace semantics).
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

  // ── Liveness heartbeat ──
  //
  // Graceful departures (leave, kick, close, stub death) are handled by events. A hard node
  // crash leaves member records behind — so the node that owns a member refreshes its `seenAt`
  // every interval, and every heartbeat also reaps members whose owner stopped refreshing.

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
      // Renew this node's binary-demand lease on every owner and sweep any crashed reporter's demand.
      // No cell I/O — runs first so member-cell latency never delays it (the demand TTL has slack for skips).
      this._demand.heartbeat()
      for (const id of this._ownedMemberIds()) {
        // Bump `seenAt` with a read-modify-write, not a whole-record `set`: the update only touches
        // `seenAt` on the record actually present, so a heartbeat can never clobber a concurrent
        // meta or track write. (That clobber is why the meta/track writers used to re-assert; with
        // the heartbeat off the collision course, those loops are gone.)
        const key = roomMemberKvKey(this.id, id)
        const record = await mutateCells(this.id, this._inc, { keys: [key] }, (cells) => {
          const raw = cells.get(key)
          if (raw === undefined) return { value: null, mutations: [] }
          const record = { ...(parse(decodeRoomText(raw)) as RoomMemberRecord), seenAt: Date.now() }
          return {
            value: record,
            mutations: [{ key, set: { bytes: encodeRoomText(stringify(record)) } }],
          }
        })
        if (record === null) {
          // Reaped or kicked while this node wasn't listening — the reaper already
          // published the leave event; only the local view needs to catch up.
          this._applyLeave(id)
          continue
        }
      }
      await this._reconcileAuthority()
    } finally {
      this._heartbeatBusy = false
    }
  }
}

// ---------------------------------------------------------------------------
// ServerLocalParticipant
// ---------------------------------------------------------------------------

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
    // `coalesce` bounds a client's uplink under a burst; a server-side publisher has no uplink
    // queue to conflate, so it's accepted for signature parity and otherwise a no-op. `retain`
    // still applies — a server-side publisher retains exactly like a client one.
    this._assertActive()
    return await this._room._publishText(this.id, data, options?.retain)
  }

  async publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishBinaryFramed(this.id, frameWithMemberId(this.id, data, options))
  }

  /** @internal — publish a client-framed payload (the frame already carries this member's ID). */
  _publishFramed(framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return this._room._publishBinaryFramed(this.id, framed)
  }

  // The impl of the overloaded `LocalParticipant.send` (see the interface for the precise result
  // types callers get); `any` is the overload-implementation signature.
  async send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    this._assertActive()
    const toId = typeof to === 'string' ? to : to.id
    if (!options?.ack) return this._room._sendDm(this.id, toId, data)
    const { receipt, reply } = await this._room._sendDmAck(this.id, toId, data)
    if (!reply.ok) throw roomFailureError(reply)
    // Superset of the plain-send receipt: the outbound DM's sequencing plus the recipient's reply.
    return { ...receipt, response: reply.result } satisfies RoomAckReceipt
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._setMemberMeta(this.id, meta)
    this._meta = meta
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._mergeMemberMeta(this.id, attrs)
    this._meta = mergeAttributes(this._meta, attrs)
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this._room._removeMember(this.id, { type: 'left' })
    this._onLeft({ type: 'left' }) // fires even when the room wasn't observing (no echo applied)
  }

  protected override _resolveSender(id: string): Sender | null {
    return this._room._state.getRemote(id) // sync view read — delivery must not wait on I/O
  }

  protected _reportError(err: unknown): void {
    reportRoomError(err)
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function runAfterHook(hook: () => unknown): Promise<void> {
  try {
    await hook()
  } catch (error) {
    reportRoomError(error)
  }
}

/** Identity is trusted — validate the server-side join option. */
function normalizeIdentity(options: JoinOptions | undefined): string | null {
  if (options?.identity === undefined) return null
  assertUsage(
    typeof options.identity === 'string' && options.identity.length > 0,
    'join() options.identity should be a non-empty string',
  )
  return options.identity
}

/** Server-side only, like `identity`: a client `join()` never reads this option, so it can't hide
 *  itself from the room. */
function normalizeHidden(options: JoinOptions | undefined): boolean {
  if (options?.hidden === undefined) return false
  assertUsage(typeof options.hidden === 'boolean', 'join() options.hidden should be a boolean')
  return options.hidden
}
