export { Room, ServerRoom, ServerLocalParticipant, reportRoomError }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { assert, assertUsage } from '../../utils/assert.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { isObject } from '../../utils/isObject.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../channel.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_KV_TTL_MS, ROOM_MEMBER_TTL_MS } from '../constants.js'
import { getBroadcastAdapter, type BroadcastAdapter } from '../server/broadcast.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../shared-ws.js'
import {
  ParticipantBase,
  ROOM_KEY_NAMESPACE,
  errorMessage,
  stampNewer,
  RoomState,
  frameWithMemberId,
  hasRoomTag,
  normalizeJoinOptions,
  roomConfigKvKey,
  roomDmKey,
  roomIdFromConfigKey,
  roomCtrlKey,
  roomTextKey,
  roomMemberDataKey,
  roomMemberKvKey,
  roomMemberKvPrefix,
  sizeFromWire,
  sizeToWire,
  unframeMemberId,
  uuidToBytes,
  type BinaryWants,
  type MemberSnapshot,
  type ReqJoinAck,
  type ReqOkAck,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomMemberRecord,
  type RoomStubRequest,
} from './shared.js'
import type { RoomStubChannel } from './stubs.js'
import type {
  JoinOptions,
  LocalParticipant,
  ParticipantMeta,
  RemoteParticipant,
  Room as RoomInstance,
  RoomInfo,
  RoomMeta,
  RoomOptions,
  PublishGuard,
  SendGuard,
  Sender,
} from './types.js'
assertIsNotBrowser()

/** This process's identity as an LWW writer — breaks `Room.update()` timestamp ties.
 *  Minted lazily: Cloudflare Workers forbid crypto RNG in module scope (this module loads at
 *  worker startup via the serializer registry), and inside a request it's always available. */
let _writerId: string | undefined
function writerId(): string {
  _writerId ??= crypto.randomUUID()
  return _writerId
}

/** `Room` is one identifier with two meanings, like the built-in `Date`: the statics object
 *  below (value) and the instance type from ./types.js — re-established locally so the two
 *  merge into a single export. */
type Room = RoomInstance

// ---------------------------------------------------------------------------
// `Room` entry point
// ---------------------------------------------------------------------------

type RoomStatic = {
  /** Create a new room. Throws if it already exists. */
  create(id: string, options?: RoomOptions): Promise<Room>
  /** Get an existing room. Throws if it doesn't exist. */
  get(id: string): Promise<Room>
  /** Guard the messages of every membership granted through `room` — server-side and
   *  client-side `join()`s alike. `onSend` runs before each private `send()`, `onPublish`
   *  before each `publish()`/`publishBinary()`; throwing rejects the sender's call with the
   *  error. Declared in the granting telefunction (close over `getContext()`); one
   *  `Room.guard()` per instance — declare all guards together. */
  guard(room: Room, guards: { onSend?: SendGuard; onPublish?: PublishGuard }): void
  /** Shorthand for `(await Room.get(id)).join(meta, options)`. */
  join(id: string, meta?: ParticipantMeta, options?: JoinOptions): Promise<LocalParticipant>
  /** List all rooms. */
  list(): Promise<RoomInfo[]>
  /** Admin: replace the room's configuration — omitted options reset to their defaults. */
  update(id: string, options: RoomOptions): Promise<void>
  /** Admin: close the room — disconnects all participants and removes the room. */
  close(id: string): Promise<void>
  /** Admin: remove a participant from the room. */
  removeParticipant(id: string, participantId: string): Promise<void>
  /** Publish a room-authored message — no sender, delivered to `onAnnounce()` (e.g. system notices). */
  announce(id: string, data: unknown): Promise<void>
  /** Send a server-authored private message to one participant — arrives on `listen()` with `from: null`. */
  send(id: string, participantId: string, data: unknown): Promise<void>
}

/**
 * Multi-party rooms with presence, membership, and admin controls. Server-side entry point —
 * clients receive `Room` and `LocalParticipant` objects by returning them from telefunctions.
 *
 * ```ts
 * import { Room } from 'telefunc'
 *
 * await Room.create('lobby', { meta: { topic: 'general' }, size: 100 })
 * const lobby = await Room.get('lobby')
 * const me = await lobby.join({ name: 'Alice' })
 * await me.publish({ text: 'hello' })
 * ```
 */
const Room: RoomStatic = {
  create: createRoom,
  get: getRoom,
  guard: guardRoom,
  join: joinRoom,
  list: listRooms,
  update: updateRoom,
  close: closeRoom,
  removeParticipant,
  announce: announceToRoom,
  send: sendToParticipant,
}

async function createRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const { meta, size } = normalizeOptions(options)
  const kv = getRoomKV()
  if ((await readConfig(kv, id)) !== null) throw new Error(`Room already exists: ${id}`)
  const config: RoomConfigRecord = {
    meta,
    size: sizeToWire(size),
    isolated: options?.isolated === true,
    at: Date.now(),
    by: writerId(),
  }
  await kv.set(roomConfigKvKey(id), stringify(config))
  return new ServerRoom(id, config, { members: [] }) // fresh room — the roster is known: empty
}

async function getRoom(id: string): Promise<Room> {
  const { kv, config } = await requireRoom(id)
  // One keys scan for the count — `isFull` capacity gates stay correct — but no per-member
  // reads: the roster itself loads lazily, on the first observation that needs it.
  const count = (await listMemberKeys(kv, id)).length
  return new ServerRoom(id, config, { count })
}

function guardRoom(room: Room, guards: { onSend?: SendGuard; onPublish?: PublishGuard }): void {
  assertUsage(ServerRoom.isServerRoom(room), 'Room.guard() expects a room obtained from Room.get()/Room.create()')
  assertUsage(isObject(guards), 'Room.guard() guards should be an object')
  assertUsage(
    guards.onSend === undefined || typeof guards.onSend === 'function',
    'Room.guard() onSend should be a function',
  )
  assertUsage(
    guards.onPublish === undefined || typeof guards.onPublish === 'function',
    'Room.guard() onPublish should be a function',
  )
  room._setGuards({ onSend: guards.onSend ?? null, onPublish: guards.onPublish ?? null })
}

async function joinRoom(id: string, meta?: ParticipantMeta, options?: JoinOptions): Promise<LocalParticipant> {
  // A pure joiner only wants its own participant handle — it never reads the roster, so it
  // skips even the count scan `Room.get()` pays: config read, join, done.
  const { config } = await requireRoom(id)
  return await new ServerRoom(id, config, { count: 0 }).join(meta, options)
}

async function listRooms(): Promise<RoomInfo[]> {
  const kv = getRoomKV()
  const rooms: RoomInfo[] = []
  for (const key of await kv.keys(ROOM_KEY_NAMESPACE)) {
    const id = roomIdFromConfigKey(key)
    if (id === null) continue
    const config = await readConfig(kv, id)
    if (config === null) continue // closed concurrently
    const count = (await listMemberKeys(kv, id)).length // scan only — no per-member reads
    const size = sizeFromWire(config.size)
    rooms.push({ id, meta: config.meta, size, count, isEmpty: count === 0, isFull: count >= size })
  }
  return rooms
}

async function updateRoom(id: string, options: RoomOptions): Promise<void> {
  const { meta, size } = normalizeOptions(options)
  const { kv, config } = await requireRoom(id)
  assertUsage(
    options?.isolated === undefined || options.isolated === config.isolated,
    "A room's `isolated` mode is fixed at creation — room.update() cannot change it",
  )
  const sizeWire = sizeToWire(size)
  // Strictly after the config we read (hybrid-clock style): back-to-back updates from one
  // writer always order, and cross-writer ordering stays wall-clock last-writer-wins.
  const at = Math.max(Date.now(), config.at + 1)
  const next: RoomConfigRecord = { meta, size: sizeWire, isolated: config.isolated, at, by: writerId() }
  await kv.set(roomConfigKvKey(id), stringify(next))
  await publishCtrl(id, { __r: 'update', meta, prev: config.meta, size: sizeWire, at: next.at, by: next.by })
  // Concurrent updates converge by the (at, by) stamp everywhere events reach — but the last KV
  // *write* wins arbitrarily. Read back: if a stamp-losing write landed on top, re-assert the
  // winner (only the winner rewrites, so the exchange terminates).
  const readBack = await readConfig(kv, id)
  if (readBack !== null && stampNewer(next, readBack)) await kv.set(roomConfigKvKey(id), stringify(next))
}

async function closeRoom(id: string): Promise<void> {
  const { kv } = await requireRoom(id)
  // Event first so observers disconnect promptly; then KV cleanup. A join racing the
  // cleanup re-checks the config after writing its member record and rolls back.
  await publishCtrl(id, { __r: 'closed' })
  for (const { key } of await listMemberKeys(kv, id)) await kv.delete(key)
  await kv.delete(roomConfigKvKey(id))
}

async function removeParticipant(id: string, participantId: string): Promise<void> {
  assertUsage(
    typeof participantId === 'string' && participantId.length > 0,
    'The participant ID should be a non-empty string',
  )
  const { kv } = await requireRoom(id)
  const memberKey = roomMemberKvKey(id, participantId)
  if ((await kv.get(memberKey)) === null) throw new Error(`Participant not found: ${participantId}`)
  await kv.delete(memberKey)
  await publishCtrl(id, { __r: 'leave', id: participantId })
}

async function announceToRoom(id: string, data: unknown): Promise<void> {
  await requireRoom(id)
  await getBroadcastAdapter().publish(roomCtrlKey(id), stringify({ __r: 'announce', data } satisfies RoomEnvelope))
}

async function sendToParticipant(id: string, participantId: string, data: unknown): Promise<void> {
  const { kv } = await requireRoom(id)
  if ((await kv.get(roomMemberKvKey(id, participantId))) === null) {
    throw new Error(`Participant not found: ${participantId}`)
  }
  // An empty `from` marks the message as server-authored — clients can't spoof it, their
  // DMs are validated against the members joined through their own stub.
  const envelope: RoomDmEnvelope = { __r: 'dm', to: participantId, from: '', fromMeta: null, data }
  await getBroadcastAdapter().publish(roomDmKey(id, participantId), stringify(envelope))
}

// ---------------------------------------------------------------------------
// ServerRoom
// ---------------------------------------------------------------------------

const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')

/**
 * Server-side `Room`.
 *
 * Not a channel itself: serializing a `ServerRoom` attaches a fresh `RoomStubChannel` per
 * response (see `roomReplacer`), so the same instance can be serialized any number of times
 * and `room.id` stays the room ID (wire channel IDs must be globally unique).
 *
 * All state changes flow through the room's pub/sub key: the node causing a change applies it
 * locally and publishes it; every other observing node — and this node's own echo — applies it
 * through its adapter subscription. Application is idempotent, so the overlap is harmless.
 */
class ServerRoom implements Room {
  readonly [SERVER_ROOM_BRAND] = true

  /** @internal */ readonly _isolated: boolean
  private _guards: { onSend: SendGuard | null; onPublish: PublishGuard | null } | null = null
  /** @internal */ readonly _state: RoomState
  private readonly _stubs = new Set<RoomStubChannel>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()

  private readonly _ctrlSub = new SubSlot()
  private readonly _textSub = new SubSlot()
  private readonly _memberTextUnsubs = new Map<string, () => void>()
  private readonly _memberBinaryUnsubs = new Map<string, () => void>()
  private readonly _dmUnsubs = new Map<string, () => void>()
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _heartbeatBusy = false
  private _pendingRefresh: Promise<void> | null = null

  constructor(roomId: string, config: RoomConfigRecord, seed: { members: MemberSnapshot[] } | { count: number }) {
    this._isolated = config.isolated
    this._state = new RoomState({
      roomId,
      meta: config.meta,
      size: sizeFromWire(config.size),
      seed,
      updateStamp: { at: config.at, by: config.by },
      onListenersChanged: () => this._syncSubs(),
      onCallbackError: reportRoomError,
    })
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
  }

  /** @internal — see `Room.guard()`. One declaration per instance keeps the grant declarative. */
  _setGuards(guards: { onSend: SendGuard | null; onPublish: PublishGuard | null }): void {
    assertUsage(
      this._guards === null,
      'Room.guard() was already called for this room instance — declare all guards in one call',
    )
    this._guards = guards
  }

  // ── Room API ──

  get id(): string {
    return this._state.roomId
  }
  get meta(): RoomMeta {
    return this._state.meta
  }
  get size(): number {
    return this._state.size
  }
  get count(): number {
    return this._state.count
  }
  get isEmpty(): boolean {
    return this._state.count === 0
  }
  get isFull(): boolean {
    return this._state.isFull
  }
  get isClosed(): boolean {
    return this._state.closed
  }

  async join(meta: ParticipantMeta = {}, options?: JoinOptions): Promise<LocalParticipant> {
    const selfDelivery = normalizeJoinOptions(meta, options)
    let participant!: ServerLocalParticipant
    await this._admitMember(meta, (id, joinedAt) => {
      participant = new ServerLocalParticipant(this, id, meta, joinedAt, selfDelivery)
      this._localParticipants.set(id, participant)
    })
    return participant
  }

  async getParticipants(): Promise<RemoteParticipant[]> {
    await this._ensureRoster()
    return this._state.listRemotes()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this._ensureRoster()
    return this._state.getRemote(id)
  }

  subscribe(callback: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._state.subscribe(callback)
  }
  subscribeBinary(callback: (data: Uint8Array, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._state.subscribeBinary(callback)
  }
  onJoin(callback: (member: RemoteParticipant) => void): () => void {
    return this._state.onJoin(callback)
  }
  onLeave(callback: (member: RemoteParticipant) => void): () => void {
    return this._state.onLeave(callback)
  }
  onUpdate(callback: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this._state.onUpdate(callback)
  }
  onEmpty(callback: () => void): () => void {
    return this._state.onEmpty(callback)
  }
  onFull(callback: () => void): () => void {
    return this._state.onFull(callback)
  }
  onClose(callback: () => void): () => void {
    return this._state.onClose(callback)
  }
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._state.onAnnounce(callback)
  }

  // ── Membership operations (shared by local participants and stub requests) ──

  /** Join choreography shared by local `join()` and stub `req-join`. `track` registers the
   *  holder first — the member must count as owned before `_syncSubs()` brings up its inbox
   *  subscription and heartbeat, and before its join is announced. */
  private async _admitMember(
    meta: ParticipantMeta,
    track: (id: string, joinedAt: number) => void,
  ): Promise<{ id: string; joinedAt: number }> {
    const { id, joinedAt } = await this._createMember(meta)
    track(id, joinedAt)
    this._syncSubs()
    this._state.applyJoin(id, meta, joinedAt)
    await publishCtrl(this.id, { __r: 'join', id, meta, joinedAt })
    return { id, joinedAt }
  }

  /** KV half of a join, guarding against a concurrent `Room.close()`. */
  private async _createMember(meta: ParticipantMeta): Promise<{ id: string; joinedAt: number }> {
    const kv = getRoomKV()
    await this._assertOpen(kv)
    const id = crypto.randomUUID()
    const joinedAt = Date.now()
    const record: RoomMemberRecord = { meta, joinedAt, seenAt: joinedAt, metaSeq: 0 }
    await kv.set(roomMemberKvKey(this.id, id), stringify(record), { ttlMs: ROOM_MEMBER_KV_TTL_MS })
    // The room may have been closed between the check and the write — roll back.
    if ((await readConfig(kv, this.id)) === null) {
      await kv.delete(roomMemberKvKey(this.id, id))
      throw new Error(`Room is closed: ${this.id}`)
    }
    return { id, joinedAt }
  }

  /** @internal */
  async _removeMember(id: string): Promise<void> {
    if (this._state.closed) return // close() already removed everyone
    await getRoomKV().delete(roomMemberKvKey(this.id, id))
    this._applyLeave(id)
    await publishCtrl(this.id, { __r: 'leave', id })
  }

  /** @internal */
  async _setMemberMeta(id: string, meta: ParticipantMeta): Promise<void> {
    assertUsage(isObject(meta), 'setMeta() meta should be an object')
    const kv = getRoomKV()
    await this._assertOpen(kv)
    const memberKey = roomMemberKvKey(this.id, id)
    const raw = await kv.get(memberKey)
    if (raw === null) throw new Error(`Participant not found (left?): ${id}`)
    const record = parse(raw) as RoomMemberRecord
    const prev = this._state.getRemote(id)?.meta ?? record.meta
    // The member's single owner serializes its setMeta() calls, so the KV record doubles as
    // the revision counter — no separate sequencer needed.
    const next: RoomMemberRecord = { ...record, meta, metaSeq: record.metaSeq + 1, seenAt: Date.now() }
    await kv.set(memberKey, stringify(next), { ttlMs: ROOM_MEMBER_KV_TTL_MS })
    this._state.applyParticipantMeta(id, meta, prev, next.metaSeq)
    await publishCtrl(this.id, { __r: 'p-meta', id, meta, prev, seq: next.metaSeq })
  }

  /** @internal — publish a member's message: the single choke point where the sender's verified
   *  meta is stamped into the envelope (never client-supplied) and the payload is routed to its
   *  lane's key. `framed` is pre-framed `[16-byte member ID][payload]`. */
  async _publishData(from: string, payload: { data: unknown } | { framed: Uint8Array }): Promise<ChannelPublishAck> {
    if (this._state.closed) throw new Error(`Room is closed: ${this.id}`)
    const onPublish = this._guards?.onPublish
    if (onPublish) {
      // The guard sees exactly what a subscriber would: the payload, without the wire frame.
      // (Every framed payload was validated at its entry point — the unframe cannot fail.)
      await onPublish(
        { id: from, meta: this._memberMeta(from) },
        'data' in payload ? payload.data : unframeMemberId(payload.framed)!.payload,
      )
    }
    const adapter = getBroadcastAdapter()
    const result =
      'data' in payload
        ? await adapter.publish(
            // Text rides the room's text key — the member's own key in isolated mode.
            this._isolated ? roomMemberDataKey(this.id, from) : roomTextKey(this.id),
            stringify({
              __r: 'data',
              from,
              fromMeta: this._memberMeta(from),
              data: payload.data,
            } satisfies RoomDataEnvelope),
          )
        : // Binary always rides the member's own key: per-publisher streams are what make
          // delivery member-selective at the source (and contention-free on every platform).
          await adapter.publishBinary(roomMemberDataKey(this.id, from), payload.framed)
    return Object.assign(makePublishInfo(this.id, result.seq, result.timestamp), { meta: result.meta })
  }

  /** The sender's meta as this node knows it — own participants first (freshest), then the view. */
  private _memberMeta(from: string): ParticipantMeta {
    return this._localParticipants.get(from)?.meta ?? this._state.getRemote(from)?.meta ?? {}
  }

  /** @internal — send a private message: published on the target's inbox key, which only
   *  the target's owning node subscribes to (see `_onDm`). The sender's verified meta rides
   *  the envelope so every receiver can surface a rich sender. */
  async _sendDm(from: string, to: string, data: unknown): Promise<void> {
    if (this._state.closed) throw new Error(`Room is closed: ${this.id}`)
    const target = await this._resolveMember(to)
    if (!target) throw new Error(`Participant not found: ${to}`)
    const fromMeta = this._memberMeta(from)
    const onSend = this._guards?.onSend
    if (onSend) await onSend({ id: from, meta: fromMeta }, target, data)
    const envelope: RoomDmEnvelope = { __r: 'dm', to, from, fromMeta, data }
    await getBroadcastAdapter().publish(roomDmKey(this.id, to), stringify(envelope))
  }

  /** The member's live view — falling back to the authoritative KV record, since the local
   *  view lags while unobserved (and briefly after the observe transition, until the KV
   *  resync lands). */
  private async _resolveMember(id: string): Promise<Sender | null> {
    const remote = this._state.getRemote(id)
    if (remote) return remote
    const raw = await getRoomKV().get(roomMemberKvKey(this.id, id))
    return raw === null ? null : { id, meta: (parse(raw) as RoomMemberRecord).meta }
  }

  private async _assertOpen(kv: RoomKV): Promise<void> {
    if (this._state.closed || (await readConfig(kv, this.id)) === null) {
      throw new Error(`Room is closed: ${this.id}`)
    }
  }

  // ── Event stream (adapter subscription callbacks) ──

  /** The control lane: presence & lifecycle events plus announcements — relayed to every stub
   *  unconditionally, since a client's live view is only correct if it sees every one. */
  private _onCtrlMessage(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the reserved key
    }
    if (!hasRoomTag(envelope)) return
    const event = envelope as RoomEnvelope
    if (event.__r === 'data') return // data never travels on the control key
    const wasClosed = this._state.closed

    if (event.__r === 'announce') {
      this._state.applyAnnounce(event.data, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
    } else {
      this._applyCtrl(event)
    }

    if (this._stubs.size > 0) {
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) stub._relayPublishText(wireText)
    }

    if (this._state.closed && !wasClosed) this._teardown()
  }

  /** The text data lane — relayed per stub, skipping the sender's own holder when it opted out. */
  private _onTextData(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the reserved key
    }
    if (!hasRoomTag(envelope) || envelope.__r !== 'data') return
    const event = envelope as RoomDataEnvelope
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    this._state.applyData(event.from, event.fromMeta, event.data, info, this._suppress(event.from))
    this._healUnknownSender(event.from)

    if (this._stubs.size > 0) {
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) {
        if (!stub._wantsText) continue
        if (stub._stubMembers.get(event.from)?.selfDelivery === false) continue
        stub._relayPublishText(wireText)
      }
    }
  }

  private _onBinary(framed: Uint8Array, rawInfo: WirePublishInfo): void {
    const unframed = unframeMemberId(framed)
    if (!unframed) return // junk on the reserved key
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    this._state.applyBinary(unframed.from, unframed.payload, info, this._suppress(unframed.from))
    this._healUnknownSender(unframed.from)

    if (this._stubs.size > 0) {
      const wireData = encodePublishBinary(framed, rawInfo)
      for (const stub of this._stubs) {
        if (!stub._wantsBinaryFrom(unframed.from)) continue
        if (stub._stubMembers.get(unframed.from)?.selfDelivery === false) continue
        stub._relayPublishBinary(wireData)
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
      return // junk on the reserved key
    }
    if (!hasRoomTag(envelope) || envelope.__r !== 'dm') return
    const dm = envelope as RoomDmEnvelope
    const local = this._localParticipants.get(dm.to)
    if (local) {
      local._deliverMessage(dm.from, dm.fromMeta, dm.data)
      return
    }
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) {
      if (stub._stubMembers.has(dm.to)) stub._relayPublishText(wireText)
    }
  }

  private _applyCtrl(event: RoomCtrlEnvelope): void {
    switch (event.__r) {
      case 'join':
        this._state.applyJoin(event.id, event.meta, event.joinedAt)
        this._syncSubs() // a new member means a new per-member key candidate
        return
      case 'leave':
        this._applyLeave(event.id)
        return
      case 'p-meta': {
        this._state.applyParticipantMeta(event.id, event.meta, event.prev, event.seq)
        const local = this._localParticipants.get(event.id)
        if (local) local._meta = event.meta
        return
      }
      case 'update':
        this._state.applyRoomUpdate(event.meta, event.prev, sizeFromWire(event.size), event.at, event.by)
        return
      case 'closed':
        this._state.applyClosed()
    }
  }

  private _applyLeave(id: string): void {
    this._state.applyLeave(id)
    const local = this._localParticipants.get(id)
    if (local) {
      this._localParticipants.delete(id)
      local._onLeft()
    }
    for (const stub of this._stubs) stub._stubMembers.delete(id)
    this._syncSubs()
  }

  /** The room closed — runs once, after the `closed` event has been applied and relayed. */
  private _teardown(): void {
    for (const local of this._localParticipants.values()) local._onLeft()
    this._localParticipants.clear()
    for (const stub of this._stubs) void stub.close().catch(() => {})
    this._syncSubs()
  }

  /** A member's own messages are suppressed for this holder when its participant opted out. */
  private _suppress(from: string): boolean {
    return this._localParticipants.get(from)?.selfDelivery === false
  }

  // ── Client stubs ──

  /** @internal — called by `roomReplacer` when this room is serialized into a response. */
  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    // The snapshot carries only scalars; the roster streams once the peer is attached (never
    // buffered — a byte-capped pre-peer buffer must not be able to evict it). Everything
    // relayed before it is already reflected in it; later events apply incrementally.
    stub.onOpen(() => {
      void this._ensureRoster()
        .then(() => {
          if (this._stubs.has(stub) && !this._state.closed) stub._relayRoster(this._state.snapshotMembers())
        })
        .catch(reportRoomError)
    })
    stub.onClose(() => {
      this._stubs.delete(stub)
      // The client is gone — presence says its members leave.
      for (const id of [...stub._stubMembers.keys()]) void this._removeMember(id).catch(reportRoomError)
      stub._stubMembers.clear()
      this._syncSubs()
    })
    this._syncSubs()
  }

  /** @internal — requests arriving on a room stub channel. The return value is the ack. */
  async _handleStubRequest(stub: RoomStubChannel, msg: unknown): Promise<ReqJoinAck | ReqOkAck | undefined> {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as RoomStubRequest
    try {
      switch (req.__r) {
        case 'req-join': {
          const meta = isObject(req.meta) ? req.meta : {}
          const { id, joinedAt } = await this._admitMember(meta, (id) =>
            stub._stubMembers.set(id, { selfDelivery: req.selfDelivery !== false }),
          )
          return { ok: true, id, joinedAt }
        }
        case 'req-leave':
          this._assertStubMember(stub, req.id)
          stub._stubMembers.delete(req.id)
          await this._removeMember(req.id)
          return { ok: true }
        case 'req-set-meta':
          this._assertStubMember(stub, req.id)
          await this._setMemberMeta(req.id, isObject(req.meta) ? req.meta : {})
          return { ok: true }
        case 'req-dm':
          this._assertStubMember(stub, req.id)
          await this._sendDm(req.id, req.to, req.data)
          return { ok: true }
        case 'sub-binary': {
          const members = Array.isArray(req.members) ? req.members.filter((m) => typeof m === 'string') : []
          stub._binaryWants = { all: req.all === true, members: new Set(members) }
          this._syncSubs()
          return { ok: true }
        }
        default:
          return undefined
      }
    } catch (err) {
      return { ok: false, err: errorMessage(err) }
    }
  }

  /** @internal — a client publish arriving on a room stub. The client's envelope is only a
   *  claim: membership is validated against this stub, and `_publishData` re-stamps the verified
   *  `fromMeta` — nothing client-supplied reaches the room stream except the payload itself. */
  async _publishFromStub(
    stub: RoomStubChannel,
    payload: { text: string } | { binary: Uint8Array },
  ): Promise<ChannelPublishAck> {
    if ('text' in payload) {
      const envelope = parse(payload.text) as unknown
      if (!hasRoomTag(envelope) || envelope.__r !== 'data') throw new Error('Malformed room publish')
      const publish = envelope as RoomDataPublish
      this._assertStubMember(stub, publish.from)
      return await this._publishData(publish.from, { data: publish.data })
    }
    const from = unframeMemberId(payload.binary)?.from
    this._assertStubMember(stub, from)
    return await this._publishData(from, { framed: payload.binary })
  }

  private _assertStubMember(stub: RoomStubChannel, id: unknown): asserts id is string {
    if (typeof id !== 'string' || !stub._stubMembers.has(id)) {
      throw new Error('Not a participant of this room (joined through this connection)')
    }
  }

  // ── Adapter subscriptions ──

  /** @internal — recompute which pub/sub keys this instance needs and (un)subscribe to match.
   *  Idempotent; called after every change that can affect the answer (listeners, members,
   *  stubs, close). */
  _syncSubs(): void {
    const adapter = getBroadcastAdapter()
    const state = this._state
    const open = !state.closed
    const observed =
      this._stubs.size > 0 ||
      this._localParticipants.size > 0 ||
      state.eventListenerCount + state.dataListenerCount + state.binaryListenerCount > 0

    // Control: one low-rate lane every observer holds — it's what keeps the live view correct.
    const becomesObserved = open && observed && !this._ctrlSub.active
    this._ctrlSub.sync(open && observed, () =>
      adapter.subscribe(roomCtrlKey(this.id), (serialized, info) => this._onCtrlMessage(serialized, info)),
    )

    // Text: its own lane, brought up only for holders that actually consume messages —
    // presence-only observers never receive the room's chatter.
    const wantText = open && (state.dataListenerCount > 0 || this._stubsWantText())
    const memberIds = open ? state.listMemberIds() : []

    // Roster loads are need-driven: a resident roster refreshes on the observe transition
    // (events between its KV read and this subscription were missed); a lazy one loads once
    // something actually needs the member view — room-level listeners (onLeave/onEmpty/onFull
    // and live senders are only correct against it) or a member-keyed lane. A holder that only
    // joins attaches neither, so `Room.join()` never loads a roster at all —
    // `getParticipants()`/serialization go through `_ensureRoster` on their own.
    const binaryWants = this._aggregateBinaryWants()
    const wantAnyBinary = open && (binaryWants.all || binaryWants.members.size > 0)
    const needsRoster =
      state.eventListenerCount + state.dataListenerCount + state.binaryListenerCount > 0 ||
      (this._isolated && wantText) ||
      wantAnyBinary
    if ((becomesObserved && state.rosterKnown) || (open && !state.rosterKnown && needsRoster)) {
      void this._refreshMembers().catch(reportRoomError)
    }
    if (this._isolated) {
      this._syncMemberSubs(this._memberTextUnsubs, wantText ? memberIds : [], (memberId) =>
        adapter.subscribe(roomMemberDataKey(this.id, memberId), (serialized, info) =>
          this._onTextData(serialized, info),
        ),
      )
    } else {
      this._textSub.sync(wantText, () =>
        adapter.subscribe(roomTextKey(this.id), (serialized, info) => this._onTextData(serialized, info)),
      )
    }

    // Binary: per-publisher keys in every mode — subscribing member-selectively at the source
    // makes upstream delivery pay-per-want, not filter-after-receive.
    const binaryIds = !wantAnyBinary
      ? []
      : binaryWants.all
        ? memberIds
        : memberIds.filter((id) => binaryWants.members.has(id))
    this._syncMemberSubs(this._memberBinaryUnsubs, binaryIds, (memberId) =>
      adapter.subscribeBinary(roomMemberDataKey(this.id, memberId), (framed, info) => this._onBinary(framed, info)),
    )

    // Inbox subscriptions follow ownership, not listeners — a holder must always be
    // able to receive direct messages addressed to its members.
    this._syncMemberSubs(this._dmUnsubs, open ? this._ownedMemberIds() : [], (memberId) =>
      adapter.subscribe(roomDmKey(this.id, memberId), (serialized, info) => this._onDm(serialized, info)),
    )

    this._syncHeartbeat()
  }

  /** Whether any client stub needs the text lane relayed. */
  private _stubsWantText(): boolean {
    for (const stub of this._stubs) if (stub._wantsText) return true
    return false
  }

  /** Union of this holder's own binary listeners and every client stub's declared wants. */
  private _aggregateBinaryWants(): { all: boolean; members: Set<string> } {
    const local: BinaryWants = this._state.binaryWants()
    if (local.all) return { all: true, members: new Set() }
    const members = new Set(local.members)
    for (const stub of this._stubs) {
      if (stub._binaryWants.all) return { all: true, members: new Set() }
      for (const id of stub._binaryWants.members) members.add(id)
    }
    return { all: false, members }
  }

  private _syncMemberSubs(
    subs: Map<string, () => void>,
    wantedIds: string[],
    subscribe: (memberId: string) => () => void,
  ): void {
    const wanted = new Set(wantedIds)
    for (const [memberId, unsub] of [...subs]) {
      if (!wanted.has(memberId)) {
        subs.delete(memberId)
        unsub()
      }
    }
    for (const memberId of wanted) {
      if (!subs.has(memberId)) subs.set(memberId, subscribe(memberId))
    }
  }

  /** Resolves once the local roster is authoritative: immediately while the live view holds it
   *  (roster known and the event stream attached), else via a KV read. */
  private _ensureRoster(): Promise<void> {
    if (this._state.closed || (this._state.rosterKnown && this._ctrlSub.active)) return Promise.resolve()
    return this._refreshMembers()
  }

  /** A message from a sender the loaded roster doesn't know is a drift signal — its join event
   *  was dropped or reordered away (pub/sub is at-most-once between nodes). The message itself
   *  already delivered correctly (identity rides the envelope); this heals the *view*, so the
   *  live participant materializes and long-lived observers can't stay stale forever.
   *  Single-flight, so a burst from the same unknown sender costs one KV read. */
  private _healUnknownSender(from: string): void {
    if (!this._state.rosterKnown || this._state.getRemote(from) !== null) return
    void this._refreshMembers().catch(reportRoomError)
  }

  /** Single-flight roster refresh. A membership event landing mid-read makes the snapshot
   *  ambiguous (its KV write may or may not be in it) — re-read: joins/leaves write KV before
   *  publishing, so the next read includes the event that dirtied this one. */
  private _refreshMembers(): Promise<void> {
    this._pendingRefresh ??= (async () => {
      try {
        while (!this._state.closed) {
          const version = this._state.membershipVersion
          const members = await readMembers(getRoomKV(), this.id)
          if (this._state.membershipVersion !== version) continue
          const drifted = this._state.reconcile(members)
          this._syncSubs() // per-member lanes may need subscriptions for the members just learned
          // Clients seeded from the pre-drift state must be re-synced the same way they were
          // seeded — the streamed roster (position-in-stream consistent, replace semantics).
          if (drifted) {
            for (const stub of this._stubs) stub._relayRoster(this._state.snapshotMembers())
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
    const want = !this._state.closed && this._ownedMemberIds().length > 0
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
    if (this._heartbeatBusy) return // a slow KV must not pile up overlapping ticks
    this._heartbeatBusy = true
    try {
      const kv = getRoomKV()
      for (const id of this._ownedMemberIds()) {
        const memberKey = roomMemberKvKey(this.id, id)
        const raw = await kv.get(memberKey)
        if (raw === null) {
          // Reaped or kicked while this node wasn't listening — the reaper already
          // published the leave event; only the local view needs to catch up.
          this._applyLeave(id)
          continue
        }
        const record = parse(raw) as RoomMemberRecord
        await kv.set(memberKey, stringify({ ...record, seenAt: Date.now() } satisfies RoomMemberRecord), {
          ttlMs: ROOM_MEMBER_KV_TTL_MS,
        })
      }
      await readMembers(kv, this.id)
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
  /** @internal */ readonly _joinedAt: number
  constructor(serverRoom: ServerRoom, id: string, meta: ParticipantMeta, joinedAt: number, selfDelivery: boolean) {
    super(id, meta, selfDelivery)
    this._room = serverRoom
    this._joinedAt = joinedAt
  }

  static isServerLocalParticipant(value: unknown): value is ServerLocalParticipant {
    return value !== null && typeof value === 'object' && SERVER_PARTICIPANT_BRAND in value
  }

  async publish(data: unknown): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishData(this.id, { data })
  }

  async publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishData(this.id, { framed: frameWithMemberId(this.id, data) })
  }

  /** @internal — publish a client-framed payload (the frame already carries this member's ID). */
  _publishFramed(framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return this._room._publishData(this.id, { framed })
  }

  async send(to: string | Sender, data: unknown): Promise<void> {
    this._assertActive()
    await this._room._sendDm(this.id, typeof to === 'string' ? to : to.id, data)
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._setMemberMeta(this.id, meta)
    this._meta = meta
  }

  async leave(): Promise<void> {
    if (this._left) return
    this._left = true
    await this._room._removeMember(this.id)
    this._onLeft() // fires even when the room wasn't observing (no echo applied)
  }

  protected override _resolveSender(id: string): Sender | null {
    return this._room._state.getRemote(id) // sync view read — delivery must not wait on I/O
  }

  protected _reportError(err: unknown): void {
    reportRoomError(err)
  }
}

// ---------------------------------------------------------------------------
// KV access
// ---------------------------------------------------------------------------

/** Room state lives in the broadcast adapter's KV so every server node sees the same rooms. */
type RoomKV = Required<Pick<BroadcastAdapter, 'get' | 'set' | 'delete' | 'keys'>>

function getRoomKV(): RoomKV {
  const adapter = getBroadcastAdapter()
  const missing = (['get', 'set', 'delete', 'keys'] as const).filter((method) => !adapter[method])
  assertUsage(
    missing.length === 0,
    `The installed broadcast adapter doesn't implement ${missing.map((m) => `\`${m}()\``).join(', ')} — the KV methods required by \`Room\`.`,
  )
  return adapter as RoomKV
}

/** Statics prologue: validate the ID and load the room's config — or throw `Room not found`. */
async function requireRoom(id: string): Promise<{ kv: RoomKV; config: RoomConfigRecord }> {
  assertRoomId(id)
  const kv = getRoomKV()
  const config = await readConfig(kv, id)
  if (config === null) throw new Error(`Room not found: ${id}`)
  return { kv, config }
}

async function readConfig(kv: RoomKV, roomId: string): Promise<RoomConfigRecord | null> {
  const raw = await kv.get(roomConfigKvKey(roomId))
  return raw === null ? null : (parse(raw) as RoomConfigRecord)
}

/** Read a room's member records, reaping members whose owning node stopped heartbeating
 *  (hard crash): their record is deleted and their leave announced to all observers. */
async function readMembers(kv: RoomKV, roomId: string): Promise<MemberSnapshot[]> {
  const members: MemberSnapshot[] = []
  for (const { key, id } of await listMemberKeys(kv, roomId)) {
    const raw = await kv.get(key)
    if (raw === null) continue // member left concurrently
    const record = parse(raw) as RoomMemberRecord
    if (Date.now() - record.seenAt > ROOM_MEMBER_TTL_MS) {
      await kv.delete(key)
      await publishCtrl(roomId, { __r: 'leave', id })
      continue
    }
    members.push({ id, meta: record.meta, joinedAt: record.joinedAt, metaSeq: record.metaSeq })
  }
  return members
}

/** Keys of a room's member records. Member IDs are UUIDs — anything else under the prefix
 *  belongs to another room whose ID happens to start with `${roomId}:m:` (e.g. its `:config` key). */
async function listMemberKeys(kv: RoomKV, roomId: string): Promise<Array<{ key: string; id: string }>> {
  const prefix = roomMemberKvPrefix(roomId)
  const memberKeys: Array<{ key: string; id: string }> = []
  for (const key of await kv.keys(prefix)) {
    const id = key.slice(prefix.length)
    if (uuidToBytes(id)) memberKeys.push({ key, id })
  }
  return memberKeys
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** One adapter subscription, reconciled to a desired on/off state. */
class SubSlot {
  private _unsub: (() => void) | null = null

  get active(): boolean {
    return this._unsub !== null
  }

  sync(want: boolean, subscribe: () => () => void): void {
    if (want && !this._unsub) {
      this._unsub = subscribe()
    } else if (!want && this._unsub) {
      const unsub = this._unsub
      this._unsub = null
      unsub()
    }
  }
}

async function publishCtrl(roomId: string, event: RoomCtrlEnvelope): Promise<void> {
  await getBroadcastAdapter().publish(roomCtrlKey(roomId), stringify(event))
}

function assertRoomId(id: unknown): asserts id is string {
  assertUsage(typeof id === 'string' && id.length > 0, 'The room ID should be a non-empty string')
}

function normalizeOptions(options: RoomOptions | undefined): { meta: RoomMeta; size: number } {
  assertUsage(options === undefined || isObject(options), 'Room options should be an object')
  const meta = options?.meta ?? {}
  assertUsage(isObject(meta), 'options.meta should be an object')
  const size = options?.size ?? Infinity
  assertUsage(typeof size === 'number' && size > 0 && !Number.isNaN(size), 'options.size should be a positive number')
  return { meta, size }
}

function reportRoomError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}
