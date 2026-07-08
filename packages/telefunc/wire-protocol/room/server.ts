export { Room, ServerRoom, ServerLocalParticipant, RoomStubChannel, bindParticipantStubChannel }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { assert, assertUsage } from '../../utils/assert.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { isObject } from '../../utils/isObject.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../channel.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_TTL_MS } from '../constants.js'
import { getBroadcastAdapter } from '../server/broadcast.js'
import { ServerChannel } from '../server/channel.js'
import { ServerBroadcast } from '../server/server-broadcast.js'
import { ACK_STATUS, encodePublishBinary, encodePublishText, type WirePublishInfo } from '../shared-ws.js'
import {
  ROOM_KEY_NAMESPACE,
  RoomState,
  frameWithMemberId,
  hasRoomTag,
  roomConfigKvKey,
  roomDmKey,
  roomMainKey,
  roomMemberDataKey,
  roomMemberKvKey,
  roomMemberKvPrefix,
  sizeFromWire,
  sizeToWire,
  unframeMemberId,
  uuidToBytes,
  type BinaryWants,
  type MemberSnapshot,
  type ParticipantStubRequest,
  type ReqJoinAck,
  type ReqOkAck,
  type ReqPublishAck,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomMemberRecord,
  type RoomStubRequest,
} from './shared.js'
import type {
  JoinOptions,
  LocalParticipant,
  ParticipantMeta,
  RemoteParticipant,
  Room as RoomInstance,
  RoomInfo,
  RoomMeta,
  RoomOptions,
} from './types.js'
assertIsNotBrowser()

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
  /** Send a server-authored private message to one participant — arrives on `listen()` with an empty `fromId`. */
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
  const config: RoomConfigRecord = { meta, size: sizeToWire(size), isolated: options?.isolated === true }
  await kv.set(roomConfigKvKey(id), stringify(config))
  return new ServerRoom(id, config, [])
}

async function getRoom(id: string): Promise<Room> {
  assertRoomId(id)
  const kv = getRoomKV()
  const config = await readConfig(kv, id)
  if (config === null) throw new Error(`Room not found: ${id}`)
  return new ServerRoom(id, config, await readMembers(kv, id, 'reap'))
}

async function listRooms(): Promise<RoomInfo[]> {
  const kv = getRoomKV()
  const configSuffix = ':config'
  const rooms: RoomInfo[] = []
  for (const key of await kv.keys(ROOM_KEY_NAMESPACE)) {
    if (!key.endsWith(configSuffix)) continue
    const id = key.slice(ROOM_KEY_NAMESPACE.length, -configSuffix.length)
    const config = await readConfig(kv, id)
    if (config === null) continue // closed concurrently
    const count = (await readMembers(kv, id, 'reap')).length
    const size = sizeFromWire(config.size)
    rooms.push({ id, meta: config.meta, size, count, isEmpty: count === 0, isFull: count >= size })
  }
  return rooms
}

async function updateRoom(id: string, options: RoomOptions): Promise<void> {
  assertRoomId(id)
  const { meta, size } = normalizeOptions(options)
  const kv = getRoomKV()
  const config = await readConfig(kv, id)
  if (config === null) throw new Error(`Room not found: ${id}`)
  assertUsage(
    options?.isolated === undefined || options.isolated === config.isolated,
    "A room's `isolated` mode is fixed at creation — room.update() cannot change it",
  )
  const sizeWire = sizeToWire(size)
  await kv.set(roomConfigKvKey(id), stringify({ meta, size: sizeWire, isolated: config.isolated }))
  await publishCtrl(id, { __r: 'update', meta, prev: config.meta, size: sizeWire, eid: makeEid() })
}

async function closeRoom(id: string): Promise<void> {
  assertRoomId(id)
  const kv = getRoomKV()
  if ((await readConfig(kv, id)) === null) throw new Error(`Room not found: ${id}`)
  // Event first so observers disconnect promptly; then KV cleanup. A join racing the
  // cleanup re-checks the config after writing its member record and rolls back.
  await publishCtrl(id, { __r: 'closed' })
  for (const member of await readMembers(kv, id, 'keep')) await kv.delete(roomMemberKvKey(id, member.id))
  await kv.delete(roomConfigKvKey(id))
}

async function removeParticipant(id: string, participantId: string): Promise<void> {
  assertRoomId(id)
  assertUsage(
    typeof participantId === 'string' && participantId.length > 0,
    'The participant ID should be a non-empty string',
  )
  const kv = getRoomKV()
  if ((await readConfig(kv, id)) === null) throw new Error(`Room not found: ${id}`)
  const memberKey = roomMemberKvKey(id, participantId)
  if ((await kv.get(memberKey)) === null) throw new Error(`Participant not found: ${participantId}`)
  await kv.delete(memberKey)
  await publishCtrl(id, { __r: 'leave', id: participantId })
}

async function announceToRoom(id: string, data: unknown): Promise<void> {
  assertRoomId(id)
  const kv = getRoomKV()
  if ((await readConfig(kv, id)) === null) throw new Error(`Room not found: ${id}`)
  await getBroadcastAdapter().publish(roomMainKey(id), stringify({ __r: 'announce', data } satisfies RoomEnvelope))
}

async function sendToParticipant(id: string, participantId: string, data: unknown): Promise<void> {
  assertRoomId(id)
  const kv = getRoomKV()
  if ((await readConfig(kv, id)) === null) throw new Error(`Room not found: ${id}`)
  if ((await kv.get(roomMemberKvKey(id, participantId))) === null) {
    throw new Error(`Participant not found: ${participantId}`)
  }
  // An empty `from` marks the message as server-authored — clients can't spoof it, their
  // DMs are validated against the members joined through their own stub.
  const envelope: RoomDmEnvelope = { __r: 'dm', to: participantId, from: '', data }
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
  /** @internal */ readonly _state: RoomState
  private readonly _stubs = new Set<RoomStubChannel>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()

  private _mainUnsub: (() => void) | null = null
  private _mainBinaryUnsub: (() => void) | null = null
  private readonly _memberTextUnsubs = new Map<string, () => void>()
  private readonly _memberBinaryUnsubs = new Map<string, () => void>()
  private readonly _dmUnsubs = new Map<string, () => void>()
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _heartbeatBusy = false

  constructor(roomId: string, config: RoomConfigRecord, members: MemberSnapshot[]) {
    this._isolated = config.isolated
    this._state = new RoomState({
      roomId,
      meta: config.meta,
      size: sizeFromWire(config.size),
      members,
      onListenersChanged: () => this._syncSubs(),
      onCallbackError: reportRoomError,
    })
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
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
    const { id, joinedAt } = await this._createMember(meta)
    const participant = new ServerLocalParticipant(this, id, meta, joinedAt, selfDelivery)
    this._localParticipants.set(id, participant)
    this._syncSubs() // subscribe before announcing, so cross-node events flow from now on
    this._state.applyJoin(id, meta, joinedAt)
    await publishCtrl(this.id, { __r: 'join', id, meta, joinedAt })
    return participant
  }

  async getParticipants(): Promise<RemoteParticipant[]> {
    // While observed, the event stream keeps the local view fresh. While unobserved,
    // no listeners exist that a change could notify — resync silently from KV.
    if (!this._state.closed && !this._mainUnsub) await this._refreshMembers()
    return this._state.listRemotes()
  }

  getParticipant(id: string): RemoteParticipant | null {
    return this._state.getRemote(id)
  }

  subscribe(callback: (data: unknown, info: ChannelPublishInfo, from: RemoteParticipant) => unknown): () => void {
    return this._state.subscribe(callback)
  }
  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo, from: RemoteParticipant) => unknown,
  ): () => void {
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

  /** @internal — KV half of a join, guarding against a concurrent `Room.close()`. */
  async _createMember(meta: ParticipantMeta): Promise<{ id: string; joinedAt: number }> {
    const kv = getRoomKV()
    await this._assertOpen(kv)
    const id = crypto.randomUUID()
    const joinedAt = Date.now()
    const record: RoomMemberRecord = { meta, joinedAt, seenAt: joinedAt }
    await kv.set(roomMemberKvKey(this.id, id), stringify(record))
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
    const next: RoomMemberRecord = { meta, joinedAt: record.joinedAt, seenAt: Date.now() }
    await kv.set(memberKey, stringify(next))
    const eid = makeEid()
    this._state.applyParticipantMeta(id, meta, prev, eid)
    await publishCtrl(this.id, { __r: 'p-meta', id, meta, prev, eid })
  }

  /** @internal — publish a member's message. `binary` is pre-framed `[16-byte member ID][payload]`. */
  async _publishData(from: string, payload: { text: string } | { binary: Uint8Array }): Promise<ChannelPublishAck> {
    if (this._state.closed) throw new Error(`Room is closed: ${this.id}`)
    const key = this._isolated ? roomMemberDataKey(this.id, from) : roomMainKey(this.id)
    const adapter = getBroadcastAdapter()
    const result =
      'text' in payload ? await adapter.publish(key, payload.text) : await adapter.publishBinary(key, payload.binary)
    return Object.assign(makePublishInfo(this.id, result.seq, result.timestamp), { meta: result.meta })
  }

  /** @internal — send a private message: published on the target's inbox key, which only
   *  the target's owning node subscribes to (see `_onDm`). */
  async _sendDm(from: string, to: string, data: unknown): Promise<void> {
    if (this._state.closed) throw new Error(`Room is closed: ${this.id}`)
    if (!this._state.getRemote(to)) throw new Error(`Participant not found: ${to}`)
    const envelope: RoomDmEnvelope = { __r: 'dm', to, from, data }
    await getBroadcastAdapter().publish(roomDmKey(this.id, to), stringify(envelope))
  }

  private async _assertOpen(kv: RoomKV): Promise<void> {
    if (this._state.closed || (await readConfig(kv, this.id)) === null) {
      throw new Error(`Room is closed: ${this.id}`)
    }
  }

  // ── Event stream (adapter subscription callbacks) ──

  private _onText(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the reserved key
    }
    if (!hasRoomTag(envelope)) return
    const event = envelope as RoomEnvelope
    const wasClosed = this._state.closed

    if (event.__r === 'data') {
      const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
      this._state.applyData(event.from, event.data, info, this._suppress(event.from))
    } else if (event.__r === 'announce') {
      this._state.applyAnnounce(event.data, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
    } else {
      this._applyCtrl(event)
    }

    if (this._stubs.size > 0) {
      const from = event.__r === 'data' ? event.from : null
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) {
        if (from !== null && stub._stubMembers.get(from)?.selfDelivery === false) continue
        stub._relayPublishText(wireText)
      }
    }

    if (this._state.closed && !wasClosed) this._teardown()
  }

  private _onBinary(framed: Uint8Array, rawInfo: WirePublishInfo): void {
    const unframed = unframeMemberId(framed)
    if (!unframed) return // junk on the reserved key
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    this._state.applyBinary(unframed.from, unframed.payload, info, this._suppress(unframed.from))

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
      local._deliverMessage(dm.from, dm.data)
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
        if (this._isolated) this._syncSubs() // a new member means a new data key
        return
      case 'leave':
        this._applyLeave(event.id)
        return
      case 'p-meta': {
        this._state.applyParticipantMeta(event.id, event.meta, event.prev, event.eid)
        const local = this._localParticipants.get(event.id)
        if (local) local._meta = event.meta
        return
      }
      case 'update':
        this._state.applyRoomUpdate(event.meta, event.prev, sizeFromWire(event.size), event.eid)
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
          const { id, joinedAt } = await this._createMember(meta)
          stub._stubMembers.set(id, { selfDelivery: req.selfDelivery !== false })
          this._syncSubs() // subscribe before announcing, so cross-node events flow from now on
          this._state.applyJoin(id, meta, joinedAt)
          await publishCtrl(this.id, { __r: 'join', id, meta, joinedAt })
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

  /** @internal — a client publish arriving on a room stub, already in adapter format. */
  async _publishFromStub(
    stub: RoomStubChannel,
    payload: { text: string } | { binary: Uint8Array },
  ): Promise<ChannelPublishAck> {
    let from: unknown
    if ('text' in payload) {
      const envelope = parse(payload.text) as unknown
      if (!hasRoomTag(envelope) || envelope.__r !== 'data') throw new Error('Malformed room publish')
      from = (envelope as RoomDataEnvelope).from
    } else {
      from = unframeMemberId(payload.binary)?.from
    }
    this._assertStubMember(stub, from)
    return await this._publishData(from, payload)
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

    const becomesObserved = open && observed && this._mainUnsub === null
    this._setSub('_mainUnsub', open && observed, () =>
      adapter.subscribe(roomMainKey(this.id), (serialized, info) => this._onText(serialized, info)),
    )
    // Events between construction (KV snapshot) and this subscription were missed — resync.
    if (becomesObserved) void this._refreshMembers().catch(reportRoomError)

    const binaryWants = this._aggregateBinaryWants()
    const wantAnyBinary = open && (binaryWants.all || binaryWants.members.size > 0)
    if (this._isolated) {
      // Isolated mode: data flows on per-member keys, one subscription per member —
      // and only the members whose binary someone actually wants.
      const wantText = open && (this._stubs.size > 0 || state.dataListenerCount > 0)
      const memberIds = open ? state.listMemberIds() : []
      const binaryIds = !wantAnyBinary
        ? []
        : binaryWants.all
          ? memberIds
          : memberIds.filter((id) => binaryWants.members.has(id))
      this._syncMemberSubs(this._memberTextUnsubs, wantText ? memberIds : [], (memberId) =>
        adapter.subscribe(roomMemberDataKey(this.id, memberId), (serialized, info) => this._onText(serialized, info)),
      )
      this._syncMemberSubs(this._memberBinaryUnsubs, binaryIds, (memberId) =>
        adapter.subscribeBinary(roomMemberDataKey(this.id, memberId), (framed, info) => this._onBinary(framed, info)),
      )
    } else {
      this._setSub('_mainBinaryUnsub', wantAnyBinary, () =>
        adapter.subscribeBinary(roomMainKey(this.id), (framed, info) => this._onBinary(framed, info)),
      )
    }

    // Inbox subscriptions follow ownership, not listeners — a holder must always be
    // able to receive direct messages addressed to its members.
    this._syncMemberSubs(this._dmUnsubs, open ? this._ownedMemberIds() : [], (memberId) =>
      adapter.subscribe(roomDmKey(this.id, memberId), (serialized, info) => this._onDm(serialized, info)),
    )

    this._syncHeartbeat()
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

  private _setSub(field: '_mainUnsub' | '_mainBinaryUnsub', want: boolean, subscribe: () => () => void): void {
    const current = this[field]
    if (want && !current) {
      this[field] = subscribe()
    } else if (!want && current) {
      this[field] = null
      current()
    }
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

  private async _refreshMembers(): Promise<void> {
    const version = this._state.membershipVersion
    const members = await readMembers(getRoomKV(), this.id, 'reap')
    // An event that applied while we were reading is fresher than the snapshot — drop it.
    if (this._state.membershipVersion === version) this._state.reconcile(members)
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
        await kv.set(memberKey, stringify({ ...record, seenAt: Date.now() } satisfies RoomMemberRecord))
      }
      await readMembers(kv, this.id, 'reap')
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
class ServerLocalParticipant implements LocalParticipant {
  readonly [SERVER_PARTICIPANT_BRAND] = true
  readonly id: string
  readonly selfDelivery: boolean

  /** @internal */ readonly _room: ServerRoom
  /** @internal */ _meta: ParticipantMeta
  /** @internal */ readonly _joinedAt: number
  private _left = false
  private _leftFired = false
  private _leaveCbs: Array<() => void> = []
  private readonly _messageCbs: Array<(data: unknown, fromId: string) => void> = []

  constructor(serverRoom: ServerRoom, id: string, meta: ParticipantMeta, joinedAt: number, selfDelivery: boolean) {
    this._room = serverRoom
    this.id = id
    this._meta = meta
    this._joinedAt = joinedAt
    this.selfDelivery = selfDelivery
  }

  static isServerLocalParticipant(value: unknown): value is ServerLocalParticipant {
    return value !== null && typeof value === 'object' && SERVER_PARTICIPANT_BRAND in value
  }

  get meta(): ParticipantMeta {
    return this._meta
  }

  async publish(data: unknown): Promise<ChannelPublishAck> {
    this._assertActive()
    const text = stringify({ __r: 'data', from: this.id, data } satisfies RoomEnvelope)
    return await this._room._publishData(this.id, { text })
  }

  async publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishData(this.id, { binary: frameWithMemberId(this.id, data) })
  }

  async send(to: string | RemoteParticipant, data: unknown): Promise<void> {
    this._assertActive()
    await this._room._sendDm(this.id, typeof to === 'string' ? to : to.id, data)
  }

  listen(callback: (data: unknown, fromId: string) => void): () => void {
    this._messageCbs.push(callback)
    return () => {
      const i = this._messageCbs.indexOf(callback)
      if (i >= 0) this._messageCbs.splice(i, 1)
    }
  }

  /** @internal — a direct message arrived on this member's inbox. */
  _deliverMessage(fromId: string, data: unknown): void {
    for (const cb of [...this._messageCbs]) {
      try {
        cb(data, fromId)
      } catch (err) {
        reportRoomError(err)
      }
    }
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

  onLeave(callback: () => void): () => void {
    if (this._leftFired) {
      invokeCallback(callback)
      return () => {}
    }
    this._leaveCbs.push(callback)
    return () => {
      const i = this._leaveCbs.indexOf(callback)
      if (i >= 0) this._leaveCbs.splice(i, 1)
    }
  }

  /** @internal — the member is gone (left, kicked, room closed, or holder disconnected). */
  _onLeft(): void {
    this._left = true
    if (this._leftFired) return
    this._leftFired = true
    const cbs = this._leaveCbs
    this._leaveCbs = []
    for (const cb of cbs) invokeCallback(cb)
  }

  private _assertActive(): void {
    if (this._left) throw new Error('Participant has left the room')
  }
}

// ---------------------------------------------------------------------------
// Wire stubs
// ---------------------------------------------------------------------------

/**
 * The channel registered with a response when a `Room` crosses the wire.
 * - server→client: room events & data relayed as PUBLISH frames (pre-peer buffered,
 *   replayed on reconnect). Text always flows — it carries presence; binary is opt-in.
 * - client→server: join/leave/set-meta as ack-bearing channel messages; publishes as
 *   PUBLISH(_BINARY)_ACK_REQ frames, validated against the members joined through this stub.
 */
class RoomStubChannel extends ServerBroadcast {
  private readonly _room: ServerRoom
  /** @internal — members the remote client joined through this stub. */
  readonly _stubMembers = new Map<string, { selfDelivery: boolean }>()
  /** @internal — which members' binary streams the client declared it wants (`sub-binary`). */
  _binaryWants: { all: boolean; members: Set<string> } = { all: false, members: new Set() }

  /** @internal */
  _wantsBinaryFrom(memberId: string): boolean {
    return this._binaryWants.all || this._binaryWants.members.has(memberId)
  }

  constructor(serverRoom: ServerRoom) {
    super({ key: roomMainKey(serverRoom.id) })
    this._room = serverRoom
    // Requests ride the plain channel message path. `ServerBroadcast` blocks the public
    // `listen()` (a `Room` isn't user-listenable) — register through the base class.
    ServerChannel.prototype.listen.call(this, (msg: unknown) => this._room._handleStubRequest(this, msg))
  }

  override _onPeerPublishAckReqMessage(text: string, seq: number): Promise<void> {
    return this._ackPublish(this._room._publishFromStub(this, { text }), seq)
  }

  override _onPeerPublishBinaryAckReqMessage(binary: Uint8Array, seq: number): Promise<void> {
    return this._ackPublish(this._room._publishFromStub(this, { binary }), seq)
  }

  private _ackPublish(publishing: Promise<ChannelPublishAck>, seq: number): Promise<void> {
    return this._trackAck(
      publishing.then(
        (ack) => this._sendAckRes(seq, stringify(ack)),
        (err: unknown) => this._sendAckRes(seq, errorMessage(err), ACK_STATUS.ERROR),
      ),
    )
  }

  // Text always flows (it carries presence); binary delivery is member-selective, declared
  // by the client's `sub-binary` request — the coarse BROADCAST_(UN)SUB ctrls are ignored.
  override _onPeerBroadcastSubscribe(): void {}
  override _onPeerBroadcastUnsubscribe(): void {}

  /** @internal */
  _relayPublishText(wireText: string): void {
    if (this._peer) this._peer.sendPublish(wireText)
    else this._prePeerBuffer.pushPublish(wireText)
  }

  /** @internal */
  _relayPublishBinary(wireData: Uint8Array): void {
    if (this._peer) this._peer.sendPublishBinary(wireData)
    else this._prePeerBuffer.pushPublishBinary(wireData)
  }
}

/**
 * Wire a fresh channel to a `ServerLocalParticipant` for serialization (see
 * `roomParticipantReplacer`). The client sends publish/set-meta/leave requests;
 * the server pushes metadata updates and the leave notice.
 */
function bindParticipantStubChannel(
  channel: ServerChannel<unknown, unknown>,
  participant: ServerLocalParticipant,
): void {
  channel.listen(async (msg: unknown) => {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as ParticipantStubRequest
    try {
      switch (req.__r) {
        case 'req-publish':
          return { ok: true, ack: await participant.publish(req.data) } satisfies ReqPublishAck
        case 'req-set-meta':
          await participant.setMeta(isObject(req.meta) ? req.meta : {})
          return { ok: true } satisfies ReqOkAck
        case 'req-dm':
          await participant.send(req.to, req.data)
          return { ok: true } satisfies ReqOkAck
        case 'req-leave':
          await participant.leave()
          return { ok: true } satisfies ReqOkAck
        default:
          return undefined
      }
    } catch (err) {
      return { ok: false, err: errorMessage(err) } satisfies ReqOkAck
    }
  })

  channel.listenBinary(async (framed: Uint8Array) => {
    try {
      if (unframeMemberId(framed)?.from !== participant.id) throw new Error('Malformed room binary publish')
      return { ok: true, ack: await participant._room._publishData(participant.id, { binary: framed }) }
    } catch (err) {
      return { ok: false, err: errorMessage(err) } satisfies ReqOkAck
    }
  })

  // Keep the client-side `participant.meta` fresh. Serializing a participant that already left
  // is possible (leave raced the response) — then there's no remote view left to observe.
  const remote = participant._room._state.getRemote(participant.id)
  const unlistenMeta = remote?.onUpdate((meta) => void channel.send({ __r: 'p-meta', meta }).catch(() => {}))

  // The participant's holder is the client — forward inbox deliveries to it.
  const unlistenDm = participant.listen((data, from) => {
    void channel.send({ __r: 'dm', from, data }).catch(() => {})
  })

  const unlistenLeave = participant.onLeave(() => {
    void channel.send({ __r: 'left' }).catch(() => {})
    void channel.close().catch(() => {})
  })

  channel.onClose(() => {
    unlistenMeta?.()
    unlistenDm()
    unlistenLeave()
    // The client is gone (page closed, GC, network death) — presence says the member leaves.
    void participant.leave().catch(reportRoomError)
  })
}

// ---------------------------------------------------------------------------
// KV access
// ---------------------------------------------------------------------------

type RoomKV = {
  get(key: string): Promise<string | null>
  set(key: string, value: string): Promise<void>
  delete(key: string): Promise<void>
  keys(prefix: string): Promise<string[]>
}

/** Room state lives in the broadcast adapter's KV so every server node sees the same rooms. */
function getRoomKV(): RoomKV {
  const adapter = getBroadcastAdapter()
  const missing = (['get', 'set', 'delete', 'keys'] as const).filter((method) => !adapter[method])
  assertUsage(
    missing.length === 0,
    `The installed broadcast adapter doesn't implement ${missing.map((m) => `\`${m}()\``).join(', ')} — the KV methods required by \`Room\`.`,
  )
  return {
    get: async (key) => await adapter.get!(key),
    set: async (key, value) => {
      await adapter.set!(key, value)
    },
    delete: async (key) => {
      await adapter.delete!(key)
    },
    keys: async (prefix) => await adapter.keys!(prefix),
  }
}

async function readConfig(kv: RoomKV, roomId: string): Promise<RoomConfigRecord | null> {
  const raw = await kv.get(roomConfigKvKey(roomId))
  return raw === null ? null : (parse(raw) as RoomConfigRecord)
}

/** Read a room's member records. With `'reap'`, members whose owning node stopped
 *  heartbeating (hard crash) are deleted and their leave is announced to all observers. */
async function readMembers(kv: RoomKV, roomId: string, staleMembers: 'reap' | 'keep'): Promise<MemberSnapshot[]> {
  const prefix = roomMemberKvPrefix(roomId)
  const members: MemberSnapshot[] = []
  for (const key of await kv.keys(prefix)) {
    const id = key.slice(prefix.length)
    // Member IDs are UUIDs — anything else under the prefix belongs to another room
    // whose ID happens to start with `${roomId}:m:` (e.g. its `:config` key).
    if (!uuidToBytes(id)) continue
    const raw = await kv.get(key)
    if (raw === null) continue // member left concurrently
    const record = parse(raw) as RoomMemberRecord
    if (staleMembers === 'reap' && Date.now() - record.seenAt > ROOM_MEMBER_TTL_MS) {
      await kv.delete(key)
      await publishCtrl(roomId, { __r: 'leave', id })
      continue
    }
    members.push({ id, meta: record.meta, joinedAt: record.joinedAt })
  }
  return members
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function publishCtrl(roomId: string, event: RoomCtrlEnvelope): Promise<void> {
  await getBroadcastAdapter().publish(roomMainKey(roomId), stringify(event))
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

/** Validates `join(meta, options)` arguments; returns the resolved `selfDelivery`. */
function normalizeJoinOptions(meta: unknown, options: JoinOptions | undefined): boolean {
  assertUsage(isObject(meta), 'join() meta should be an object')
  assertUsage(options === undefined || isObject(options), 'join() options should be an object')
  return options?.selfDelivery !== false
}

function makeEid(): string {
  return Math.random().toString(36).slice(2, 10)
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function invokeCallback(cb: () => void): void {
  try {
    cb()
  } catch (err) {
    reportRoomError(err)
  }
}

function reportRoomError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}
