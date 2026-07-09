export { ClientRoom, ClientRoomParticipant, ClientStandaloneParticipant }

import { assert } from '../../utils/assert.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { isObject } from '../../utils/isObject.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../channel.js'
import type { ClientBroadcast, ClientChannel } from '../client/channel.js'
import {
  ParticipantBase,
  RoomState,
  frameWithMemberId,
  hasRoomTag,
  normalizeJoinOptions,
  sizeFromWire,
  unframeMemberId,
  type BinaryWants,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type ReqJoinAck,
  type ReqOkAck,
  type ReqPublishAck,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomSnapshotMetadata,
  type RoomStubRequest,
} from './shared.js'
import type {
  JoinOptions,
  LocalParticipant,
  ParticipantMeta,
  RemoteParticipant,
  Room,
  RoomMeta,
  Sender,
} from './types.js'

// ---------------------------------------------------------------------------
// ClientRoom
// ---------------------------------------------------------------------------

/**
 * Client-side `Room`, revived from a serialized `ServerRoom`.
 *
 * Composes over a plain `ClientBroadcast` stub: room events & data arrive as its broadcast
 * messages, requests (join/leave/set-meta) ride its channel messages. Membership starts from
 * the serialized snapshot; the relayed event stream keeps it fresh from there.
 */
class ClientRoom implements Room {
  private readonly _stub: ClientBroadcast
  private readonly _state: RoomState
  private readonly _localParticipants = new Map<string, ClientRoomParticipant>()
  private _binaryUnsub: (() => void) | null = null
  private _lastBinaryWantsSent = ''

  constructor(stub: ClientBroadcast, snapshot: RoomSnapshotMetadata) {
    this._stub = stub
    this._state = new RoomState({
      roomId: snapshot.roomId,
      meta: snapshot.meta,
      size: sizeFromWire(snapshot.size),
      members: snapshot.members,
      closed: snapshot.closed,
      onListenersChanged: () => this._syncBinarySub(),
      onCallbackError: reportRoomError,
    })

    // Text events always flow (they carry presence); binary is subscribed on demand.
    stub.subscribe((envelope, info) => this._onEnvelope(envelope, info))
    // Wire death — server closed the room, network gave up, or the stub was GC'd.
    stub.onClose(() => this._applyClosed())
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
    const ack = (await this._request({ __r: 'req-join', meta, selfDelivery })) as ReqJoinAck
    if (!ack.ok) throw new Error(ack.err)
    const participant = new ClientRoomParticipant(this, ack.id, meta, selfDelivery)
    this._localParticipants.set(ack.id, participant)
    this._state.applyJoin(ack.id, meta, ack.joinedAt) // the relayed event is absorbed
    return participant
  }

  async getParticipants(): Promise<RemoteParticipant[]> {
    return this._state.listRemotes() // kept fresh by the event stream
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

  // ── Requests & publishes (used by ClientRoomParticipant) ──

  /** @internal */
  async _request(req: RoomStubRequest): Promise<ReqJoinAck | ReqOkAck> {
    const ack = await this._stub.send(req, { ack: true })
    assert(isObject(ack) && typeof ack.ok === 'boolean')
    return ack as ReqJoinAck | ReqOkAck
  }

  /** @internal */
  async _publishData(from: string, data: unknown): Promise<ChannelPublishAck> {
    return await this._stub.publish({ __r: 'data', from, data } satisfies RoomEnvelope)
  }

  /** @internal */
  async _publishBinaryData(framed: Uint8Array): Promise<ChannelPublishAck> {
    return await this._stub.publishBinary(framed)
  }

  /** @internal — a local participant completed its voluntary leave. */
  _dropParticipant(id: string): void {
    this._localParticipants.delete(id)
    this._state.applyLeave(id) // the relayed event is absorbed
  }

  // ── Event stream (relayed broadcast messages) ──

  private _onEnvelope(envelope: unknown, rawInfo: ChannelPublishInfo): void {
    if (!hasRoomTag(envelope)) return
    const event = envelope as RoomEnvelope | RoomDmEnvelope
    switch (event.__r) {
      case 'data':
        this._state.applyData(
          event.from,
          event.data,
          makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp),
          isSuppressed(this.id, event.from),
        )
        return
      case 'join':
        this._state.applyJoin(event.id, event.meta, event.joinedAt)
        return
      case 'leave': {
        this._state.applyLeave(event.id)
        const local = this._localParticipants.get(event.id)
        if (local) {
          this._localParticipants.delete(event.id)
          local._onLeft() // kicked, or left through another handle
        }
        return
      }
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
        this._applyClosed()
        return
      case 'announce':
        this._state.applyAnnounce(event.data, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
        return
      case 'dm':
        // Relayed from this member's private inbox — only its own stub ever receives it.
        this._localParticipants.get(event.to)?._deliverMessage(event.from, event.fromMeta, event.data)
    }
  }

  private _onBinaryFrame(framed: Uint8Array, rawInfo: ChannelPublishInfo): void {
    const unframed = unframeMemberId(framed)
    if (!unframed) return
    this._state.applyBinary(
      unframed.from,
      unframed.payload,
      makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp),
      isSuppressed(this.id, unframed.from),
    )
  }

  private _applyClosed(): void {
    if (this._state.closed) return
    this._state.applyClosed()
    // After onClose, like on the server: the room-level signal fires before per-handle cleanup.
    for (const local of this._localParticipants.values()) local._onLeft()
    this._localParticipants.clear()
  }

  /** Binary delivery is member-selective: the wanted set (all / specific members) is declared
   *  to the server, which relays only those members' frames. Declared synchronously — like
   *  `subscribe()` — so same-connection FIFO guarantees a publish right after subscribing gets
   *  its own frame back. Listener changes that leave the set unchanged send nothing. */
  private _syncBinarySub(): void {
    const wants: BinaryWants = this._state.closed ? { all: false, members: [] } : this._state.binaryWants()
    const wantAny = wants.all || wants.members.length > 0

    if (wantAny && !this._binaryUnsub) {
      this._binaryUnsub = this._stub.subscribeBinary((framed, info) => this._onBinaryFrame(framed, info))
    } else if (!wantAny && this._binaryUnsub) {
      const unsub = this._binaryUnsub
      this._binaryUnsub = null
      unsub()
    }

    if (this._state.closed) return // stub is dead — nothing to declare
    const encoded = wants.all ? 'all' : [...wants.members].sort().join(',')
    if (encoded === this._lastBinaryWantsSent) return
    this._lastBinaryWantsSent = encoded
    void this._stub.send({ __r: 'sub-binary', all: wants.all, members: wants.members }, { ack: false }).catch(() => {})
  }
}

// ---------------------------------------------------------------------------
// Local participants
// ---------------------------------------------------------------------------

/** Client-side half of both `LocalParticipant` flavors: links `selfDelivery` suppression
 *  to the sibling room via the registry below. */
abstract class ClientParticipantBase extends ParticipantBase {
  protected readonly _roomId: string

  constructor(roomId: string, id: string, meta: ParticipantMeta, selfDelivery: boolean) {
    super(id, meta, selfDelivery)
    this._roomId = roomId
    if (!selfDelivery) setSuppressed(roomId, id, true)
  }

  override _onLeft(): void {
    setSuppressed(this._roomId, this.id, false)
    super._onLeft()
  }

  protected _reportError(err: unknown): void {
    reportRoomError(err)
  }
}

/** `LocalParticipant` returned by `ClientRoom.join()` — operates through the room's stub. */
class ClientRoomParticipant extends ClientParticipantBase {
  private readonly _room: ClientRoom

  constructor(clientRoom: ClientRoom, id: string, meta: ParticipantMeta, selfDelivery: boolean) {
    super(clientRoom.id, id, meta, selfDelivery)
    this._room = clientRoom
  }

  protected override _resolveSender(id: string): Sender | null {
    return this._room.getParticipant(id)
  }

  async publish(data: unknown): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishData(this.id, data)
  }

  async publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishBinaryData(frameWithMemberId(this.id, data))
  }

  async send(to: string | Sender, data: unknown): Promise<void> {
    this._assertActive()
    const toId = typeof to === 'string' ? to : to.id
    unwrapOkAck(await this._room._request({ __r: 'req-dm', id: this.id, to: toId, data }))
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    unwrapOkAck(await this._room._request({ __r: 'req-set-meta', id: this.id, meta }))
    this._meta = meta
  }

  async leave(): Promise<void> {
    if (this._left) return
    this._left = true
    try {
      unwrapOkAck(await this._room._request({ __r: 'req-leave', id: this.id }))
    } finally {
      // Local cleanup even when the wire is gone — the server reaps the member on stub death.
      this._room._dropParticipant(this.id)
      this._onLeft()
    }
  }
}

/** `LocalParticipant` revived from a serialized `ServerLocalParticipant` — owns its stub channel. */
class ClientStandaloneParticipant extends ClientParticipantBase {
  private readonly _channel: ClientChannel

  constructor(channel: ClientChannel, metadata: ParticipantStubMetadata) {
    super(metadata.roomId, metadata.id, metadata.meta, metadata.selfDelivery)
    this._channel = channel

    channel.listen((notice: unknown) => {
      if (!hasRoomTag(notice)) return
      const msg = notice as ParticipantStubNotice
      if (msg.__r === 'p-meta') this._meta = msg.meta
      else if (msg.__r === 'dm') this._deliverMessage(msg.from, msg.fromMeta, msg.data)
      else this._onLeft()
    })
    channel.onClose(() => this._onLeft())
  }

  async publish(data: unknown): Promise<ChannelPublishAck> {
    this._assertActive()
    return unwrapPublishAck(await this._request({ __r: 'req-publish', data }))
  }

  async publishBinary(data: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return unwrapPublishAck(await this._channel.sendBinary(frameWithMemberId(this.id, data), { ack: true }))
  }

  async send(to: string | Sender, data: unknown): Promise<void> {
    this._assertActive()
    unwrapOkAck(await this._request({ __r: 'req-dm', to: typeof to === 'string' ? to : to.id, data }))
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    unwrapOkAck(await this._request({ __r: 'req-set-meta', meta }))
    this._meta = meta
  }

  async leave(): Promise<void> {
    if (this._left) return
    this._left = true
    try {
      unwrapOkAck(await this._request({ __r: 'req-leave' }))
    } finally {
      // Local cleanup even when the wire is gone — the server reaps the member on stub death.
      this._onLeft()
      void this._channel.close().catch(() => {})
    }
  }

  private _request(req: ParticipantStubRequest): Promise<unknown> {
    return this._channel.send(req, { ack: true })
  }
}

// ---------------------------------------------------------------------------
// selfDelivery registry — links participants to separately revived rooms
// ---------------------------------------------------------------------------

// A telefunction typically returns `{ room, participant }` — two independent wire values. When
// the participant sets `selfDelivery = false`, the sibling `ClientRoom` (same page, different
// wire object) must suppress that member's echoed messages. This registry is their only link.
const globalObject = getGlobalObject('wire-protocol/room/client.ts', {
  suppressed: new Map<string, Set<string>>(), // roomId → members with selfDelivery off
})

function setSuppressed(roomId: string, memberId: string, suppressed: boolean): void {
  const members = globalObject.suppressed.get(roomId)
  if (suppressed) {
    if (members) members.add(memberId)
    else globalObject.suppressed.set(roomId, new Set([memberId]))
  } else if (members) {
    members.delete(memberId)
    if (members.size === 0) globalObject.suppressed.delete(roomId)
  }
}

function isSuppressed(roomId: string, memberId: string): boolean {
  return globalObject.suppressed.get(roomId)?.has(memberId) ?? false
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function unwrapOkAck(ack: unknown): void {
  assert(isObject(ack) && typeof ack.ok === 'boolean')
  const res = ack as ReqOkAck
  if (!res.ok) throw new Error(res.err)
}

function unwrapPublishAck(ack: unknown): ChannelPublishAck {
  assert(isObject(ack) && typeof ack.ok === 'boolean')
  const res = ack as ReqPublishAck
  if (!res.ok) throw new Error(res.err)
  return res.ack
}

function reportRoomError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
