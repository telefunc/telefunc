export {
  ClientParticipantBase,
  ClientRoomParticipant,
  ClientStandaloneParticipant,
  reportClientCallbackError,
  request,
  sendDmReply,
}

import { assert, assertUsage } from '../utils/assert.js'
import { getOrCreate, markHandled } from 'telefunc/__internal'
import type { ChannelPublishAck, ClientBroadcast } from 'telefunc/__internal'
import type { TrackKey } from '../shared/binary.js'
import { assertKnownOptions, assertPublishOptions, ownMetaArgument, recipientId } from '../shared/arguments.js'
import {
  hasRoomTag,
  inboxMessageFromWire,
  leaveCauseFromWire,
  type RoomResult,
  type ParticipantStubMetadata,
  type ParticipantStubNotice,
  type ParticipantStubRequest,
  type MemberMetaUpdate,
  type MemberMutationRequest,
  type DemandChange,
  type TrackWatch,
  type TrackWatchState,
  type RoomStubRequest,
} from '../shared/protocol.js'
import { CLIENT_ROOM_BUG_MESSAGE, roomResultValue } from '../shared/errors.js'
import { ParticipantBase } from '../shared/ParticipantBase.js'
import type { MetaRewinds } from '../shared/MemberMeta.js'
import { heldConnectionChanged, holdMember, holdRoomOpen, letGoOfMember, releaseRoom, roomsOf } from './page.js'
import type { ClientRoomHandle } from './ClientRoomHandle.js'
import type {
  BinaryPublishOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  Sender,
} from '../types.js'

/** A reply that can't be sent is reported, and the sender gets an error instead. */
function sendDmReply(end: ClientBroadcast, ackId: string, reply: Promise<RoomResult>): void {
  const declare = (answer: RoomResult): RoomStubRequest => ({ __r: 'dm-reply', ackId, reply: answer })
  end._sendReply(reply.then(declare), declare({ ok: false, kind: 'error', message: CLIENT_ROOM_BUG_MESSAGE }))
}

/** An acked request on a room's or a participant's channel, as its value or the error it failed with. */
async function request(end: ClientBroadcast, req: RoomStubRequest | ParticipantStubRequest): Promise<unknown> {
  return roomResultValue(end.send(req, { ack: true }))
}

abstract class ClientParticipantBase extends ParticipantBase {
  private readonly watches = new Map<TrackKey, TrackWatch>()
  protected readonly _bugMessage = CLIENT_ROOM_BUG_MESSAGE

  /** @internal The page holds this member from now on, until it leaves. */
  _holdOnPage(): void {
    holdMember(this)
  }

  override _onDisconnected(): void {
    heldConnectionChanged(this.id, true)
    super._onDisconnected()
  }

  override _onReconnected(): void {
    heldConnectionChanged(this.id, false)
    super._onReconnected()
  }

  override _onLeft(cause: LeaveCause): void {
    super._onLeft(cause)
    letGoOfMember(this)
  }

  /** @internal As the room's stub last told the page. */
  get _watches(): ReadonlyMap<TrackKey, TrackWatch> {
    return this.watches
  }

  /** @internal Who receives one of its tracks changed. */
  _applyDemand(track: TrackKey, { add, remove, count }: DemandChange): void {
    if (count === 0) {
      this.watches.delete(track)
    } else {
      const watchers = new Set(this.watches.get(track)?.watchers)
      for (const id of add ?? []) watchers.add(id)
      for (const id of remove ?? []) watchers.delete(id)
      this.watches.set(track, { watchers, count })
    }
    this._changed()
  }

  /** @internal Who receives each of its tracks, whole, as a holder hears it when it attaches again. */
  _applyDemandState(tracks: TrackWatchState[]): void {
    this.watches.clear()
    for (const { track, watchers, count } of tracks) this.watches.set(track, { watchers: new Set(watchers), count })
    this._changed()
  }

  /** Each flavor's wire publish. */
  protected abstract _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck>
  protected abstract _sendPublishBinary(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck>
  /** The wire request of a flavor: the base's DMs and metadata writes, and the standalone's publish and leave. */
  protected abstract _request(request: MemberMutationRequest): Promise<unknown>

  // Messaging is often fire-and-forget: a usage error throws, and a failure is a rejection left handled.
  publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    assertPublishOptions(options)
    if (options?.track === undefined) return markHandled(this._sendPublish(data, options?.retain))
    return this.publishOnTrack(this.encodeTextFrame(data, options), options)
  }

  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    return this.publishOnTrack(this.encodeBinaryFrame(data, options), options)
  }

  private publishOnTrack(framed: Uint8Array, options: BinaryPublishOptions | undefined): Promise<ChannelPublishAck> {
    return markHandled(this._sendPublishBinary(framed, this.trackStreamOf(options?.track), options?.lossy === true))
  }

  send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    assertKnownOptions(options, ['ack'], 'send()')
    assertUsage(
      options?.ack === undefined || typeof options.ack === 'boolean',
      'send() options.ack should be a boolean',
    )
    return markHandled(this.sendDm(recipientId(to), data, options?.ack === true))
  }

  private async sendDm(to: string, data: unknown, ack: boolean): Promise<unknown> {
    this._assertActive()
    const request: MemberMutationRequest = { __r: 'req-dm', to, data }
    if (ack) request.ack = true
    return await this._request(request)
  }

  async _sendPrivate(to: string, field: string, data: unknown): Promise<void> {
    this._assertActive()
    await this._request({ __r: 'req-private', to, field, data })
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(meta, 'setMeta() meta')
    this._onOwnMetaWritten((await this._request({ __r: 'req-set-meta', meta: owned })) as MemberMetaUpdate)
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(attrs, 'setAttributes() attributes')
    this._onOwnMetaWritten((await this._request({ __r: 'req-set-attrs', attrs: owned })) as MemberMetaUpdate)
  }

  /** Concurrent writes can commit out of request order: adopt the sequence-accepted value, as observers do. */
  protected abstract _onOwnMetaWritten(accepted: MemberMetaUpdate): void

  protected _reportError(err: unknown): void {
    reportClientCallbackError(err)
  }
}

type ClientRoomParticipantOptions = {
  room: ClientRoomHandle
  id: string
  meta: ParticipantMeta
  selfDelivery: boolean
  rewinds: MetaRewinds
}

/** `LocalParticipant` returned by a page's `room.join()`. Operates through the room's channel. */
class ClientRoomParticipant extends ClientParticipantBase {
  private readonly room: ClientRoomHandle
  /** Weak: the room reaches this member, so a strong one would hold the room open for good. */
  private handle: WeakRef<LocalParticipant> | null = null

  constructor({ room, id, meta, selfDelivery, rewinds }: ClientRoomParticipantOptions) {
    super({
      // Client-side joins carry no identity: it's server-assigned (see JoinOptions.identity).
      participant: { id, meta, selfDelivery, identity: null },
      rewinds,
    })
    this.room = room
  }

  get isConnected(): boolean {
    return this.room.isConnected
  }

  protected override _resolveSender(id: string): Sender | null {
    // A sync read: delivery must not wait on I/O.
    return this.room.state.getRemote(id)
  }

  protected _shownParticipant(id: string): RemoteParticipant | null {
    return this.room.state.shownParticipantOf(id)
  }

  protected _followRoom(onChange: () => void): () => void {
    return this.room.state.onChange(onChange)
  }

  protected _deliverPrivate(field: string, from: string, data: unknown): void {
    this.room.state.applyPrivate(field, from, data)
  }

  protected override _onOwnMetaWritten(accepted: MemberMetaUpdate): void {
    this.room.acceptParticipantMeta(this.id, accepted)
  }

  protected async _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this.room.publishText(this.id, data, retain)
  }

  protected async _sendPublishBinary(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this.room.publishFrame(framed, stream, lossy)
  }

  protected _request(request: MemberMutationRequest): Promise<unknown> {
    return this.room.request({ ...request, id: this.id })
  }

  /** @internal The page's handle: it holds the room open until this member leaves, and nothing in the room reaches
   *  it. */
  _handOut(joining: Promise<LocalParticipant>): LocalParticipant {
    const bound = new Map<PropertyKey, unknown>()
    const handle = new Proxy<LocalParticipant>(this, {
      get(target, prop) {
        const value: unknown = Reflect.get(target, prop, target)
        return typeof value === 'function' ? getOrCreate(bound, prop, () => value.bind(target)) : value
      },
    })
    if (this._left) return handle
    holdRoomOpen(handle, joining)
    this.handle = new WeakRef(handle)
    return handle
  }

  override _onLeft(cause: LeaveCause): void {
    super._onLeft(cause)
    const handle = this.handle?.deref()
    if (handle) releaseRoom(handle)
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this.room.request({ __r: 'req-leave', id: this.id })
    this.room.dropParticipant(this.id)
    this._onLeft({ type: 'left' })
  }
}

type ClientStandaloneParticipantOptions = { end: ClientBroadcast; metadata: ParticipantStubMetadata }

/** `LocalParticipant` revived from a serialized `ServerLocalParticipant`. Owns its channel. */
class ClientStandaloneParticipant extends ClientParticipantBase {
  private readonly end: ClientBroadcast
  private readonly roomId: string

  constructor({ end, metadata }: ClientStandaloneParticipantOptions) {
    // Its own: the `rewound` notice forgets its seq.
    super({ participant: metadata, rewinds: { count: 0 } })
    this.end = end
    this.roomId = metadata.roomId

    end._subscribeLocal('text', (notice) => this.onNotice(notice))
    end.onClose(() => this._onLeft({ type: 'disconnected' }))
    // Held from its channel's open: before it, the channel reads disconnected.
    end.onOpen(() => this._holdOnPage())
    end.onDisconnect(() => this._onDisconnected())
    end.onReconnect(() => this._onReconnected())
  }

  get isConnected(): boolean {
    return this.end.isConnected
  }

  private onNotice(notice: unknown): void {
    assert(hasRoomTag(notice))
    const msg = notice as ParticipantStubNotice
    switch (msg.__r) {
      case 'p-meta':
        return this._onOwnMetaWritten(msg)
      case 'rewound':
        return this.memberMeta.forget()
      case 'demand':
        return this._applyDemand(msg.track, msg)
      case 'demand-state':
        return this._applyDemandState(msg.tracks)
      case 'dm':
        if (msg.ackId === undefined) return this._deliverMessage(inboxMessageFromWire(msg))
        return sendDmReply(this.end, msg.ackId, this._deliverMessageAck(inboxMessageFromWire(msg)))
      case 'left':
        return this._onLeft(leaveCauseFromWire(msg))
    }
  }

  /** It has no room of its own: the page's rooms of its room list the participants. */
  protected _shownParticipant(id: string): RemoteParticipant | null {
    for (const room of roomsOf(this.roomId)) {
      const participant = room.state.shownParticipantOf(id)
      if (participant) return participant
    }
    return null
  }

  protected _followRoom(onChange: () => void): () => void {
    const stops = Array.from(roomsOf(this.roomId), (room) => room.state.onChange(onChange))
    return () => stops.forEach((stop) => stop())
  }

  protected _deliverPrivate(field: string, from: string, data: unknown): void {
    for (const room of roomsOf(this.roomId)) room.state.applyPrivate(field, from, data)
  }

  /** Its own writes and the room's `p-meta` both land here. */
  protected override _onOwnMetaWritten(accepted: MemberMetaUpdate): void {
    this._acceptMeta(accepted)
  }

  protected async _sendPublish(data: unknown, retain?: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    const request: ParticipantStubRequest = { __r: 'req-publish', data }
    if (retain) request.retain = true
    return (await this._request(request)) as ChannelPublishAck
  }

  protected async _sendPublishBinary(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return roomResultValue<ChannelPublishAck>(this.end._publishBinaryUnreported(framed, stream, lossy))
  }

  protected _request(req: ParticipantStubRequest): Promise<unknown> {
    return request(this.end, req)
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this._request({ __r: 'req-leave' })
    this._onLeft({ type: 'left' })
    void this.end.close().catch(() => {})
  }
}

function reportClientCallbackError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
