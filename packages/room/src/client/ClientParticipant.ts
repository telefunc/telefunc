export { ClientParticipant, reportClientCallbackError, request, sendDmReply }
export type { ClientParticipantOptions, PageLink }

import { assertUsage } from '../utils/assert.js'
import { getOrCreate, markHandled } from 'telefunc/__internal'
import type { ChannelPublishAck, ClientBroadcast } from 'telefunc/__internal'
import type { TrackKey } from '../shared/binary.js'
import { assertKnownOptions, assertPublishOptions, ownMetaArgument, recipientId } from '../shared/arguments.js'
import type {
  RoomResult,
  ParticipantStubRequest,
  MemberMetaUpdate,
  MemberMutationRequest,
  DemandChange,
  TrackWatch,
  TrackWatchState,
  RoomStubRequest,
} from '../shared/protocol.js'
import { CLIENT_ROOM_BUG_MESSAGE, roomResultValue } from '../shared/errors.js'
import { ParticipantBase, type ParticipantBaseOptions } from '../shared/ParticipantBase.js'
import type { RoomState } from '../shared/state.js'
import { heldConnectionChanged, holdMember, holdRoomOpen, letGoOfMember, releaseRoom } from './page.js'
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

/** Where a page's participant `from` acts, and the page's rooms it lists the other participants by: the room it
 *  joined through (`ClientRoomHandle`), or, for one a telefunction returned alone, its own channel (`LoneMember`). */
interface PageLink {
  readonly isConnected: boolean
  publishText(from: string, data: unknown, retain: boolean | undefined): Promise<ChannelPublishAck>
  publishFrame(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck>
  request(from: string, req: MemberMutationRequest): Promise<unknown>
  /** Leaves on the server, then ends the participant. */
  leave(from: string): Promise<void>
  /** The participant's own meta write, as the room accepted it. */
  acceptOwnMeta(from: string, accepted: MemberMetaUpdate): void
  /** The sender of a message to the participant, live, where its link has a room of its own. */
  resolveSender(id: string): Sender | null
  /** The states of the page's rooms it lists participants by. */
  rooms(): Iterable<RoomState>
}

type ClientParticipantOptions = ParticipantBaseOptions & { link: PageLink }

/** A page's `LocalParticipant`: joined through a room the page holds, or returned alone by a telefunction. */
class ClientParticipant extends ParticipantBase {
  private readonly link: PageLink
  private readonly watches = new Map<TrackKey, TrackWatch>()
  protected readonly _bugMessage = CLIENT_ROOM_BUG_MESSAGE
  /** Weak: the room reaches this member, so a strong one would hold the room open for good. */
  private handle: WeakRef<LocalParticipant> | null = null

  constructor({ link, ...options }: ClientParticipantOptions) {
    super(options)
    this.link = link
  }

  get isConnected(): boolean {
    return this.link.isConnected
  }

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
    const handle = this.handle?.deref()
    if (handle) releaseRoom(handle)
  }

  /** @internal The page's handle of a member its room's `join()` returns: it holds the room open until the member
   *  leaves, and nothing in the room reaches it. */
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

  protected override _resolveSender(id: string): Sender | null {
    // A sync read: delivery must not wait on I/O.
    return this.link.resolveSender(id)
  }

  protected _shownParticipant(id: string): RemoteParticipant | null {
    for (const state of this.link.rooms()) {
      const participant = state.shownParticipantOf(id)
      if (participant) return participant
    }
    return null
  }

  protected _followRoom(onChange: () => void): () => void {
    const stops = Array.from(this.link.rooms(), (state) => state.onChange(onChange))
    return () => stops.forEach((stop) => stop())
  }

  protected _deliverPrivate(field: string, from: string, data: unknown): void {
    for (const state of this.link.rooms()) state.applyPrivate(field, from, data)
  }

  // Messaging is often fire-and-forget: a usage error throws, and a failure is a rejection left handled.
  publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    assertPublishOptions(options)
    if (options?.track === undefined) return markHandled(this.publishText(data, options?.retain))
    return this.publishOnTrack(this.encodeTextFrame(data, options), options)
  }

  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    return this.publishOnTrack(this.encodeBinaryFrame(data, options), options)
  }

  private async publishText(data: unknown, retain: boolean | undefined): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this.link.publishText(this.id, data, retain)
  }

  private publishOnTrack(framed: Uint8Array, options: BinaryPublishOptions | undefined): Promise<ChannelPublishAck> {
    return markHandled(this.publishFrame(framed, this.trackStreamOf(options?.track), options?.lossy === true))
  }

  private async publishFrame(framed: Uint8Array, stream: string, lossy: boolean): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this.link.publishFrame(framed, stream, lossy)
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
    return await this.link.request(this.id, request)
  }

  async _sendPrivate(to: string, field: string, data: unknown): Promise<void> {
    this._assertActive()
    await this.link.request(this.id, { __r: 'req-private', to, field, data })
  }

  // Concurrent writes can commit out of request order: the participant adopts the sequence-accepted value, as
  // observers do.
  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(meta, 'setMeta() meta')
    const accepted = await this.link.request(this.id, { __r: 'req-set-meta', meta: owned })
    this.link.acceptOwnMeta(this.id, accepted as MemberMetaUpdate)
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    const owned = ownMetaArgument(attrs, 'setAttributes() attributes')
    const accepted = await this.link.request(this.id, { __r: 'req-set-attrs', attrs: owned })
    this.link.acceptOwnMeta(this.id, accepted as MemberMetaUpdate)
  }

  async leave(): Promise<void> {
    if (this._left) return
    await this.link.leave(this.id)
  }

  protected _reportError(err: unknown): void {
    reportClientCallbackError(err)
  }
}

function reportClientCallbackError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
