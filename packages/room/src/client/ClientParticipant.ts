export { ClientParticipant, reportClientCallbackError, request, sendDmReply }
export type { AckedChannel, RoomChannel, PageLink }

import { getOrCreate } from 'telefunc/__internal'
import type { ChannelPublishAck, ClientBroadcast } from 'telefunc/__internal'
import type { TrackKey } from '../shared/binary.js'
import type {
  RoomResult,
  ParticipantStubRequest,
  MemberMetaUpdate,
  DemandChange,
  TrackWatch,
  TrackWatchState,
  RoomStubRequest,
} from '../shared/protocol.js'
import { CLIENT_ROOM_BUG_MESSAGE, publishAck, roomResultValue } from '../shared/errors.js'
import { ParticipantBase, type MemberLink } from '../shared/ParticipantBase.js'
import type { RoomRemotes } from '../shared/RoomView.js'
import { fieldsOf, memberMetaUpdateOf, numberOf, textOf } from '../shared/readers.js'
import { heldConnectionChanged, holdMember, holdRoomOpen, letGoOfMember, releaseRoom } from './page.js'
import { withChange } from '../utils/sets.js'
import type { LeaveCause, LocalParticipant, RemoteParticipant, Sender } from '../types.js'

/** A reply that can't be sent is reported, and the sender gets an error instead. */
function sendDmReply(end: Pick<ClientBroadcast, '_sendReply'>, ackId: string, reply: Promise<RoomResult>): void {
  const declare = (answer: RoomResult): RoomStubRequest => ({ __r: 'dm-reply', ackId, reply: answer })
  end._sendReply(
    reply.then(declare),
    declare({ ok: false, kind: 'error', code: 'INTERNAL', message: CLIENT_ROOM_BUG_MESSAGE }),
  )
}

/** A request a page sends on a room's or a participant's channel. */
type PageRequest = RoomStubRequest | ParticipantStubRequest

/** A channel a page's requests go out on, each answered. */
type AckedChannel = {
  send(req: PageRequest, opts: { ack: true }): Promise<unknown>
}

/** A page's channel to a Room stub, a room's or a lone participant's: it relays `Notice` on its text lane and takes
 *  `Publish` as a text publish. The stub answers a publish with a `RoomResult`, which core leaves unknown, so the
 *  answer is read with `roomResultValue()`. */
type RoomChannel<Notice, Publish> = AckedChannel &
  Pick<
    ClientBroadcast<Notice, Publish>,
    | '_publishUnreported'
    | '_publishBinaryUnreported'
    | '_sendReply'
    | '_subscribeLocal'
    | '_setSubscribed'
    | '_setWantParts'
    | '_confirmSoon'
    | 'onOpen'
    | 'onClose'
    | 'onDisconnect'
    | 'onReconnect'
    | 'isConnected'
    | 'close'
  >

/** What the server answers each kind of request with: a join's member, a meta write's update, a publish's place. */
type RequestAnswers = {
  'req-join': { id: string; joinedAt: number }
  'req-set-meta': MemberMetaUpdate
  'req-update-meta': MemberMetaUpdate
  'req-publish': ChannelPublishAck
  'req-leave': unknown
  'req-dm': unknown
  'req-private': unknown
  'dm-reply': unknown
}

/** Reads each kind's answer as the channel hands it over, which no type carries across the wire. */
const ANSWERS: { [Kind in PageRequest['__r']]: (value: unknown) => RequestAnswers[Kind] } = {
  'req-join': joinedMember,
  'req-set-meta': memberMetaUpdate,
  'req-update-meta': memberMetaUpdate,
  'req-publish': publishAck,
  'req-leave': (value) => value,
  'req-dm': (value) => value,
  'req-private': (value) => value,
  'dm-reply': (value) => value,
}

/** An acked request, as the answer its kind gets, or the error it failed with. */
async function request<Kind extends PageRequest['__r']>(
  end: AckedChannel,
  req: PageRequest & { __r: Kind },
): Promise<RequestAnswers[Kind]> {
  const answer = ANSWERS[req.__r]
  return answer(await roomResultValue(end.send(req, { ack: true })))
}

/** A join's answer: the member it made. */
function joinedMember(value: unknown): { id: string; joinedAt: number } {
  const fields = fieldsOf(value, "A join's answer")
  return {
    id: textOf(fields.id, "A joined member's id"),
    joinedAt: numberOf(fields.joinedAt, "A joined member's time"),
  }
}

/** A meta write's answer: the member's meta at the write's seq, whole or as the write merged it
 *  (`RoomWrites.writeMetaInOrder()`). */
function memberMetaUpdate(value: unknown): MemberMetaUpdate {
  return memberMetaUpdateOf(fieldsOf(value, "A meta write's answer"))
}

/** Where a page's participant acts, and the room it lists the other participants by: the room it joined through
 *  (`ClientRoomHandle`), or, for one a telefunction returned alone, its own channel (`LoneMember`). */
interface PageLink extends MemberLink {
  /** Whether the participant's channel is connected. */
  readonly isConnected: boolean
  /** The sender of a message to the participant, live, where its link has a room of its own. */
  resolveSender(id: string): Sender | null
  /** The app's participants of the room it lists the other participants by. */
  readonly remotes: RoomRemotes
  /** A package's message to the participant, for its field on the page's rooms of its room. */
  deliverPrivate(field: string, from: string, data: unknown): void
}

/** A page's `LocalParticipant`: joined through a room the page holds, or returned alone by a telefunction. */
class ClientParticipant extends ParticipantBase<PageLink> {
  private readonly watches = new Map<TrackKey, TrackWatch>()
  /** Weak: the room reaches this member, so a strong one would hold the room open for good. */
  private handle: WeakRef<LocalParticipant> | null = null

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
  _applyDemand(track: TrackKey, change: DemandChange): void {
    const was = this.watches.get(track)
    const watch = { watchers: withChange(was?.watchers, change), pages: withChange(was?.pages, change.pages) }
    if (watch.watchers.size === 0 && watch.pages.size === 0) this.watches.delete(track)
    else this.watches.set(track, watch)
    this._demandStale(track)
    this._changed()
  }

  /** @internal Who receives each of its tracks, whole, as a holder hears it when it attaches again. */
  _applyDemandState(tracks: TrackWatchState[]): void {
    this.watches.clear()
    for (const { track, watchers, pages } of tracks) {
      this.watches.set(track, { watchers: new Set(watchers), pages: new Set(pages) })
    }
    this._demandStale()
    this._changed()
  }

  protected override _resolveSender(id: string): Sender | null {
    // A sync read: delivery must not wait on I/O.
    return this.link.resolveSender(id)
  }

  protected _shownParticipants(ids: ReadonlySet<string>): RemoteParticipant[] {
    return this.link.remotes.shownOf(ids)
  }

  protected get _rosterVersion(): number {
    return this.link.remotes.state.model.version
  }

  protected _followRoom(onChange: () => void): () => void {
    return this.link.remotes.state.onChange(onChange, undefined, 'app')
  }

  protected _deliverPrivate(field: string, from: string, data: unknown): void {
    this.link.deliverPrivate(field, from, data)
  }

  protected _reportError(err: unknown): void {
    reportClientCallbackError(err)
  }
}

function reportClientCallbackError(err: unknown): void {
  console.error('[telefunc:room-error]', err instanceof Error ? err : new Error(String(err)))
}
