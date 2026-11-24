export { ClientParticipant, acceptedMeta, reportClientCallbackError, request, sendDmReply }
export type { AckedChannel, PageLink }

import { getOrCreate, isObject } from 'telefunc/__internal'
import type { ClientBroadcast } from 'telefunc/__internal'
import type { TrackKey } from '../shared/binary.js'
import type {
  RoomResult,
  ParticipantStubRequest,
  MemberMetaUpdate,
  MemberRequest,
  DemandChange,
  TrackWatch,
  TrackWatchState,
  RoomStubRequest,
} from '../shared/protocol.js'
import { CLIENT_ROOM_BUG_MESSAGE, roomResultValue } from '../shared/errors.js'
import { ParticipantBase, type MemberLink } from '../shared/ParticipantBase.js'
import type { RoomRemotes } from '../shared/RoomView.js'
import { isRecord } from '../shared/arguments.js'
import { heldConnectionChanged, holdMember, holdRoomOpen, letGoOfMember, releaseRoom } from './page.js'
import { withChange } from '../utils/sets.js'
import type { LeaveCause, LocalParticipant, RemoteParticipant, Sender } from '../types.js'
import { assert } from '../utils/assert.js'

/** A reply that can't be sent is reported, and the sender gets an error instead. */
function sendDmReply(end: Pick<ClientBroadcast, '_sendReply'>, ackId: string, reply: Promise<RoomResult>): void {
  const declare = (answer: RoomResult): RoomStubRequest => ({ __r: 'dm-reply', ackId, reply: answer })
  end._sendReply(
    reply.then(declare),
    declare({ ok: false, kind: 'error', code: 'INTERNAL', message: CLIENT_ROOM_BUG_MESSAGE }),
  )
}

/** An acked request on a room's or a participant's channel, as its value or the error it failed with. */
/** A channel a page's requests go out on, each answered. */
type AckedChannel = {
  send(req: RoomStubRequest | ParticipantStubRequest, opts: { ack: true }): Promise<unknown>
}

async function request(end: AckedChannel, req: RoomStubRequest | ParticipantStubRequest): Promise<unknown> {
  return roomResultValue(end.send(req, { ack: true }))
}

/** The meta the room accepted, which a meta write resolves to. */
function acceptedMeta(req: MemberRequest, value: unknown): MemberMetaUpdate | null {
  return req.__r === 'req-set-meta' || req.__r === 'req-update-meta' ? memberMetaUpdate(value) : null
}

/** A meta write's answer: the member's meta at the write's seq, whole or as the write merged it
 *  (`RoomWrites.writeMetaInOrder()`). */
function memberMetaUpdate(value: unknown): MemberMetaUpdate {
  assert(isObject(value), 'A meta write was answered with no update')
  const { seq, meta, attrs, connected } = value
  assert(typeof seq === 'number', 'A meta update has no seq')
  if (isRecord(meta)) return value.disconnected === true ? { seq, meta, disconnected: true } : { seq, meta }
  if (isRecord(attrs)) return { seq, attrs }
  assert(typeof connected === 'boolean', 'A meta update carries neither meta nor a connection')
  return { seq, connected }
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
