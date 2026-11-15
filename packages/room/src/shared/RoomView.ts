export { RoomView, RoomRemotes, remoteBacking }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import { type ChannelPublishInfo, untether, type TELEFUNC_SHIELDS } from 'telefunc/__internal'
import type { ListenerOptions } from './disposer.js'
import type { MemberEntry, ResolvedSender } from './RoomModel.js'
import type { Adder, MessageListener, RoomState, TrackListenerOptions } from './RoomState.js'
import { fieldValue, participantFields } from './participantFields.js'
import { roomMessages } from './messages.js'
import type { MemberSnapshot } from './protocol.js'
import type {
  BinaryFrameInfo,
  LeaveCause,
  ParticipantMeta,
  RemoteParticipant,
  RoomMessage,
  RoomMeta,
  RoomSnapshotView,
  Sender,
} from '../types.js'

/** The participant `snapshot()` lists, with the meta, connection and package fields it shows. */
type ShownParticipant = {
  meta: ParticipantMeta
  isConnected: boolean
  fields: Readonly<Record<string, unknown>>
  participant: RemoteParticipant
}

/** Exact-keyed backing lets the serializer recover (room, member) without exposing a public brand. A remote and the
 *  participants `snapshot()` lists of it share one. */
type RemoteBacking = { state: RoomState; entry: MemberEntry }

const { remoteBackings } = getGlobalObject('shared/RoomView.ts', () => ({
  remoteBackings: new WeakMap<object, RemoteBacking>(),
}))

/** The state and entry behind a minted `RemoteParticipant` (`null` for anything else). */
function remoteBacking(value: unknown): RemoteBacking | null {
  return typeof value === 'object' && value !== null ? (remoteBackings.get(value) ?? null) : null
}

type RoomRemotesOptions = { state: RoomState; adder: Adder }

/** One adder's participants of a room: a remote per member, the participants its snapshots list, and the snapshots.
 *  What is added through them is the adder's. A side's handle makes the app's, which its participants share with the
 *  user's `Room`. */
class RoomRemotes {
  readonly state: RoomState
  readonly adder: Adder
  private readonly remotes = new WeakMap<MemberEntry, RemoteParticipant>()
  private readonly shown = new WeakMap<MemberEntry, ShownParticipant>()
  private latestSnapshot: { version: number; value: WeakRef<RoomSnapshotView> } | null = null

  constructor({ state, adder }: RoomRemotesOptions) {
    this.state = state
    this.adder = adder
  }

  get(id: string): RemoteParticipant | null {
    const entry = this.state.model.entry(id)
    return entry ? this.of(entry) : null
  }

  /** Who a listener hears from: the member's remote, or the snapshot of a sender this room doesn't know. */
  sender(from: ResolvedSender): Sender {
    return 'entry' in from ? this.of(from.entry) : from.sender
  }

  visible(): RemoteParticipant[] {
    return this.state.model.visibleEntries().map((entry) => this.of(entry))
  }

  /** `join({ hidden: true })` members: routable, excluded from every presence read. */
  hidden(): RemoteParticipant[] {
    return [...this.state.model.entries()].filter((entry) => entry.hidden).map((entry) => this.of(entry))
  }

  /** Immutable view of the whole room, cached by the model's state version, so the reference is stable until something
   *  actually changes (the `useSyncExternalStore` contract). */
  snapshot(): RoomSnapshotView {
    const { model } = this.state
    const latest = this.latestSnapshot
    const cached = latest?.version === model.version ? latest.value.deref() : undefined
    if (cached) return cached
    const participants = Object.freeze(model.visibleEntries().map((entry) => this.shownParticipant(entry)))
    const value = Object.freeze({
      id: model.roomId,
      meta: model.meta,
      count: model.count,
      isClosed: model.closed,
      participants,
    })
    untether(value) // holding a snapshot must not keep the room's wrapper alive
    this.latestSnapshot = { version: model.version, value: new WeakRef(value) }
    return value
  }

  /** The members of `ids` as `snapshot()` lists them now, in its order: a hidden member is never listed. */
  shownOf(ids: ReadonlySet<string>): RemoteParticipant[] {
    const shown: RemoteParticipant[] = []
    for (const entry of this.state.model.entries()) {
      if (shown.length === ids.size) break
      if (ids.has(entry.id) && !entry.hidden) shown.push(this.shownParticipant(entry))
    }
    return shown
  }

  /** A revived `RemoteParticipant`: the member's remote, or one that left where this room doesn't have it. */
  fromSnapshot(snap: MemberSnapshot): RemoteParticipant {
    return this.of(this.state.model.entryFromSnapshot(snap))
  }

  /** The member's remote, the same object while the entry lives. */
  of(entry: MemberEntry): RemoteParticipant {
    const existing = this.remotes.get(entry)
    if (existing) return existing
    const { state, adder } = this
    const { model } = state
    const remote: RemoteParticipant = {
      id: entry.id,
      get meta() {
        return entry.meta.object
      },
      get joinedAt() {
        return entry.joinedAt
      },
      get identity() {
        return entry.identity
      },
      get isConnected() {
        return model.isConnected(entry)
      },
      subscribe: (cb: MessageListener<[]>, opts?: TrackListenerOptions) =>
        state.subscribeMember(entry, cb, opts, adder),
      subscribeBinary: (cb, opts) => state.subscribeMemberBinary(entry, cb, opts, adder),
      onChange: (cb, opts) => state.onMember(entry, 'change', cb, opts, adder),
      onDisconnect: (cb, opts) => state.onMember(entry, 'disconnect', cb, opts, adder),
      onReconnect: (cb, opts) => state.onMember(entry, 'reconnect', cb, opts, adder),
      onLeave: (cb: (cause: LeaveCause) => void, opts?: ListenerOptions) => state.onMemberLeave(entry, cb, opts, adder),
    }
    for (const name of participantFields().keys()) {
      Object.defineProperty(remote, name, { get: () => fieldValue(entry.fields, name), enumerable: true })
    }
    this.remotes.set(entry, remote)
    remoteBackings.set(remote, { state, entry })
    // A read through the page's room wrapper tethers what it returns to it, but only the member `join()` returns holds
    // its room open.
    untether(remote)
    return remote
  }

  /** The member's remote with its fields as they are now, the same object until they change. */
  private shownParticipant(entry: MemberEntry): RemoteParticipant {
    const meta = entry.meta.object
    const isConnected = this.state.model.isConnected(entry)
    const { fields } = entry
    const shown = this.shown.get(entry)
    if (shown?.meta === meta && shown.isConnected === isConnected && shown.fields === fields) return shown.participant
    const remote = this.of(entry)
    const values: PropertyDescriptorMap = {
      id: ownValue(entry.id),
      identity: ownValue(entry.identity),
      meta: ownValue(meta),
      joinedAt: ownValue(entry.joinedAt),
      isConnected: ownValue(isConnected),
    }
    for (const name of participantFields().keys()) values[name] = ownValue(fieldValue(fields, name))
    const participant: RemoteParticipant = Object.freeze(Object.create(remote, values))
    remoteBackings.set(participant, remoteBackings.get(remote)!)
    this.shown.set(entry, { meta, isConnected, fields, participant })
    return participant
  }
}

/** The side-neutral public `Room` over one adder's remotes: the user's (`ServerRoom`, `ClientRoom`), or the room a
 *  package's field sees, where what the field adds is the field's. Client and server keep their own I/O and
 *  lifecycle. */
class RoomView {
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }
  protected readonly _remotes: RoomRemotes

  constructor(remotes: RoomRemotes) {
    this._remotes = remotes
  }

  /** The remotes a read answers from; a server's room checks first how old its view is. */
  protected get _read(): RoomRemotes {
    return this._remotes
  }

  get id(): string {
    return this._remotes.state.model.roomId
  }

  get meta(): RoomMeta {
    return this._read.state.model.meta
  }

  get count(): number {
    return this._read.state.model.count
  }

  get isEmpty(): boolean {
    return this._read.state.model.count === 0
  }

  get isClosed(): boolean {
    return this._read.state.model.closed
  }

  getParticipant(id: string): RemoteParticipant | null {
    return this._read.get(id)
  }

  snapshot(): RoomSnapshotView {
    return this._read.snapshot()
  }

  subscribe(callback: MessageListener<[Sender]>, options?: TrackListenerOptions): () => void {
    const remotes = this._remotes
    const cb = (data: unknown, info: any, from: ResolvedSender) => callback(data, info, remotes.sender(from))
    return remotes.state.subscribe(cb, options, remotes.adder)
  }

  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender) => unknown,
    options?: TrackListenerOptions,
  ): () => void {
    const remotes = this._remotes
    const cb = (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: ResolvedSender) =>
      callback(data, info, remotes.sender(from))
    return remotes.state.subscribeBinary(cb, options, remotes.adder)
  }

  messages(options?: ListenerOptions): AsyncIterableIterator<RoomMessage> {
    return roomMessages(this, options?.signal)
  }

  onJoin(callback: (member: RemoteParticipant) => void, options?: ListenerOptions): () => void {
    const remotes = this._remotes
    return remotes.state.onJoin((entry) => callback(remotes.of(entry)), options, remotes.adder)
  }

  onLeave(callback: (member: RemoteParticipant, cause: LeaveCause) => void, options?: ListenerOptions): () => void {
    const remotes = this._remotes
    return remotes.state.onLeave((entry, cause) => callback(remotes.of(entry), cause), options, remotes.adder)
  }

  onEmpty(callback: () => void, options?: ListenerOptions): () => void {
    return this._remotes.state.onEmpty(callback, options, this._remotes.adder)
  }

  onClose(callback: (cause: LeaveCause) => void, options?: ListenerOptions): () => void {
    return this._remotes.state.onClose(callback, options, this._remotes.adder)
  }

  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void, options?: ListenerOptions): () => void {
    return this._remotes.state.onAnnounce(callback, options, this._remotes.adder)
  }

  onChange(callback: () => void, options?: ListenerOptions): () => void {
    return this._remotes.state.onChange(callback, options, this._remotes.adder)
  }
}

/** A snapshot participant's value: its own, so it shadows the remote's getter. */
function ownValue(value: unknown): PropertyDescriptor {
  return { value, enumerable: true }
}
