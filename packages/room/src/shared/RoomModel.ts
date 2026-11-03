export { RoomModel }
export type { MemberEntry, MembershipEvent, RosterRead, RoomModelOwner, ServerModelOwner }

import { assert } from '../utils/assert.js'
import { untether, getOrCreate } from 'telefunc/__internal'
import { Slice } from './Slice.js'
import { RoomMetaState } from './RoomMetaState.js'
import { MemberMeta, type MetaRewinds } from './MemberMeta.js'
import { ownLeaveCause, senderSnapshot, unknownLeaveCause } from './arguments.js'
import type {
  MemberMetaUpdate,
  MemberSnapshot,
  RoomMetaEntry,
  RoomMetaUpdate,
  SenderStamp,
  WholeMemberUpdate,
} from './protocol.js'
import type { LeaveCause, ParticipantMeta, RoomMeta, RoomSnapshotView, Sender } from '../types.js'
import type { RoomState } from './state.js'

/** A membership event as this model got it, whether it applied it or not. */
type MembershipEvent = { join: MemberSnapshot } | { leave: string } | { meta: { id: string; update: MemberMetaUpdate } }

/** A roster as read: its members; why each member that left recently left (a server reads its departing records);
 *  and `behind` while this side's delivery hadn't reached what the read covers: a leave event may still be on its
 *  way. */
type RosterRead = { members: MemberSnapshot[]; departing?: ReadonlyMap<string, LeaveCause>; behind?: boolean }

type MemberEntry = {
  id: string
  /** Stale and echoed `p-meta` events are absorbed. */
  meta: MemberMeta
  joinedAt: number
  identity: string | null
  /** An off-presence participant: a member for routing/discovery, excluded from every presence read (`count`,
   *  `snapshot`, `onJoin`/`onLeave`/`onEmpty`). Any number per room. */
  hidden: boolean
  left: boolean
  leaveCause?: LeaveCause
}

type RoomModelOptions = {
  roomId: string
  /** In key order. */
  meta: readonly RoomMetaEntry[]
  /** Either the authoritative roster, or just its member count. A lazy model seeds with `{ count }` and learns the
   *  members from its first roster (read on the server, streamed to the client). */
  seed: { members: MemberSnapshot[] } | { count: number }
  /** How many meta writes `meta` reflects (see `applyRoomUpdate`). */
  metaSeq: number
  closed?: boolean
  owner: RoomModelOwner
}

/** What a model tells and asks the side that holds it. */
interface RoomModelOwner {
  /** Every leave, from an event or a reconciled roster, after every view's callbacks; `hidden` is null for a member
   *  this model didn't know. */
  onMemberLeft(id: string, cause: LeaveCause, hidden: boolean | null): void
  // Only a client answers these: a server reads its rosters whole, and shows each member as the room records it.
  /** A member of a roster still streaming in or applying, which an unstamped message may name. */
  pendingMember?(id: string): MemberSnapshot | undefined
  /** This side's own connection to a member it holds, shown in place of the room's record of it; `null` for one it
   *  doesn't hold. */
  heldConnection?(id: string): boolean | null
  // Only a server acts on these (see `ServerModelOwner`): a client's local participants take their meta where it
  // arrives, and the server repairs the meta.
  onMemberMeta?(id: string, update: MemberMetaUpdate): void
  onMemberMetaGap?(): void
}

/** What a server's model also tells its replica. */
interface ServerModelOwner extends RoomModelOwner {
  /** A known member's meta update applied, from an event, a stamp or a roster read. */
  onMemberMeta(id: string, update: MemberMetaUpdate): void
  /** A member's meta change arrived ahead of one before it, which never arrived: the next roster read repairs it. */
  onMemberMetaGap(): void
}

/** A room's members, meta and closure as one side knows them, shared by every view over it. A `join` for a known
 *  member or a `leave` for an unknown one changes nothing, so a snapshot and a concurrent event stream compose without
 *  double-firing; each change it applies reaches every view with listeners. */
class RoomModel {
  readonly roomId: string
  closed: boolean
  /** Bumped on every observable change (membership, participant meta, room config, closure). Drives
   *  `onChange`/`snapshot()` cache invalidation. */
  private stateVersion = 0
  private snapshotCache: { version: number; value: WeakRef<RoomSnapshotView> } | null = null
  /** The membership events the in-flight roster read has seen so far. */
  private recording: MembershipEvent[] | null = null
  private readonly members = new Map<string, MemberEntry>()
  private hiddenMembers = 0
  /** The views with a listener: one without has nothing to fire. */
  private readonly views = new Set<RoomState>()
  private readonly owner: RoomModelOwner
  private readonly metaState: RoomMetaState
  /** Shared by the members' metas here and by the local participants of this view. */
  readonly metaRewinds: MetaRewinds = { count: 0 }
  private _rosterKnown: boolean
  private _closedCause: LeaveCause = ownLeaveCause({ type: 'closed' })
  private seedCount = 0
  /** The meta changes per member that reached this model before its first roster, in order. */
  private readonly preRosterMeta = new Map<string, MemberMetaUpdate[]>()

  constructor(opts: RoomModelOptions) {
    this.roomId = opts.roomId
    this.metaState = new RoomMetaState({ entries: opts.meta, seq: opts.metaSeq })
    this.closed = opts.closed === true
    this.owner = opts.owner
    if ('members' in opts.seed) {
      this._rosterKnown = true
      for (const member of opts.seed.members) this.createEntry(member)
    } else {
      this._rosterKnown = false
      this.seedCount = opts.seed.count
    }
  }

  /** Before the roster is known: the seed count (which excludes hidden members) adjusted by the events since. */
  get count(): number {
    if (!this._rosterKnown) return this.seedCount
    return this.members.size - this.hiddenMembers
  }

  /** How the room closed for this model; `null` while it is open. */
  get closedCause(): LeaveCause | null {
    return this.closed ? this._closedCause : null
  }

  /** Whether this model holds the authoritative member list (vs just a count). */
  get rosterKnown(): boolean {
    return this._rosterKnown
  }

  /** A member's meta and connection, whole, with the revision they were accepted at. */
  acceptedMeta(id: string): WholeMemberUpdate | null {
    const entry = this.members.get(id)
    return entry ? wholeUpdate(entry.meta.seq, entry.meta.object, !entry.meta.connected) : null
  }

  /** Whether a sender's stamp at `seq` is newer than the member's meta here. */
  newerStamp(id: string, seq: number): boolean {
    return this.members.get(id)?.meta.newerStamp(seq) ?? false
  }

  /** A failover rewound the room's and its members' meta seqs: the next whole meta of each applies, whatever its
   *  seq. */
  forgetMetaSeqs(): void {
    this.metaState.forget()
    this.metaRewinds.count++
    this.bumpState()
  }

  hasMember(id: string): boolean {
    return this.members.has(id)
  }

  /** Null for a member this model doesn't know. */
  memberHidden(id: string): boolean | null {
    return this.members.get(id)?.hidden ?? null
  }

  /** Members currently known, hidden ones included. */
  get memberCount(): number {
    return this.members.size
  }

  /** Member IDs currently known, live. Drives the per-member binary key subscriptions. */
  memberIds(): IterableIterator<string> {
    return this.members.keys()
  }

  /** The member's entry, which its remotes read. */
  entry(id: string): MemberEntry | undefined {
    return this.members.get(id)
  }

  /** Every member's entry, live. */
  entries(): IterableIterator<MemberEntry> {
    return this.members.values()
  }

  /** A member `memberIds()` listed, as a roster carries it. */
  memberSnapshot(id: string): MemberSnapshot {
    const { meta, joinedAt, identity, hidden } = this.members.get(id)!
    const member: MemberSnapshot = { id, meta: meta.object, joinedAt, metaSeq: meta.seq, identity }
    if (hidden) member.hidden = true
    if (!meta.connected) member.disconnected = true
    return member
  }

  /** Bumped on every observable change: a roster read at one version lists the same members as any other read at it. */
  get version(): number {
    return this.stateVersion
  }

  /** The entry of a revived `RemoteParticipant`: the live one if known, else seeded silently from the
   *  snapshot (its seed count already includes it). */
  entryFromSnapshot(snap: MemberSnapshot): MemberEntry {
    const existing = this.members.get(snap.id)
    if (existing) return existing
    if (this.closed) {
      // A closed room has no members: one it hands out left with it.
      const entry = newEntry(snap, this.metaRewinds)
      entry.left = true
      entry.leaveCause = this._closedCause
      return entry
    }
    const entry = this.createEntry(snap)
    this.bumpState()
    return entry
  }

  /** A view's listeners start or stop: only a view with one hears the changes. */
  listen(view: RoomState, on: boolean): void {
    if (on) this.views.add(view)
    else this.views.delete(view)
  }

  /** Immutable view of the whole room, cached by state version, so the reference is stable until something actually
   *  changes (the `useSyncExternalStore` contract). */
  snapshot(): RoomSnapshotView {
    const cached = this.snapshotCache?.version === this.stateVersion ? this.snapshotCache.value.deref() : undefined
    if (cached) return cached
    const participants = Object.freeze(
      [...this.members.values()]
        .filter((entry) => !entry.hidden)
        .map((entry) => {
          const { id, identity, meta, joinedAt } = entry
          return Object.freeze({ id, identity, meta: meta.object, joinedAt, isConnected: this.isConnected(entry) })
        }),
    )
    const value = Object.freeze({
      id: this.roomId,
      meta: this.meta,
      count: this.count,
      isClosed: this.closed,
      participants,
    })
    untether(value) // plain data: holding a snapshot must not keep the room's wrapper alive
    this.snapshotCache = { version: this.stateVersion, value: new WeakRef(value) }
    return value
  }

  /** State changed observably: invalidate the snapshot and tell `onChange` subscribers. */
  private bumpState(): void {
    this.stateVersion++
    for (const view of [...this.views]) view.changed()
  }

  /** As this side shows it: its own connection to a member it holds, the room's record for any other. */
  isConnected(entry: MemberEntry): boolean {
    return this.owner.heldConnection?.(entry.id) ?? entry.meta.connected
  }

  /** This side's own connection to a member it holds changed from `wasConnected`, or `null` for one it didn't hold. */
  heldConnectionChanged(id: string, wasConnected: boolean | null): void {
    const entry = this.members.get(id)
    if (!entry || this.isConnected(entry) === (wasConnected ?? entry.meta.connected)) return
    this.bumpState()
    this.connectionChanged(entry)
  }

  private connectionChanged(entry: MemberEntry): void {
    for (const view of [...this.views]) view.connectionChanged(entry)
  }

  /** Records the membership events this model gets into the returned list, until `stopRecordingMembership()`. */
  recordMembership(): readonly MembershipEvent[] {
    assert(this.recording === null)
    this.recording = []
    return this.recording
  }

  stopRecordingMembership(): void {
    this.recording = null
  }

  private record(event: MembershipEvent): void {
    this.recording?.push(event)
  }

  applyJoin(member: MemberSnapshot): boolean {
    this.record({ join: member })
    if (this.closed) return false
    // A known member's join echo has nothing new: its meta is the admission meta, frozen before any guard.
    if (this.members.has(member.id)) return false
    const entry = this.createEntry(member)
    // A hidden participant is no presence event (no count, no `onJoin`), but the roster changed, so `onChange` fires.
    if (entry.hidden) {
      this.bumpState()
      return true
    }
    if (!this._rosterKnown) this.seedCount++ // pre-roster, `count` is the seed adjusted by applied events
    this.bumpState()
    for (const view of [...this.views]) view.joined(entry)
    return true
  }

  /** A leave a server relayed, which it does once for each member it knew: before the first roster, a visible member
   *  this model has not seen is one its seed count holds. */
  applyRelayedLeave(id: string, cause: LeaveCause, hidden: boolean): void {
    if (!this._rosterKnown && !hidden && !this.closed && !this.members.has(id)) {
      this.seedCount = Math.max(0, this.seedCount - 1)
      this.bumpState()
    }
    this.applyLeave(id, cause)
  }

  applyLeave(id: string, cause: LeaveCause): void {
    this.record({ leave: id })
    const entry = this.members.get(id)
    if (entry) this.removeEntry(entry, ownLeaveCause(cause))
    this.owner.onMemberLeft(id, cause, entry ? entry.hidden : null)
  }

  private removeEntry(entry: MemberEntry, cause: LeaveCause): void {
    entry.left = true
    entry.leaveCause = cause
    this.members.delete(entry.id)
    if (entry.hidden) this.hiddenMembers--
    // A hidden participant's leave is no presence event either; its own handlers and listener release still run.
    if (!entry.hidden && !this._rosterKnown) this.seedCount = Math.max(0, this.seedCount - 1)
    this.bumpState()
    for (const view of [...this.views]) view.left(entry, cause)
  }

  /** Applies updates in seq order (see `MemberMeta.apply`): the origin's echo (same seq) and events arriving behind a
   *  fresher reconcile are absorbed. Only a listener builds the meta object per update. */
  applyParticipantMeta(id: string, update: MemberMetaUpdate): boolean {
    this.record({ meta: { id, update } })
    const entry = this.members.get(id)
    // Before the first roster every member is unknown, and the first roster applies the changes after its own.
    if (!entry && !this._rosterKnown) getOrCreate(this.preRosterMeta, id, () => []).push(update)
    if (!entry) return false
    const listening = 'connected' in update ? [] : [...this.views].filter((view) => view.listensToMeta(entry))
    const prev = listening.length > 0 ? entry.meta.object : null
    const wasConnected = this.isConnected(entry)
    const applied = entry.meta.apply(update)
    if (applied === 'gap') this.owner.onMemberMetaGap?.()
    if (applied !== 'applied') return false
    this.owner.onMemberMeta?.(id, update)
    this.bumpState()
    if (this.isConnected(entry) !== wasConnected) this.connectionChanged(entry)
    if (prev === null) return true
    const next = entry.meta.object
    for (const view of listening) view.metaChanged(entry, next, prev)
    return true
  }

  /** Applies updates in `seq` order (see `RoomMetaState.apply`), so every model converges; returns the ones applied.
   *  `prev` is this model's own previous meta, which can differ per model. */
  applyRoomUpdate(update: RoomMetaUpdate): RoomMetaUpdate[] {
    const listening = [...this.views].filter((view) => view.listensToRoomMeta())
    const prev = listening.length > 0 ? this.metaState.object : null
    const applied = this.metaState.apply(update)
    if (applied.length === 0) return applied
    this.bumpState()
    if (prev !== null) for (const view of listening) view.roomMetaChanged(this.metaState.object, prev)
    return applied
  }

  /** Built when read, and kept until the next change. */
  get meta(): RoomMeta {
    return this.metaState.object
  }

  /** In key order, as a snapshot or a whole `update` carries it. */
  metaEntries(): RoomMetaEntry[] {
    return this.metaState.entries()
  }

  /** How many meta writes this model's meta reflects (serialized into room snapshots). */
  get metaSeq(): number {
    return this.metaState.seq
  }

  get metaWaiting(): boolean {
    return this.metaState.waiting
  }

  /** Room closed: member-level cleanup callbacks run (decoders etc.), then `onClose`. Room-level `onLeave`/`onEmpty`
   *  intentionally don't fire: `onClose` is the signal. */
  applyClosed(cause: LeaveCause = { type: 'closed' }): boolean {
    if (this.closed) return false
    cause = ownLeaveCause(cause)
    this._closedCause = cause
    const departed = [...this.members.values()]
    this.closed = true
    this._rosterKnown = true // authoritatively empty
    this.members.clear()
    this.hiddenMembers = 0
    this.bumpState()
    // State and snapshot are already closed-and-empty when cleanup callbacks run.
    for (const entry of departed) {
      entry.left = true
      entry.leaveCause = cause
    }
    for (const view of [...this.views]) view.closed(departed, cause)
    return true
  }

  /** A message's sender as of its send: a stamp newer than the member's meta here applies first, as the meta
   *  update it is. Unstamped, the sender is one this model was sent, maybe in a roster still applying. */
  resolveSender({
    from,
    fromMeta,
    fromMetaSeq,
    fromIdentity,
    fromDisconnected,
  }: { from: string } & Partial<SenderStamp>): { entry: MemberEntry } | { sender: Sender } {
    const entry = this.members.get(from)
    if (fromMeta !== undefined && fromMetaSeq !== undefined) {
      if (!entry) return { sender: senderSnapshot(from, fromMeta, fromIdentity ?? null) }
      if (entry.meta.newerStamp(fromMetaSeq)) {
        this.applyParticipantMeta(from, wholeUpdate(fromMetaSeq, fromMeta, fromDisconnected === true))
      }
      return { entry }
    }
    const pending = this.owner.pendingMember?.(from)
    if (entry) {
      if (pending !== undefined && pending.metaSeq > entry.meta.seq) {
        this.applyParticipantMeta(from, wholeUpdate(pending.metaSeq, pending.meta, pending.disconnected === true))
      }
      return { entry }
    }
    assert(pending !== undefined)
    // With the meta changes relayed after the member's roster part.
    const meta = new MemberMeta({ meta: pending.meta, seq: pending.metaSeq })
    for (const update of this.preRosterMeta.get(from) ?? []) meta.apply(update)
    return { sender: senderSnapshot(from, meta.object, pending.identity ?? null) }
  }

  /** The first roster loads silently; later ones narrate the drift they correct as events. Runs `reconcilingRoster` a
   *  `ROOM_PLAN_SLICE_MS` slice per turn; null if the room closed between slices. */
  async reconcileRoster(roster: RosterRead, events: readonly MembershipEvent[]): Promise<boolean | null> {
    const steps = this.reconcilingRoster(roster, events)
    const slice = new Slice()
    let step = steps.next()
    for (; step.done !== true; step = steps.next()) {
      if (!slice.over) continue
      await slice.next()
      if (this.closed) return null
    }
    return step.value
  }

  /** Reconciles with a roster read a member per step, the membership `events` since the read began, which may grow
   *  between steps, replayed onto it first, so no step undoes one. Returns whether a drift was narrated. */
  *reconcilingRoster(roster: RosterRead, events: readonly MembershipEvent[]): Generator<void, boolean> {
    const listed = new Map<string, MemberSnapshot>()
    for (const member of roster.members) {
      listed.set(member.id, member)
      yield
    }
    const departing = new Map(roster.departing)
    let replayed = 0
    const replay = () => {
      for (; replayed < events.length; replayed++) replayMembershipEvent(listed, departing, events[replayed]!)
    }
    replay()
    // The first roster narrates nothing, and `count` stays the seed until its members are in.
    const narrate = this._rosterKnown
    let narratedDrift = false
    // A live iteration: a member the replay removes is skipped, one it adds is merged.
    for (const member of listed.values()) {
      narratedDrift = this.mergeRosterMember(member, narrate) || narratedDrift
      yield
      replay()
    }
    if (!narrate) {
      this._rosterKnown = true
      this.preRosterMeta.clear()
      this.bumpState()
    }
    for (const id of [...this.members.keys()]) {
      if (this.members.has(id) && !listed.has(id)) {
        // No leave event reached this model: its departing record says why it left. Without one, a model behind the
        // read keeps the member for its event. Any other missed the leave while it didn't follow the room, and the
        // record expired since: its cause is `unknown`.
        const cause = departing.get(id) ?? (roster.behind === true ? undefined : unknownLeaveCause())
        if (cause !== undefined) {
          this.applyLeave(id, cause)
          narratedDrift = true
        }
      }
      yield
      replay()
    }
    return narrate && narratedDrift
  }

  /** Whether the member's drift was narrated as events. */
  private mergeRosterMember(member: MemberSnapshot, narrate: boolean): boolean {
    const entry = this.members.get(member.id)
    if (!entry) {
      if (!narrate) {
        this.createEntry(member)
        return false
      }
      this.applyJoin(member)
      return true
    }
    if (member.metaSeq <= entry.meta.seq) return false
    const update = wholeUpdate(member.metaSeq, member.meta, member.disconnected === true)
    if (narrate) {
      this.applyParticipantMeta(member.id, update)
      return true
    }
    if (entry.meta.apply(update) === 'applied') this.owner.onMemberMeta?.(member.id, update)
    return false
  }

  private createEntry(entrySeed: MemberSnapshot): MemberEntry {
    const entry = newEntry(entrySeed, this.metaRewinds)
    this.members.set(entry.id, entry)
    if (entry.hidden) this.hiddenMembers++
    return entry
  }
}

function newEntry(
  { id, meta, joinedAt, metaSeq, identity, hidden, disconnected }: MemberSnapshot,
  rewinds: MetaRewinds,
): MemberEntry {
  return {
    id,
    meta: new MemberMeta({ meta, seq: metaSeq, rewinds, connected: disconnected !== true }),
    joinedAt,
    identity: identity ?? null,
    hidden: hidden === true,
    left: false,
  }
}

function replayMembershipEvent(
  members: Map<string, MemberSnapshot>,
  departing: Map<string, LeaveCause>,
  event: MembershipEvent,
): void {
  if ('join' in event) {
    if (!members.has(event.join.id)) members.set(event.join.id, event.join)
  } else if ('leave' in event) {
    members.delete(event.leave)
    departing.delete(event.leave)
  } else {
    const { id, update } = event.meta
    const member = members.get(id)
    if (member === undefined) return
    const meta = new MemberMeta({ meta: member.meta, seq: member.metaSeq, connected: member.disconnected !== true })
    if (meta.apply(update) !== 'applied') return
    const { disconnected, ...rest } = member
    const next: MemberSnapshot = { ...rest, meta: meta.object, metaSeq: meta.seq }
    if (!meta.connected) next.disconnected = true
    members.set(id, next)
  }
}

/** A member's whole meta and connection at `seq`, as a roster or a stamp carries them. */
function wholeUpdate(seq: number, meta: ParticipantMeta, disconnected: boolean): WholeMemberUpdate {
  const update: WholeMemberUpdate = { seq, meta }
  if (disconnected) update.disconnected = true
  return update
}
