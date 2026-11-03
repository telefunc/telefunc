export { BinaryLanes }
export type { BinaryLanesOptions, PairShape, Replans }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { encodeLaneKey } from '../../backend/laneKey.js'
import { EVERY_TRACK, type EveryTrack, type TrackKey } from '../../shared/binary.js'
import type { RoomModel } from '../../shared/RoomModel.js'
import { LaneSubscription, type LaneAddress, type LaneSubscriptionOwner } from './LaneSubscription.js'
import { toggle, toggleEntry } from '../../utils/sets.js'
import { withinRoomHorizon } from './lanes.js'
assertIsNotBrowser()

/** The lane that serves a (member, track) pair: its named lane, the member's every-track lane, the track's
 *  every-member lane, or none. */
type PairShape = 'named' | 'every-track' | 'every-member' | null

/** What the lanes' readiness and switches replan: members, tracks, and named pairs of members. */
type Replans = { members: Set<string>; tracks: Set<string>; pairs: Map<string, Set<string>> }

/** A lane of binary frames: a track of a member, every track of a member, or a track of every member. */
type BinaryLane = Extract<LaneAddress, { kind: 'binary' | 'binary-all' | 'binary-track' }>

/** Fan-in lanes of one kind (every-track or every-member) that don't deliver yet. `waiting` holds the members or tracks
 *  whose lanes of the other kind stay open meanwhile; once the last of `lanes` delivers or stops, they're replanned. */
type Establishing = { lanes: Set<LaneSubscription>; waiting: Set<string>; replans: 'members' | 'tracks' }

type BinaryLanesOptions = {
  /** Where every lane reports, as each lane of the room does. */
  laneOwner: LaneSubscriptionOwner
  model: Pick<RoomModel, 'closed'>
}

/** The binary lanes this process subscribes, by shape, and the switch between them: a lane that no longer serves a
 *  pair stays until the lane of the pair's new shape delivers, and a fan-in lane also stays while a fan-in lane of the
 *  other shape is establishing (`ARCHITECTURE.md`, "The lane plan"). A subscription delivers only what is published
 *  after it confirms, so this is what keeps a switch from losing a frame. It answers which lanes deliver, and keeps
 *  what its readiness and its switches replan until the plan takes it; it never calls the plan. */
class BinaryLanes {
  /** The (member, track) lanes; `namedByMember` lists them by member, `namedByTrack` by track. */
  private readonly named = new Map<string, LaneSubscription>()
  private readonly namedByMember = new Map<string, Set<string>>()
  private readonly namedByTrack = new Map<string, Set<string>>()
  /** Each member's every-track lane. */
  private readonly everyTrack = new Map<string, LaneSubscription>()
  /** Each track's every-member lane. */
  private readonly everyMember = new Map<string, LaneSubscription>()
  /** The tracks named of every member that the members' every-track lanes carry, since their every-member lane
   *  retired for them, until it delivers again. */
  private readonly fetchedByMember = new Set<string>()
  /** Every-track lanes not delivering yet, and the tracks whose every-member lanes stay open until they do. */
  private readonly establishingEveryTrack: Establishing = { lanes: new Set(), waiting: new Set(), replans: 'tracks' }
  /** Every-member lanes not delivering yet, and the members whose every-track lanes stay open until they do. */
  private readonly establishingEveryMember: Establishing = { lanes: new Set(), waiting: new Set(), replans: 'members' }
  /** The lanes not ready yet, so a readiness wait never visits the ready ones. */
  private readonly pending = new Set<LaneSubscription>()
  /** The wait for `pending`, until another lane starts pending. */
  private allReadyWait: Promise<void> | null = null
  private readonly replans: Replans = { members: new Set(), tracks: new Set(), pairs: new Map() }

  private readonly laneOwner: LaneSubscriptionOwner
  private readonly model: BinaryLanesOptions['model']

  constructor({ laneOwner, model }: BinaryLanesOptions) {
    this.laneOwner = laneOwner
    this.model = model
  }

  /** Moves what the lanes replanned since the last call into `next`, the plan's next round. */
  takeReplans(next: Replans): void {
    const { members, tracks, pairs } = this.replans
    for (const member of members) next.members.add(member)
    for (const track of tracks) next.tracks.add(track)
    for (const [member, memberTracks] of pairs)
      for (const track of memberTracks) toggleEntry(next.pairs, member, track, true)
    members.clear()
    tracks.clear()
    pairs.clear()
  }

  /** Whether a lane here carries the member's `track`. */
  carries(member: string, track: string): boolean {
    return this.everyTrack.has(member) || this.everyMember.has(track) || this.named.has(namedKey(member, track))
  }

  /** Whether a lane here is the member's own: a named lane of it or its every-track lane. */
  holds(member: string): boolean {
    return this.namedByMember.has(member) || this.everyTrack.has(member)
  }

  /** The members with a lane of their own, which a full pass replans, so the lanes of members gone close too. */
  *members(): Generator<string> {
    yield* this.namedByMember.keys()
    yield* this.everyTrack.keys()
  }

  /** The tracks with an every-member lane, then those fetched by member. */
  *everyMemberTracks(): Generator<string> {
    yield* this.everyMember.keys()
    yield* this.fetchedByMember
  }

  /** Whether a lane here serves a track of every member: its every-member lane, or every-track lanes fetching it. */
  get servesTracks(): boolean {
    return this.everyMember.size > 0 || this.fetchedByMember.size > 0
  }

  get fetchesByMember(): boolean {
    return this.fetchedByMember.size > 0
  }

  isFetchedByMember(track: string): boolean {
    return this.fetchedByMember.has(track)
  }

  hasEveryMemberOf(track: string): boolean {
    return this.everyMember.has(track)
  }

  /** The tracks of the member's named lanes, read live: a lane a switch stops meanwhile drops out. */
  namedTracksOf(member: string): Iterable<string> {
    return this.namedByMember.get(member) ?? []
  }

  everyTrackOf(member: string): LaneSubscription | undefined {
    return this.everyTrack.get(member)
  }

  everyTrackDelivers(member: string): boolean {
    return this.everyTrack.get(member)?.delivering === true
  }

  everyMemberDelivers(track: string): boolean {
    return this.everyMember.get(track)?.delivering === true
  }

  /** Switches the pair's named lane to `shape`: it opens while the pair is named, and stays while another shape serves
   *  the pair and that lane doesn't deliver yet. The named lane that remains, if any. */
  servePair(member: string, track: string, shape: PairShape): LaneSubscription | undefined {
    const key = namedKey(member, track)
    const lane = this.named.get(key)
    const taking = shape === 'every-track' ? this.everyTrack.get(member) : this.everyMember.get(track)
    if (shape === 'named' || (lane !== undefined && shape !== null && taking?.delivering !== true)) {
      const kept = lane ?? this.openNamed(member, track)
      kept.resume()
      return kept
    }
    if (lane === undefined) return undefined
    lane.stop()
    this.named.delete(key)
    toggleEntry(this.namedByMember, member, track, false)
    toggleEntry(this.namedByTrack, track, member, false)
    if (this.everyMember.has(track)) this.replans.tracks.add(track)
    return undefined
  }

  /** The member's every-track lane serves it: it opens, or is subscribed again if its recovery gave up. */
  keepEveryTrack(member: string): void {
    this.establishingEveryMember.waiting.delete(member)
    const lane = this.everyTrack.get(member) ?? this.openFanIn({ kind: 'binary-all', member }, this.everyTrack, member)
    lane.resume()
  }

  /** The track's every-member lane serves it. Whether it delivers. */
  keepEveryMember(track: string): boolean {
    this.establishingEveryTrack.waiting.delete(track)
    const lane = this.everyMember.get(track) ?? this.openFanIn({ kind: 'binary-track', track }, this.everyMember, track)
    lane.resume()
    if (lane.delivering) this.fetchedByMember.delete(track)
    return lane.delivering
  }

  /** Stops the member's every-track lane once the named lanes and every-member lanes replacing it deliver; until
   *  then, each one's readiness replans the member. Whether it stopped. */
  releaseEveryTrack(member: string): boolean {
    const lane = this.everyTrack.get(member)
    if (lane === undefined) return false
    for (const track of this.namedByMember.get(member) ?? []) {
      if (!this.named.get(namedKey(member, track))!.delivering) return false
    }
    if (this.waitsFor(this.establishingEveryMember, member)) return false
    this.everyTrack.delete(member)
    this.stopFanIn(lane, this.establishingEveryTrack)
    return true
  }

  /** Stops the track's every-member lane, if any, once the named lanes and every-track lanes replacing it deliver;
   *  until then, each replacing lane that becomes ready replans the track. Once they do, the track is fetched by
   *  member if `fetched`. Whether they deliver. */
  retireEveryMember(track: string, fetched: boolean): boolean {
    for (const member of this.namedByTrack.get(track) ?? []) {
      if (!this.named.get(namedKey(member, track))!.delivering) return false
    }
    if (this.waitsFor(this.establishingEveryTrack, track)) return false
    const lane = this.everyMember.get(track)
    if (lane !== undefined) {
      this.everyMember.delete(track)
      this.stopFanIn(lane, this.establishingEveryMember)
    }
    toggle(this.fetchedByMember, track, fetched)
    return true
  }

  /** The lanes delivering the member's `track` (or `EVERY_TRACK`) are ready, so a retained read after this
   *  misses no live frame. */
  async pairReady(member: string, track: TrackKey | EveryTrack): Promise<void> {
    const lanes = [this.everyTrack.get(member)]
    if (track !== EVERY_TRACK) {
      lanes.push(this.named.get(namedKey(member, track)))
      lanes.push(this.everyMember.get(track))
    }
    const pending: Promise<void>[] = []
    for (const lane of lanes) if (lane !== undefined && this.pending.has(lane)) pending.push(lane.ready)
    if (pending.length > 0) await withinRoomHorizon(Promise.all(pending))
  }

  /** Every binary lane is ready; concurrent callers share one wait. */
  async allReady(): Promise<void> {
    if (this.pending.size === 0) return
    if (this.allReadyWait === null) {
      const ready = withinRoomHorizon(Promise.all(Array.from(this.pending, (lane) => lane.ready))).then(() => {})
      const clear = () => {
        if (this.allReadyWait === ready) this.allReadyWait = null
      }
      ready.then(clear, clear)
      this.allReadyWait = ready
    }
    await this.allReadyWait
  }

  /** A binary lane went pending, or ready; once a wanted one delivers, it replans what it serves. */
  lanePending(lane: LaneSubscription, pending: boolean): void {
    if (pending) {
      this.pending.add(lane)
      this.allReadyWait = null
      return
    }
    this.pending.delete(lane)
    if (!lane.wanted) return
    const address = lane.address
    switch (address.kind) {
      case 'binary-all':
        this.replans.members.add(address.member)
        return this.established(lane, this.establishingEveryTrack)
      case 'binary-track':
        // The named lanes and every-track lanes it serves stop once it delivers.
        this.replans.tracks.add(address.track)
        for (const member of this.namedByTrack.get(address.track) ?? []) this.replanPair(member, address.track)
        return this.established(lane, this.establishingEveryMember)
      case 'binary':
        // An every-track lane beside it may wait for it to deliver, so it replans the whole member; an every-member
        // lane that holders no longer name waits for it too.
        if (this.everyTrack.has(address.member)) this.replans.members.add(address.member)
        else this.replanPair(address.member, address.track)
        if (this.everyMember.has(address.track)) this.replans.tracks.add(address.track)
    }
  }

  private openNamed(member: string, track: string): LaneSubscription {
    const address = { kind: 'binary', member, track } as const
    const lane = new LaneSubscription({ owner: this.laneOwner, address })
    this.named.set(encodeLaneKey(address), lane)
    toggleEntry(this.namedByMember, member, track, true)
    toggleEntry(this.namedByTrack, track, member, true)
    lane.start()
    return lane
  }

  private openFanIn(address: BinaryLane, lanes: Map<string, LaneSubscription>, key: string): LaneSubscription {
    const lane = new LaneSubscription({ owner: this.laneOwner, address })
    const establishing = address.kind === 'binary-all' ? this.establishingEveryTrack : this.establishingEveryMember
    establishing.lanes.add(lane)
    lanes.set(key, lane)
    lane.start()
    return lane
  }

  /** Whether `item`'s lane stays while a lane of the other kind is not delivering yet: a closed room waits for none. */
  private waitsFor(other: Establishing, item: string): boolean {
    const waits = !this.model.closed && other.lanes.size > 0
    toggle(other.waiting, item, waits)
    return waits
  }

  private stopFanIn(lane: LaneSubscription, establishing: Establishing): void {
    lane.stop()
    this.established(lane, establishing)
  }

  private established(lane: LaneSubscription, establishing: Establishing): void {
    if (!establishing.lanes.delete(lane) || establishing.lanes.size > 0) return
    const replans = this.replans[establishing.replans]
    for (const item of establishing.waiting) replans.add(item)
  }

  private replanPair(member: string, track: string): void {
    toggleEntry(this.replans.pairs, member, track, true)
  }
}

function namedKey(member: string, track: string): string {
  return encodeLaneKey({ kind: 'binary', member, track })
}
