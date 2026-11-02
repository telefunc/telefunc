export { RoomDemand }
export type { RoomDemandOptions, RoomDemandOwner }

import { EVERY_MEMBER, DEFAULT_TRACK, type EveryMember } from '../../shared/binary.js'
import { getOrCreate } from 'telefunc/__internal'
import { ROOM_DEMAND_TTL_MS } from '../constants.js'
import type { DemandEntry, RoomDemandPage } from '../protocol.js'
import { hasEntry, runAll, toggle, toggleEntry } from '../../utils/sets.js'
import { Slice } from '../../shared/Slice.js'
import type { PublishedTracks } from './PublishedTracks.js'

/** The reporters of an entry, each with the telling of everything (`Reporter.telling`) it last told the entry in. */
type Leases = Map<string, number>
/** A reporter's entry of one track of every member: the members it doesn't receive that track from, and whether it
 *  receives it only of the members it fetches by member. */
type TrackLease = { instance: string; telling: number; except: Set<string>; byMember: boolean }
type TrackLeases = Map<string, TrackLease>
/** `undefined` where `instance` holds no lease. */
type TrackLeaseChange = { instance: string; was: TrackLease | undefined; now: TrackLease | undefined }
/** Another process that tells demand. `expiresAt`: its lease on all it told, which each of its pages renews, so a
 *  crashed process lapses. `page`: the number of its last page, so a page this process missed shows as a gap.
 *  `telling`: how many times it told everything again. `asked`: whether this process asked it to since. */
type Reporter = { expiresAt: number; page: number; telling: number; asked: boolean }
/** A reporter's entry as it told it: started or stopped (`on`), in its `telling`. */
type Report = { instance: string; on: boolean; telling: number }
/** Whether a reporter's lease, last told in `telling`, is dropped. */
type LeaseDropped = (instance: string, telling: number) => boolean

/** The replica whose owned members the demand is for. */
interface RoomDemandOwner {
  /** Tells an owned member whether a track of it is wanted. */
  deliverDemand(member: string, track: string, wanted: boolean): void
}

type RoomDemandOptions = {
  owner: RoomDemandOwner
  /** The members this process owns, kept current by the replica. */
  ownedMembers: ReadonlyMap<string, unknown>
  /** A report of a member's every track covers the tracks it published on. */
  publishedTracks: Pick<PublishedTracks, 'has' | 'namedTracks'>
}

/** The demand for the members a process owns: every process's reports, each process's leased, and one `wanted` boolean
 *  per owned member and track, told once as it flips. */
class RoomDemand {
  /** The other processes telling demand; this process's entries are never leased. */
  private readonly reporters = new Map<string, Reporter>()
  /** Per owned member, per track, the reports of that pair. */
  private readonly pairs = new Map<string, Map<string, Leases>>()
  /** Per owned member, the reports of its every track. */
  private readonly members = new Map<string, Leases>()
  /** Per owned member, the reporters that fetch its tracks by member. */
  private readonly fetched = new Map<string, Leases>()
  /** Per track, the reports of it of every member, whoever owns them. */
  private readonly tracks = new Map<string, TrackLeases>()
  /** Per track, the owned members told it is wanted, kept by track as `due` is evaluated a track at a time. */
  private readonly told = new Map<string, Set<string>>()
  /** Per track, the owned members whose `wanted` may have changed, or `EVERY_MEMBER`: all of them. */
  private readonly due = new Map<string, Set<string> | EveryMember>()
  private settling = false

  private readonly owner: RoomDemandOwner
  private readonly ownedMembers: ReadonlyMap<string, unknown>
  private readonly publishedTracks: RoomDemandOptions['publishedTracks']

  constructor({ owner, ownedMembers, publishedTracks }: RoomDemandOptions) {
    this.owner = owner
    this.ownedMembers = ownedMembers
    this.publishedTracks = publishedTracks
  }

  /** Another process's page, which renews its lease, and whether to ask that process to tell everything again: once a
   *  page count skips one, until it does or its next page is only a renewal; what it tells again replaces what it told
   *  before. */
  applyPage(page: RoomDemandPage): boolean {
    const { instance } = page
    const reporter = getOrCreate(this.reporters, instance, () => ({ expiresAt: 0, page: 0, telling: 0, asked: false }))
    const continues = page.page === reporter.page + 1
    reporter.expiresAt = performance.now() + ROOM_DEMAND_TTL_MS
    reporter.page = page.page
    let tellsAgain = false
    for (const want of page.wants) {
      if (!('again' in want)) {
        this.take(instance, want, want.on)
      } else if (want.again) {
        tellsAgain = true
        reporter.telling++
        reporter.asked = false
      } else {
        const telling = reporter.telling
        runAll(this.drop((from, told) => from === instance && told < telling))
        this.settle()
      }
    }
    // A renewal while asked means the ask or its answer was lost.
    const asksAgain = reporter.asked ? page.wants.length === 0 : !continues
    if (tellsAgain || !asksAgain) return false
    reporter.asked = true
    return true
  }

  /** Drops what the other processes told, read from a demand lane this one no longer subscribes. It owns no member
   *  then, so only track leases are held. */
  forgetLeased(): void {
    for (const [track, leases] of this.tracks) {
      for (const instance of leases.keys()) if (this.reporters.has(instance)) leases.delete(instance)
      if (leases.size === 0) this.tracks.delete(track)
    }
    this.reporters.clear()
  }

  /** A reporter's entry starts or stops; one from another process lasts while it renews its lease. Only a member's
   *  owner keeps its pairs; ownership is fixed from before the member exists anywhere. */
  take(instance: string, entry: DemandEntry, on: boolean): void {
    const report: Report = { instance, on, telling: this.reporters.get(instance)?.telling ?? 0 }
    if ('except' in entry) {
      this.takeTrack(entry, report)
    } else if (!this.ownedMembers.has(entry.member)) {
      return
    } else if ('byMember' in entry) {
      const { member } = entry
      const held = this.fetched.get(member)?.has(instance) === true
      lease(this.fetched, member, report)
      if (held !== on) this.queueFetched(instance, member)
    } else if ('track' in entry) {
      const { member, track } = entry
      const byTrack = getOrCreate(this.pairs, member, () => new Map())
      if (lease(byTrack, track, report)) this.queue(track, member)
      if (byTrack.size === 0) this.pairs.delete(member)
    } else if (lease(this.members, entry.member, report)) {
      for (const track of this.everyTrackOf(entry.member)) this.queue(track, entry.member)
    }
    this.settle()
  }

  /** A member this process owns from now on is wanted on each track reported of every member, unless excepted. */
  owned(member: string): void {
    for (const track of this.tracks.keys()) this.queue(track, member)
    this.settle()
  }

  /** The member publishes on a named track it didn't before, which a report of its every track covers from now on. */
  published(member: string, track: string): void {
    if (!this.members.has(member)) return
    this.queue(track, member)
    this.settle()
  }

  /** Drops the reporters whose lease lapsed, and all they told: a step per entry once one did. */
  *expire(): Generator<void> {
    const now = performance.now()
    const lapsed = new Set<string>()
    for (const [instance, { expiresAt }] of this.reporters) if (expiresAt <= now) lapsed.add(instance)
    if (lapsed.size === 0) return
    for (const instance of lapsed) this.reporters.delete(instance)
    yield* this.drop((instance) => lapsed.has(instance))
    this.settle()
  }

  /** Drops each entry `dropped` names: a step per entry. */
  private *drop(dropped: LeaseDropped): Generator<void> {
    yield* this.dropPairs(dropped)
    yield* this.dropMembers(dropped)
    yield* this.dropFetched(dropped)
    yield* this.dropTracks(dropped)
  }

  private *dropPairs(dropped: LeaseDropped): Generator<void> {
    for (const [member, byTrack] of this.pairs) {
      for (const [track, leases] of byTrack) {
        if (lapse(leases, dropped)) {
          byTrack.delete(track)
          this.queue(track, member)
        }
      }
      if (byTrack.size === 0) this.pairs.delete(member)
      yield
    }
  }

  private *dropMembers(dropped: LeaseDropped): Generator<void> {
    for (const [member, leases] of this.members) {
      if (lapse(leases, dropped)) {
        this.members.delete(member)
        for (const track of this.everyTrackOf(member)) this.queue(track, member)
      }
      yield
    }
  }

  private *dropFetched(dropped: LeaseDropped): Generator<void> {
    for (const [member, leases] of this.fetched) {
      for (const [instance, telling] of leases) {
        if (dropped(instance, telling)) {
          leases.delete(instance)
          this.queueFetched(instance, member)
        }
      }
      if (leases.size === 0) this.fetched.delete(member)
      yield
    }
  }

  private *dropTracks(dropped: LeaseDropped): Generator<void> {
    for (const [track, leases] of this.tracks) {
      for (const [instance, held] of leases) {
        if (dropped(instance, held.telling)) {
          leases.delete(instance)
          this.queueLease(track, { instance, was: held, now: undefined })
        }
      }
      if (leases.size === 0) this.tracks.delete(track)
      yield
    }
  }

  /** The owned member's tracks told wanted. */
  wanted(member: string): string[] {
    const tracks: string[] = []
    for (const [track, members] of this.told) if (members.has(member)) tracks.push(track)
    return tracks
  }

  forgetMember(member: string): void {
    this.pairs.delete(member)
    this.members.delete(member)
    this.fetched.delete(member)
    for (const [track, members] of this.told) {
      if (members.delete(member) && members.size === 0) this.told.delete(track)
    }
  }

  /** The room closed: it holds no demand from now on, so its members' leaves forget nothing one at a time. */
  close(): void {
    this.reporters.clear()
    this.pairs.clear()
    this.members.clear()
    this.fetched.clear()
    this.tracks.clear()
    this.told.clear()
    this.due.clear()
  }

  private takeTrack(entry: Extract<DemandEntry, { except: unknown }>, { instance, on, telling }: Report): void {
    const leases = this.tracks.get(entry.track)
    const held = leases?.get(instance)
    // A member's change follows its track's entry; a lease this process lost comes whole with the telling it asks.
    if ('member' in entry) {
      if (held === undefined) return
      toggle(held.except, entry.member, on)
      return this.queue(entry.track, entry.member)
    }
    const { track, except } = entry
    const byMember = entry.byMember === true
    if (!on) {
      if (held === undefined) return
      leases!.delete(instance)
      if (leases!.size === 0) this.tracks.delete(track)
      return this.queueLease(track, { instance, was: held, now: undefined })
    }
    if (held !== undefined && held.byMember === byMember && sameMembers(held.except, except)) {
      held.telling = telling
      return
    }
    const now = { instance, telling, except: new Set(except), byMember }
    getOrCreate(this.tracks, track, () => new Map()).set(instance, now)
    this.queueLease(track, { instance, was: held, now })
  }

  /** Queues the members whose `track` a change of `instance`'s lease of it from `was` to `now` may have turned: those
   *  another reporter's lease of it of every member leaves out, else those one lease excepts and the other not when
   *  both are fetched alike, else every member. */
  private queueLease(track: string, { instance, was, now }: TrackLeaseChange): void {
    for (const [other, lease] of this.tracks.get(track) ?? []) {
      if (other !== instance && !lease.byMember) return this.queueEach(track, lease.except)
    }
    if (was !== undefined && now !== undefined && was.byMember === now.byMember) {
      for (const member of was.except) if (!now.except.has(member)) this.queue(track, member)
      for (const member of now.except) if (!was.except.has(member)) this.queue(track, member)
    } else {
      this.queue(track, EVERY_MEMBER)
    }
  }

  /** The member, on each track `instance` fetches by member. */
  private queueFetched(instance: string, member: string): void {
    for (const [track, leases] of this.tracks) if (leases.get(instance)?.byMember) this.queue(track, member)
  }

  private queueEach(track: string, members: Iterable<string>): void {
    for (const member of members) this.queue(track, member)
  }

  private queue(track: string, member: string | EveryMember): void {
    const due = this.due.get(track)
    if (due === EVERY_MEMBER) return
    if (member === EVERY_MEMBER) this.due.set(track, EVERY_MEMBER)
    else if (due) due.add(member)
    else this.due.set(track, new Set([member]))
  }

  /** Tells what is due a slice at a time; what falls due meanwhile is told in the same run. */
  private settle(): void {
    if (this.settling) return
    this.settling = true
    const slice = new Slice()
    const steps = this.evaluations()
    const run = (): void => {
      try {
        while (!steps.next().done) {
          if (slice.over) {
            void slice.next().then(run)
            return
          }
        }
      } catch (error) {
        this.settling = false
        throw error
      }
      this.settling = false
    }
    run()
  }

  private *evaluations(): Generator<void> {
    for (const [track, members] of this.due) {
      this.due.delete(track)
      for (const member of members === EVERY_MEMBER ? this.ownedMembers.keys() : members) {
        this.evaluate(member, track)
        yield
      }
    }
  }

  private evaluate(member: string, track: string): void {
    if (!this.ownedMembers.has(member)) return
    const wanted = this.isWanted(member, track)
    if (wanted === hasEntry(this.told, track, member)) return
    toggleEntry(this.told, track, member, wanted)
    this.owner.deliverDemand(member, track, wanted)
  }

  /** Whether some reporter receives the owned member's track. */
  private isWanted(member: string, track: string): boolean {
    if (this.pairs.get(member)?.has(track)) return true
    if (this.publishedTracks.has(member, track) && this.members.has(member)) return true
    for (const { instance, except, byMember } of this.tracks.get(track)?.values() ?? []) {
      if (!except.has(member) && (!byMember || this.fetched.get(member)?.has(instance) === true)) return true
    }
    return false
  }

  private everyTrackOf(member: string): string[] {
    return [DEFAULT_TRACK, ...this.publishedTracks.namedTracks(member)]
  }
}

/** Takes, renews or drops `instance`'s lease on `key`, and whether `key` went from held to not or back. */
function lease<K>(leases: Map<K, Leases>, key: K, { instance, on, telling }: Report): boolean {
  const held = leases.get(key)
  if (on) {
    if (held) held.set(instance, telling)
    else leases.set(key, new Map([[instance, telling]]))
    return held === undefined
  }
  if (!held?.delete(instance) || held.size > 0) return false
  leases.delete(key)
  return true
}

/** Drops the leases `dropped` names, and whether none is left. */
function lapse(leases: Leases, dropped: LeaseDropped): boolean {
  for (const [instance, telling] of leases) if (dropped(instance, telling)) leases.delete(instance)
  return leases.size === 0
}

function sameMembers(a: ReadonlySet<string>, b: readonly string[]): boolean {
  return a.size === b.length && b.every((member) => a.has(member))
}
