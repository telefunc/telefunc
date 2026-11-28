export { RoomDemand }
export type { RoomDemandOptions }

import { EVERY_MEMBER, DEFAULT_TRACK, type EveryMember } from '../../shared/binary.js'
import { getOrCreate } from 'telefunc/__internal'
import { ROOM_DEMAND_TTL_MS } from '../constants.js'
import type { DemandEntry, RoomDemandPage, WhoEntry } from '../protocol.js'
import { runAll, setChange, toggle } from '../../utils/sets.js'
import { Slice } from '../../shared/Slice.js'
import type { PublishedTracks } from './PublishedTracks.js'
import { WhoScopes } from './WhoScopes.js'
import type { DemandChange, TrackWatch } from '../../shared/protocol.js'

/** The reporters of an entry, each with the generation (`Reporter.generation`) it last told the entry in. */
type Leases = Map<string, number>
/** A reporter's entry of one track of every member: the members it doesn't receive that track from, and whether it
 *  receives it only of the members it fetches by member. */
type TrackLease = { instance: string; generation: number; except: Set<string>; byMember: boolean }
type TrackLeases = Map<string, TrackLease>
/** `undefined` where `instance` holds no lease. */
type TrackLeaseChange = { instance: string; was: TrackLease | undefined; now: TrackLease | undefined }
/** Another process that tells demand. `expiresAt`: its lease on all it told, which each of its pages renews, so a
 *  crashed process lapses. `page`: the number of its last page, so a page this process missed shows as a gap.
 *  `generation`: how many times it told everything again. `asked`: whether this process asked it to since. */
type Reporter = { expiresAt: number; page: number; generation: number; asked: boolean }
/** A reporter's entry as it told it: started or stopped (`on`), in its `generation`. */
type Report = { instance: string; on: boolean; generation: number }
/** Whether a reporter's lease, last told in `generation`, is dropped. */
type LeaseDropped = (instance: string, generation: number) => boolean
/** A who-entry of one scope (see `WhoEntry`): its page, the participant it names if any, and its reporters. */
type Who = { page: string; by: string | undefined; leases: Leases }
/** A scope's who-entries, by page and participant. */
type WhoScope = Map<string, Who>

/** The handle that owns a member, which hears who receives each of its tracks as it changes. */
type MemberOwner = { deliverDemand(member: string, track: string, change: DemandChange): void }

type RoomDemandOptions = {
  /** The members this process owns, by their owning handle, kept current by the replica. */
  ownedMembers: ReadonlyMap<string, MemberOwner>
  /** A report of a member's every track covers the tracks it published on. */
  publishedTracks: PublishedTracks
}

/** The demand for the members a process owns: every process's reports, each process's leased, and who receives each
 *  owned member's track, told as it changes. Who-entries say who at a page receives what; a reporter's entry counts for
 *  a pair while that reporter's delivery entries say it receives the pair. */
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
  private readonly who = new WhoScopes<Who>()
  /** Per owned member, who receives each of its tracks as last told. */
  private readonly told = new Map<string, Map<string, TrackWatch>>()
  /** Per track, the owned members whose `wanted` may have changed, or `EVERY_MEMBER`: all of them. */
  private readonly due = new Map<string, Set<string> | EveryMember>()
  private settling = false

  private readonly ownedMembers: ReadonlyMap<string, MemberOwner>
  private readonly publishedTracks: PublishedTracks

  constructor({ ownedMembers, publishedTracks }: RoomDemandOptions) {
    this.ownedMembers = ownedMembers
    this.publishedTracks = publishedTracks
  }

  /** Another process's page, which renews its lease, and whether to ask that process to tell everything again: once a
   *  page count skips one, until it does or its next page is only a renewal; what it tells again replaces what it told
   *  before. */
  applyPage(page: RoomDemandPage): boolean {
    const { instance } = page
    const reporter = getOrCreate(this.reporters, instance, () => ({
      expiresAt: 0,
      page: 0,
      generation: 0,
      asked: false,
    }))
    const continues = page.page === reporter.page + 1
    reporter.expiresAt = performance.now() + ROOM_DEMAND_TTL_MS
    reporter.page = page.page
    let tellsAgain = false
    for (const want of page.wants) {
      if (!('again' in want)) {
        this.take(instance, want, want.on)
      } else if (want.again) {
        tellsAgain = true
        reporter.generation++
        reporter.asked = false
      } else {
        const generation = reporter.generation
        runAll(this.drop((from, told) => from === instance && told < generation))
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
    runAll(this.dropWho((instance) => this.reporters.has(instance)))
    this.reporters.clear()
  }

  /** A reporter's entry starts or stops; one from another process lasts while it renews its lease. Only a member's
   *  owner keeps its pairs; ownership is fixed from before the member exists anywhere. */
  take(instance: string, entry: DemandEntry, on: boolean): void {
    const report: Report = { instance, on, generation: this.reporters.get(instance)?.generation ?? 0 }
    if ('page' in entry) {
      this.takeWho(entry, report)
    } else if ('except' in entry) {
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

  /** A member this process owns from now on is received on each track reported of every member, unless excepted. */
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
    yield* this.dropWho(dropped)
  }

  /** A step per scope. */
  private *dropWho(dropped: LeaseDropped): Generator<void> {
    for (const [member, track, scope] of this.who.scopes()) {
      if (lapseWho(scope, dropped)) this.queueCovered(member, track)
      this.who.prune(member, track)
      yield
    }
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
      for (const [instance, generation] of leases) {
        if (dropped(instance, generation)) {
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
        if (dropped(instance, held.generation)) {
          leases.delete(instance)
          this.queueLease(track, { instance, was: held, now: undefined })
        }
      }
      if (leases.size === 0) this.tracks.delete(track)
      yield
    }
  }

  /** Who receives each of the owned member's tracks, as told. */
  watchesOf(member: string): ReadonlyMap<string, TrackWatch> {
    return this.told.get(member) ?? NO_WATCHES
  }

  forgetMember(member: string): void {
    this.pairs.delete(member)
    this.members.delete(member)
    this.fetched.delete(member)
    this.who.forget(member)
    this.told.delete(member)
  }

  /** The room closed: it holds no demand from now on, so its members' leaves forget nothing one at a time. */
  close(): void {
    this.reporters.clear()
    this.pairs.clear()
    this.members.clear()
    this.fetched.clear()
    this.tracks.clear()
    this.who.clear()
    this.told.clear()
    this.due.clear()
  }

  /** A who-entry starts or stops; one naming a member only its owner keeps. */
  private takeWho(entry: WhoEntry, report: Report): void {
    const { member, track } = entry
    if (member !== undefined && !this.ownedMembers.has(member)) return
    if (leaseWho(this.who.of(member, track), entry, report)) this.queueCovered(member, track)
    this.who.prune(member, track)
  }

  /** Queues each owned member's track the scope covers: `undefined` for every one. */
  private queueCovered(member: string | undefined, track: string | undefined): void {
    if (track !== undefined) return this.queue(track, member ?? EVERY_MEMBER)
    for (const owned of member === undefined ? this.ownedMembers.keys() : [member]) {
      for (const ownedTrack of this.everyTrackOf(owned)) this.queue(ownedTrack, owned)
    }
  }

  private takeTrack(entry: Extract<DemandEntry, { except: unknown }>, { instance, on, generation }: Report): void {
    const leases = this.tracks.get(entry.track)
    const held = leases?.get(instance)
    // A member's change follows its track's entry; a lease this process lost comes whole with the generation it asks.
    if ('member' in entry) {
      if (held === undefined) return
      toggle(held.except, entry.member, on)
      return this.queue(entry.track, entry.member)
    }
    const { track, except } = entry
    const byMember = entry.byMember === true
    if (!on) {
      if (leases === undefined || held === undefined) return
      leases.delete(instance)
      if (leases.size === 0) this.tracks.delete(track)
      return this.queueLease(track, { instance, was: held, now: undefined })
    }
    if (held !== undefined && held.byMember === byMember && sameMembers(held.except, except)) {
      held.generation = generation
      return
    }
    const now = { instance, generation, except: new Set(except), byMember }
    getOrCreate(this.tracks, track, () => new Map()).set(instance, now)
    this.queueLease(track, { instance, was: held, now })
  }

  /** Queues the members whose `track` a change of `instance`'s lease of it from `was` to `now` may have turned: those
   *  another reporter's lease of it of every member leaves out, else those one lease excepts and the other not when
   *  both are fetched alike, else every member. */
  private queueLease(track: string, { instance, was, now }: TrackLeaseChange): void {
    for (const [other, lease] of this.tracks.get(track) ?? []) {
      if (other !== instance && !lease.byMember) {
        for (const member of lease.except) this.queue(track, member)
        return
      }
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

  private queue(track: string, member: string | EveryMember): void {
    const due = this.due.get(track)
    if (due === EVERY_MEMBER) return
    if (member === EVERY_MEMBER) this.due.set(track, EVERY_MEMBER)
    else if (due) due.add(member)
    else this.due.set(track, new Set([member]))
  }

  /** Tells what is due a slice at a time, from the next microtask on, so the entries told in one turn are told as one
   *  change; what falls due meanwhile is told in the same run. */
  private settle(): void {
    if (this.settling) return
    this.settling = true
    void this.tellDue()
  }

  private async tellDue(): Promise<void> {
    await Promise.resolve()
    const slice = new Slice()
    const steps = this.evaluations()
    try {
      while (!steps.next().done) {
        if (slice.over) await slice.next()
      }
    } finally {
      this.settling = false
    }
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
    const owner = this.ownedMembers.get(member)
    if (owner === undefined) return
    const watch = this.watchOf(member, track)
    const told = getOrCreate(this.told, member, () => new Map())
    const was = told.get(track)
    if (watch.watchers.size > 0 || watch.pages.size > 0) told.set(track, watch)
    else told.delete(track)
    if (told.size === 0) this.told.delete(member)
    const change = changeOf(was, watch)
    if (change !== null) owner.deliverDemand(member, track, change)
  }

  /** Who receives the owned member's track: the participants each page that receives it names but the member, and the
   *  pages that name none of them. A reporter's who-entry counts where its delivery entries say it receives the pair. */
  private watchOf(member: string, track: string): TrackWatch {
    const people = new Map<string, Set<string>>()
    for (const scope of this.who.covering(member, track, this.publishedTracks.has(member, track))) {
      for (const { page, by, leases } of scope?.values() ?? []) {
        if (!someKey(leases, (instance) => this.receives(instance, member, track))) continue
        const named = getOrCreate(people, page, () => new Set())
        if (by !== undefined && by !== member) named.add(by)
      }
    }
    const watchers = new Set<string>()
    const pages = new Set<string>()
    for (const [page, named] of people) {
      if (named.size === 0) pages.add(page)
      for (const id of named) watchers.add(id)
    }
    return { watchers, pages }
  }

  /** Whether the reporter's delivery entries say it receives the owned member's track. */
  private receives(instance: string, member: string, track: string): boolean {
    if (this.pairs.get(member)?.get(track)?.has(instance)) return true
    if (this.publishedTracks.has(member, track) && this.members.get(member)?.has(instance)) return true
    const lease = this.tracks.get(track)?.get(instance)
    if (lease === undefined || lease.except.has(member)) return false
    return !lease.byMember || this.fetched.get(member)?.has(instance) === true
  }

  private everyTrackOf(member: string): string[] {
    return [DEFAULT_TRACK, ...this.publishedTracks.namedTracks(member)]
  }
}

/** Takes, renews or drops `instance`'s lease on `key`, and whether `key` went from held to not or back. */
function lease<K>(leases: Map<K, Leases>, key: K, { instance, on, generation }: Report): boolean {
  const held = leases.get(key)
  if (on) {
    if (held) held.set(instance, generation)
    else leases.set(key, new Map([[instance, generation]]))
    return held === undefined
  }
  if (!held?.delete(instance) || held.size > 0) return false
  leases.delete(key)
  return true
}

const NO_WATCHES: ReadonlyMap<string, TrackWatch> = new Map()

/** What changed from `was` to `now`, if anything did. */
function changeOf(was: TrackWatch | undefined, now: TrackWatch): DemandChange | null {
  const change: DemandChange = setChange(was?.watchers, now.watchers)
  const pages = setChange(was?.pages, now.pages)
  if (pages.add || pages.remove) change.pages = pages
  return change.add || change.remove || change.pages ? change : null
}

/** Takes, renews or drops a reporter's lease on a who-entry, and whether the entry came or went. */
function leaseWho(scope: WhoScope, { page, by }: WhoEntry, { instance, on, generation }: Report): boolean {
  const key = `${page} ${by ?? ''}`
  const held = scope.get(key)
  if (on) {
    if (held) held.leases.set(instance, generation)
    else scope.set(key, { page, by, leases: new Map([[instance, generation]]) })
    return held === undefined
  }
  if (!held?.leases.delete(instance) || held.leases.size > 0) return false
  scope.delete(key)
  return true
}

/** Drops the leases `dropped` names from a scope's who-entries, and whether an entry went. */
function lapseWho(scope: WhoScope, dropped: LeaseDropped): boolean {
  let went = false
  for (const [key, { leases }] of scope) {
    if (!lapse(leases, dropped)) continue
    scope.delete(key)
    went = true
  }
  return went
}

function someKey<K>(map: Map<K, unknown>, test: (key: K) => boolean): boolean {
  for (const key of map.keys()) if (test(key)) return true
  return false
}

/** Drops the leases `dropped` names, and whether none is left. */
function lapse(leases: Leases, dropped: LeaseDropped): boolean {
  for (const [instance, generation] of leases) if (dropped(instance, generation)) leases.delete(instance)
  return leases.size === 0
}

function sameMembers(a: ReadonlySet<string>, b: readonly string[]): boolean {
  return a.size === b.length && b.every((member) => a.has(member))
}
