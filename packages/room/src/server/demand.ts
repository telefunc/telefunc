export { RoomDemand, DemandReport }
export type { DemandReportOptions, DemandReportOwner, RoomDemandOptions, RoomDemandOwner }

import { getOrCreate } from '../utils/getOrCreate.js'
import { EVERY_MEMBER, DEFAULT_TRACK, type EveryMember } from '../shared/binary.js'
import { ROOM_DEMAND_TTL_MS } from './constants.js'
import type { DemandEntry, RoomDemandPage } from './protocol.js'
import { hasEntry, runAll, toggle, toggleEntry } from './sets.js'
import { Slice } from '../shared/Slice.js'
import type { PublishedTracks } from './PublishedTracks.js'

/** The reporters of an entry, each with the telling of everything (`Reporter.telling`) it last told the entry in. */
type Leases = Map<string, number>
/** A reporter's entry of one track of every member: the members it doesn't receive that track from, and whether it
 *  receives it only of the members it fetches by member. */
type TrackLease = { instance: string; telling: number; except: Set<string>; byMember: boolean }
type TrackLeases = Map<string, TrackLease>
/** Another process that tells demand. `expiresAt`: its lease on all it told, which each of its pages renews, so a
 *  crashed process lapses. `page`: the number of its last page, so a page this process missed shows as a gap.
 *  `telling`: how many times it told everything again. `asked`: whether this process asked it to since. */
type Reporter = { expiresAt: number; page: number; telling: number; asked: boolean }

/** The replica whose owned members the demand is for. */
interface RoomDemandOwner {
  ownsMember(id: string): boolean
  /** Live. */
  ownedMembers(): Iterable<string>
  /** Tells an owned member whether a track of it is wanted. */
  deliverDemand(member: string, track: string, wanted: boolean): void
  /** Asks the reporter to tell everything again. */
  askDemand(instance: string): void
}

type RoomDemandOptions = {
  owner: RoomDemandOwner
  /** A report of a member's every track covers the tracks it published on. */
  publishedTracks: Pick<PublishedTracks, 'has' | 'namedOf'>
}

/** The replica that tells the room each change of its planner's demand. */
interface DemandReportOwner {
  tellDemand(entry: DemandEntry, on: boolean): void
}

type DemandReportOptions = { owner: DemandReportOwner }

/** What some holder of a replica receives. */
class Receives {
  readonly pairs = new Map<string, Set<string>>()
  /** The members whose every track is received. */
  readonly members = new Set<string>()
  /** The tracks of every member. */
  readonly tracks = new Set<string>()
  /** The members whose tracks fetched by member reach this process. */
  readonly fetched = new Set<string>()
  /** Per track of every member, the members it isn't received from. */
  readonly except = new Map<string, Set<string>>()
}

/** What a replica's planner sets, told as it changes: one track of a member, every track of a member, and one track of
 *  every member less the members no holder here receives it from. */
class DemandReport {
  /** The reports of this process carry it, so its own echo is told apart. */
  readonly instance = crypto.randomUUID()
  /** Its `except` is set whether or not the track is planned. */
  private readonly planned = new Receives()
  /** The planned tracks of every member that are fetched by member. */
  private readonly byMember = new Set<string>()
  private untold = new Receives()
  /** A telling a deadline stopped, resumed by the next one. */
  private telling: Iterator<void> | null = null

  private readonly told = {
    pairs: new Map<string, Set<string>>(),
    members: new Set<string>(),
    /** Each track told, with the members it was told without and whether it was told fetched by member. */
    tracks: new Map<string, { except: Set<string>; byMember: boolean }>(),
    fetched: new Set<string>(),
  }

  private readonly owner: DemandReportOwner

  constructor({ owner }: DemandReportOptions) {
    this.owner = owner
  }

  setPair(member: string, track: string, on: boolean): void {
    if (on === hasEntry(this.planned.pairs, member, track)) return
    toggleEntry(this.planned.pairs, member, track, on)
    toggleEntry(this.untold.pairs, member, track, true)
  }

  /** Whether some holder here receives every track of the member. */
  setMember(member: string, on: boolean): void {
    if (on === this.planned.members.has(member)) return
    toggle(this.planned.members, member, on)
    this.untold.members.add(member)
  }

  /** Whether some holder here receives `track` of every member, and whether only of the members it is fetched by. */
  setTrack(track: string, on: boolean, opts?: { byMember?: boolean }): void {
    const byMember = opts?.byMember === true
    if (on === this.planned.tracks.has(track) && (on && byMember) === this.byMember.has(track)) return
    toggle(this.planned.tracks, track, on)
    toggle(this.byMember, track, on && byMember)
    this.untold.tracks.add(track)
  }

  /** Whether the member's tracks fetched by member reach this process. */
  setFetched(member: string, on: boolean): void {
    if (on === this.planned.fetched.has(member)) return
    toggle(this.planned.fetched, member, on)
    this.untold.fetched.add(member)
  }

  /** Whether no holder here receives `track` from the member, though some holder names it of every member. */
  setExcept(track: string, member: string, on: boolean): void {
    if (on === hasEntry(this.planned.except, track, member)) return
    toggleEntry(this.planned.except, track, member, on)
    toggleEntry(this.untold.except, track, member, true)
  }

  /** No holder here names `track` of every member any more. */
  forgetTrack(track: string): void {
    this.setTrack(track, false)
    for (const member of this.planned.except.get(track) ?? []) this.setExcept(track, member, false)
  }

  /** Whether the member's tracks are told here, as pairs or as every track. */
  receivesFrom(member: string): boolean {
    return this.planned.pairs.has(member) || this.planned.members.has(member)
  }

  /** The member's tracks told as pairs, read live. */
  localTracks(member: string): Iterable<string> {
    return this.planned.pairs.get(member) ?? []
  }

  /** Whether some entry is told. */
  get reporting(): boolean {
    const told = this.told
    return told.pairs.size > 0 || told.members.size > 0 || told.tracks.size > 0 || told.fetched.size > 0
  }

  /** Tells the planned changes, each once. */
  flush(): void {
    this.tellUntil(Infinity)
  }

  /** Whether some planned change isn't told yet. */
  get changed(): boolean {
    const changed = this.untold
    return (
      this.telling !== null ||
      changed.pairs.size > 0 ||
      changed.members.size > 0 ||
      changed.tracks.size > 0 ||
      changed.fetched.size > 0 ||
      changed.except.size > 0
    )
  }

  /** Tells the planned changes until `deadline`, a `performance.now()` time, and whether all are told; a call after
   *  one the deadline stopped resumes it, so their order holds. */
  tellUntil(deadline: number): boolean {
    const telling = (this.telling ??= this.tellChanges())
    while (!telling.next().done) if (performance.now() >= deadline) return false
    this.telling = null
    return true
  }

  /** Tells every entry told again: a step per entry. */
  *all(): Generator<void> {
    const told = this.told
    for (const [member, tracks] of told.pairs) {
      for (const track of tracks) {
        this.owner.tellDemand({ member, track }, true)
        yield
      }
    }
    for (const member of told.members) {
      this.owner.tellDemand({ member }, true)
      yield
    }
    for (const member of told.fetched) {
      this.owner.tellDemand({ member, byMember: true }, true)
      yield
    }
    for (const [track, entry] of told.tracks) {
      this.owner.tellDemand(trackEntry(track, entry), true)
      yield
    }
  }

  /** Tells the untold changes, ons before offs, so no pair reads as unreceived while one state hands over to another. A
   *  track of every member is told before the pairs it replaces stop, and after the pairs that replace it start; a
   *  member whose every-track lane fetches the track is told before the track, and stops after it. A track whose
   *  fetching changed is told again whole. A member that a told track stops leaving out is told with the ons; a member
   *  that a told track starts leaving out is told with the offs. Only a planning round sets what this reads, and no
   *  round runs until the telling ends, so this reads it as it goes. Each change it reads is one step. */
  private *tellChanges(): Generator<void, void, void> {
    const { pairs, members, tracks, fetched, except } = this.untold
    this.untold = new Receives()
    const planned = this.planned
    const told = this.told
    for (const [member, byTrack] of pairs) {
      for (const track of byTrack) {
        if (hasEntry(planned.pairs, member, track) && !hasEntry(told.pairs, member, track)) {
          toggleEntry(told.pairs, member, track, true)
          this.owner.tellDemand({ member, track }, true)
        }
        yield
      }
    }
    for (const member of members) {
      if (planned.members.has(member) && !told.members.has(member)) {
        told.members.add(member)
        this.owner.tellDemand({ member }, true)
      }
      yield
    }
    for (const member of fetched) {
      if (planned.fetched.has(member) && !told.fetched.has(member)) {
        told.fetched.add(member)
        this.owner.tellDemand({ member, byMember: true }, true)
      }
      yield
    }
    for (const track of tracks) {
      const byMember = this.byMember.has(track)
      if (planned.tracks.has(track) && told.tracks.get(track)?.byMember !== byMember) {
        const entry = { except: new Set(planned.except.get(track)), byMember }
        told.tracks.set(track, entry)
        this.owner.tellDemand(trackEntry(track, entry), true)
      }
      yield
    }
    for (const [track, byTrack] of except) {
      for (const member of byTrack) {
        const was = told.tracks.get(track)
        if (planned.tracks.has(track) && !hasEntry(planned.except, track, member) && was?.except.has(member) === true) {
          was.except.delete(member)
          this.owner.tellDemand({ track, member, except: true }, false)
        }
        yield
      }
    }
    for (const [track, byTrack] of except) {
      for (const member of byTrack) {
        const was = told.tracks.get(track)
        if (planned.tracks.has(track) && hasEntry(planned.except, track, member) && was?.except.has(member) === false) {
          was.except.add(member)
          this.owner.tellDemand({ track, member, except: true }, true)
        }
        yield
      }
    }
    for (const track of tracks) {
      const was = told.tracks.get(track)
      if (!planned.tracks.has(track) && was !== undefined) {
        told.tracks.delete(track)
        this.owner.tellDemand(trackEntry(track, was), false)
      }
      yield
    }
    for (const member of fetched) {
      if (!planned.fetched.has(member) && told.fetched.has(member)) {
        told.fetched.delete(member)
        this.owner.tellDemand({ member, byMember: true }, false)
      }
      yield
    }
    for (const member of members) {
      if (!planned.members.has(member) && told.members.has(member)) {
        told.members.delete(member)
        this.owner.tellDemand({ member }, false)
      }
      yield
    }
    for (const [member, byTrack] of pairs) {
      for (const track of byTrack) {
        if (!hasEntry(planned.pairs, member, track) && hasEntry(told.pairs, member, track)) {
          toggleEntry(told.pairs, member, track, false)
          this.owner.tellDemand({ member, track }, false)
        }
        yield
      }
    }
  }
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
  private readonly publishedTracks: RoomDemandOptions['publishedTracks']

  constructor({ owner, publishedTracks }: RoomDemandOptions) {
    this.owner = owner
    this.publishedTracks = publishedTracks
  }

  /** Another process's page, which renews its lease. A page count that skips one makes this process ask it to tell
   *  everything again, once until it does or its next page is only a renewal; what it tells again replaces what it
   *  told before. */
  applyPage(page: RoomDemandPage): void {
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
    if (tellsAgain || !asksAgain) return
    reporter.asked = true
    this.owner.askDemand(instance)
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

  /** A reporter's entry starts or stops; another process's lasts while it renews its lease. Only a member's owner
   *  keeps its pairs; ownership is fixed from before the member exists anywhere. */
  take(instance: string, entry: DemandEntry, on: boolean): void {
    const telling = this.reporters.get(instance)?.telling ?? 0
    if ('except' in entry) {
      this.takeTrack(instance, entry, on, telling)
    } else if (!this.owner.ownsMember(entry.member)) {
      return
    } else if ('byMember' in entry) {
      const { member } = entry
      const held = this.fetched.get(member)?.has(instance) === true
      lease(this.fetched, member, instance, on, telling)
      if (held !== on) this.queueFetched(instance, member)
    } else if ('track' in entry) {
      const { member, track } = entry
      const byTrack = getOrCreate(this.pairs, member, () => new Map())
      if (lease(byTrack, track, instance, on, telling)) this.queue(track, member)
      if (byTrack.size === 0) this.pairs.delete(member)
    } else if (lease(this.members, entry.member, instance, on, telling)) {
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

  /** Drops each entry `dropped` names by its reporter and the telling it was last told in: a step per entry. */
  private *drop(dropped: (instance: string, telling: number) => boolean): Generator<void> {
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
    for (const [member, leases] of this.members) {
      if (lapse(leases, dropped)) {
        this.members.delete(member)
        for (const track of this.everyTrackOf(member)) this.queue(track, member)
      }
      yield
    }
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
    for (const [track, leases] of this.tracks) {
      for (const [instance, held] of leases) {
        if (dropped(instance, held.telling)) {
          leases.delete(instance)
          this.queueLease(track, instance, held, undefined)
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

  private takeTrack(
    instance: string,
    entry: Extract<DemandEntry, { except: unknown }>,
    on: boolean,
    telling: number,
  ): void {
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
      return this.queueLease(track, instance, held, undefined)
    }
    if (held !== undefined && held.byMember === byMember && sameMembers(held.except, except)) {
      held.telling = telling
      return
    }
    const now = { instance, telling, except: new Set(except), byMember }
    getOrCreate(this.tracks, track, () => new Map()).set(instance, now)
    this.queueLease(track, instance, held, now)
  }

  /** Queues the members whose `track` a change of `instance`'s lease of it from `was` to `now` may have turned: those
   *  another reporter's lease of it of every member leaves out, else those one lease excepts and the other not when
   *  both are fetched alike, else every member. */
  private queueLease(track: string, instance: string, was: TrackLease | undefined, now: TrackLease | undefined): void {
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
      for (const member of members === EVERY_MEMBER ? this.owner.ownedMembers() : members) {
        this.evaluate(member, track)
        yield
      }
    }
  }

  private evaluate(member: string, track: string): void {
    if (!this.owner.ownsMember(member)) return
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
    return [DEFAULT_TRACK, ...this.publishedTracks.namedOf(member)]
  }
}

function trackEntry(track: string, { except, byMember }: { except: Set<string>; byMember: boolean }): DemandEntry {
  return byMember ? { track, except: [...except], byMember } : { track, except: [...except] }
}

/** Takes, renews or drops `instance`'s lease on `key`, and whether `key` went from held to not or back. */
function lease<K>(leases: Map<K, Leases>, key: K, instance: string, on: boolean, telling: number): boolean {
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
function lapse(leases: Leases, dropped: (instance: string, telling: number) => boolean): boolean {
  for (const [instance, telling] of leases) if (dropped(instance, telling)) leases.delete(instance)
  return leases.size === 0
}

function sameMembers(a: ReadonlySet<string>, b: readonly string[]): boolean {
  return a.size === b.length && b.every((member) => a.has(member))
}
