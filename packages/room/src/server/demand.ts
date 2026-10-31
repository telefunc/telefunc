export { RoomDemand, DemandReport }

import { getOrCreate } from '../utils/getOrCreate.js'
import { ALL, DEFAULT_TRACK, type All } from '../binary.js'
import { ROOM_MEMBER_TRACKS_MAX } from '../constants.js'
import { ROOM_DEMAND_TTL_MS } from './constants.js'
import type { DemandEntry, RoomDemandPage } from './protocol.js'
import { hasEntry, runAll, toggle, toggleEntry } from './sets.js'
import { Slice } from '../Slice.js'

/** The reporters of an entry, each with the telling of everything (`Reporter.telling`) it last told the entry in. */
type Leases = Map<string, number>
/** A reporter's entry of one track of every member: the members it doesn't receive that track from, and whether it
 *  receives it only of the members it fetches by member. */
type TrackLease = { instance: string; telling: number; except: Set<string>; byMember: boolean }
type TrackLeases = Map<string, TrackLease>
/** Another process telling demand: its lease on all it told, renewed by each of its pages, so a crashed one lapses; the
 *  count of its last page, which skips one this process missed; how many times it told everything again; and whether
 *  this process asked it to since. */
type Reporter = { expiresAt: number; page: number; telling: number; asked: boolean }

/** The replica whose owned members the demand is for. */
type DemandHost = {
  ownsMember(id: string): boolean
  /** Live. */
  ownedMembers(): Iterable<string>
  deliver(member: string, track: string, wanted: boolean): void
  /** Asks the reporter to tell everything again. */
  ask(instance: string): void
}

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
  private readonly _planned = new Receives()
  /** The planned tracks of every member that are fetched by member. */
  private readonly _byMember = new Set<string>()
  private _changed = new Receives()
  /** A telling a deadline stopped, resumed by the next one. */
  private _telling: Iterator<void> | null = null

  private readonly _told = {
    pairs: new Map<string, Set<string>>(),
    members: new Set<string>(),
    /** Each track told, with the members it was told without and whether it was told fetched by member. */
    tracks: new Map<string, { except: Set<string>; byMember: boolean }>(),
    fetched: new Set<string>(),
  }

  constructor(private readonly _tell: (entry: DemandEntry, on: boolean) => void) {}

  setPair(member: string, track: string, on: boolean): void {
    if (on === hasEntry(this._planned.pairs, member, track)) return
    toggleEntry(this._planned.pairs, member, track, on)
    toggleEntry(this._changed.pairs, member, track, true)
  }

  /** Whether some holder here receives every track of the member. */
  setMember(member: string, on: boolean): void {
    if (on === this._planned.members.has(member)) return
    toggle(this._planned.members, member, on)
    this._changed.members.add(member)
  }

  /** Whether some holder here receives `track` of every member, and whether only of the members it is fetched by. */
  setTrack(track: string, on: boolean, opts?: { byMember?: boolean }): void {
    const byMember = opts?.byMember === true
    if (on === this._planned.tracks.has(track) && (on && byMember) === this._byMember.has(track)) return
    toggle(this._planned.tracks, track, on)
    toggle(this._byMember, track, on && byMember)
    this._changed.tracks.add(track)
  }

  /** Whether the member's tracks fetched by member reach this process. */
  setFetched(member: string, on: boolean): void {
    if (on === this._planned.fetched.has(member)) return
    toggle(this._planned.fetched, member, on)
    this._changed.fetched.add(member)
  }

  /** Whether no holder here receives `track` from the member, though some holder names it of every member. */
  setExcept(track: string, member: string, on: boolean): void {
    if (on === hasEntry(this._planned.except, track, member)) return
    toggleEntry(this._planned.except, track, member, on)
    toggleEntry(this._changed.except, track, member, true)
  }

  /** No holder here names `track` of every member any more. */
  forgetTrack(track: string): void {
    this.setTrack(track, false)
    for (const member of this._planned.except.get(track) ?? []) this.setExcept(track, member, false)
  }

  /** Whether the member's tracks are told here, as pairs or as every track. */
  receivesFrom(member: string): boolean {
    return this._planned.pairs.has(member) || this._planned.members.has(member)
  }

  /** The member's tracks told as pairs, read live. */
  localTracks(member: string): Iterable<string> {
    return this._planned.pairs.get(member) ?? []
  }

  /** Whether some entry is told. */
  get reporting(): boolean {
    const told = this._told
    return told.pairs.size > 0 || told.members.size > 0 || told.tracks.size > 0 || told.fetched.size > 0
  }

  /** Tells the planned changes, each once. */
  flush(): void {
    this.tellUntil(Infinity)
  }

  /** Whether some planned change isn't told yet. */
  get changed(): boolean {
    const changed = this._changed
    return (
      this._telling !== null ||
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
    const telling = (this._telling ??= this._tellChanges())
    while (!telling.next().done) if (performance.now() >= deadline) return false
    this._telling = null
    return true
  }

  /** Tells every entry told again: a step per entry. */
  *all(): Generator<void> {
    const told = this._told
    for (const [member, tracks] of told.pairs) {
      for (const track of tracks) {
        this._tell({ member, track }, true)
        yield
      }
    }
    for (const member of told.members) {
      this._tell({ member }, true)
      yield
    }
    for (const member of told.fetched) {
      this._tell({ member, byMember: true }, true)
      yield
    }
    for (const [track, entry] of told.tracks) {
      this._tell(trackEntry(track, entry), true)
      yield
    }
  }

  /** Ons go before offs, so no pair reads as unreceived while one state hands over to another: a track of every member
   *  is told before the pairs it replaces stop, and after the pairs replacing it start; a member it is fetched by is
   *  told before it, and stops after it. A track whose fetching changed is told again whole; a member a told track
   *  stops being told without is told with the ons, one it starts being told without with the offs. Only a planning
   *  round sets what this reads, and none runs until the telling ends, so it is read as it goes. Each change it reads
   *  is a step. */
  private *_tellChanges(): Generator<void, void, void> {
    const { pairs, members, tracks, fetched, except } = this._changed
    this._changed = new Receives()
    const planned = this._planned
    const told = this._told
    for (const [member, byTrack] of pairs) {
      for (const track of byTrack) {
        if (hasEntry(planned.pairs, member, track) && !hasEntry(told.pairs, member, track)) {
          toggleEntry(told.pairs, member, track, true)
          this._tell({ member, track }, true)
        }
        yield
      }
    }
    for (const member of members) {
      if (planned.members.has(member) && !told.members.has(member)) {
        told.members.add(member)
        this._tell({ member }, true)
      }
      yield
    }
    for (const member of fetched) {
      if (planned.fetched.has(member) && !told.fetched.has(member)) {
        told.fetched.add(member)
        this._tell({ member, byMember: true }, true)
      }
      yield
    }
    for (const track of tracks) {
      const byMember = this._byMember.has(track)
      if (planned.tracks.has(track) && told.tracks.get(track)?.byMember !== byMember) {
        const entry = { except: new Set(planned.except.get(track)), byMember }
        told.tracks.set(track, entry)
        this._tell(trackEntry(track, entry), true)
      }
      yield
    }
    for (const [track, byTrack] of except) {
      for (const member of byTrack) {
        const was = told.tracks.get(track)
        if (planned.tracks.has(track) && !hasEntry(planned.except, track, member) && was?.except.has(member) === true) {
          was.except.delete(member)
          this._tell({ track, member, except: true }, false)
        }
        yield
      }
    }
    for (const [track, byTrack] of except) {
      for (const member of byTrack) {
        const was = told.tracks.get(track)
        if (planned.tracks.has(track) && hasEntry(planned.except, track, member) && was?.except.has(member) === false) {
          was.except.add(member)
          this._tell({ track, member, except: true }, true)
        }
        yield
      }
    }
    for (const track of tracks) {
      const was = told.tracks.get(track)
      if (!planned.tracks.has(track) && was !== undefined) {
        told.tracks.delete(track)
        this._tell(trackEntry(track, was), false)
      }
      yield
    }
    for (const member of fetched) {
      if (!planned.fetched.has(member) && told.fetched.has(member)) {
        told.fetched.delete(member)
        this._tell({ member, byMember: true }, false)
      }
      yield
    }
    for (const member of members) {
      if (!planned.members.has(member) && told.members.has(member)) {
        told.members.delete(member)
        this._tell({ member }, false)
      }
      yield
    }
    for (const [member, byTrack] of pairs) {
      for (const track of byTrack) {
        if (!hasEntry(planned.pairs, member, track) && hasEntry(told.pairs, member, track)) {
          toggleEntry(told.pairs, member, track, false)
          this._tell({ member, track }, false)
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
  private readonly _reporters = new Map<string, Reporter>()
  /** Per owned member, per track, the reports of that pair. */
  private readonly _pairs = new Map<string, Map<string, Leases>>()
  /** Per owned member, the reports of its every track. */
  private readonly _members = new Map<string, Leases>()
  /** Per owned member, the reporters that fetch its tracks by member. */
  private readonly _fetched = new Map<string, Leases>()
  /** Per track, the reports of it of every member, whoever owns them. */
  private readonly _tracks = new Map<string, TrackLeases>()
  /** The named tracks each member published on; a report of its every track covers them and the default one. */
  private readonly _published = new Map<string, Set<string>>()
  /** Per track, the owned members told it is wanted, kept by track as `_due` is evaluated a track at a time. */
  private readonly _told = new Map<string, Set<string>>()
  /** Per track, the owned members whose `wanted` may have changed, or `ALL` of them. */
  private readonly _due = new Map<string, Set<string> | All>()
  private _settling = false

  constructor(private readonly _host: DemandHost) {}

  /** Another process's page, which renews its lease. A page count that skips one makes this process ask it to tell
   *  everything again, once until it does or its next page is only a renewal; what it tells again replaces what it
   *  told before. */
  applyPage(page: RoomDemandPage): void {
    const { instance } = page
    const reporter = getOrCreate(this._reporters, instance, () => ({ expiresAt: 0, page: 0, telling: 0, asked: false }))
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
        runAll(this._drop((from, told) => from === instance && told < telling))
        this._settle()
      }
    }
    // A renewal while asked means the ask or its answer was lost.
    const asksAgain = reporter.asked ? page.wants.length === 0 : !continues
    if (tellsAgain || !asksAgain) return
    reporter.asked = true
    this._host.ask(instance)
  }

  /** Drops what the other processes told, read from a demand lane this one no longer subscribes. It owns no member
   *  then, so only track leases are held. */
  forgetLeased(): void {
    for (const [track, leases] of this._tracks) {
      for (const instance of leases.keys()) if (this._reporters.has(instance)) leases.delete(instance)
      if (leases.size === 0) this._tracks.delete(track)
    }
    this._reporters.clear()
  }

  /** A reporter's entry starts or stops; another process's lasts while it renews its lease. Only a member's owner
   *  keeps its pairs; ownership is fixed from before the member exists anywhere. */
  take(instance: string, entry: DemandEntry, on: boolean): void {
    const telling = this._reporters.get(instance)?.telling ?? 0
    if ('except' in entry) {
      this._takeTrack(instance, entry, on, telling)
    } else if (!this._host.ownsMember(entry.member)) {
      return
    } else if ('byMember' in entry) {
      const { member } = entry
      const held = this._fetched.get(member)?.has(instance) === true
      lease(this._fetched, member, instance, on, telling)
      if (held !== on) this._queueFetched(instance, member)
    } else if ('track' in entry) {
      const { member, track } = entry
      const byTrack = getOrCreate(this._pairs, member, () => new Map())
      if (lease(byTrack, track, instance, on, telling)) this._queue(track, member)
      if (byTrack.size === 0) this._pairs.delete(member)
    } else if (lease(this._members, entry.member, instance, on, telling)) {
      for (const track of this._everyTrackOf(entry.member)) this._queue(track, entry.member)
    }
    this._settle()
  }

  /** A member this process owns from now on is wanted on each track reported of every member, unless excepted. */
  owned(member: string): void {
    for (const track of this._tracks.keys()) this._queue(track, member)
    this._settle()
  }

  /** Whether the member may publish on `track`: the default track, one it publishes on, or a new one while it publishes
   *  on fewer than `ROOM_MEMBER_TRACKS_MAX` named tracks. */
  admitsTrack(member: string, track: string): boolean {
    const tracks = this._published.get(member)
    return track === DEFAULT_TRACK || tracks === undefined || tracks.size < ROOM_MEMBER_TRACKS_MAX || tracks.has(track)
  }

  /** The member publishes on `track`, which a report of its every track covers from now on. */
  published(member: string, track: string): void {
    if (track === DEFAULT_TRACK) return
    if (hasEntry(this._published, member, track)) return
    toggleEntry(this._published, member, track, true)
    if (!this._members.has(member)) return
    this._queue(track, member)
    this._settle()
  }

  /** Drops the reporters whose lease lapsed, and all they told: a step per entry once one did. */
  *expire(): Generator<void> {
    const now = performance.now()
    const lapsed = new Set<string>()
    for (const [instance, { expiresAt }] of this._reporters) if (expiresAt <= now) lapsed.add(instance)
    if (lapsed.size === 0) return
    for (const instance of lapsed) this._reporters.delete(instance)
    yield* this._drop((instance) => lapsed.has(instance))
    this._settle()
  }

  /** Drops each entry `dropped` names by its reporter and the telling it was last told in: a step per entry. */
  private *_drop(dropped: (instance: string, telling: number) => boolean): Generator<void> {
    for (const [member, byTrack] of this._pairs) {
      for (const [track, leases] of byTrack) {
        if (lapse(leases, dropped)) {
          byTrack.delete(track)
          this._queue(track, member)
        }
      }
      if (byTrack.size === 0) this._pairs.delete(member)
      yield
    }
    for (const [member, leases] of this._members) {
      if (lapse(leases, dropped)) {
        this._members.delete(member)
        for (const track of this._everyTrackOf(member)) this._queue(track, member)
      }
      yield
    }
    for (const [member, leases] of this._fetched) {
      for (const [instance, telling] of leases) {
        if (dropped(instance, telling)) {
          leases.delete(instance)
          this._queueFetched(instance, member)
        }
      }
      if (leases.size === 0) this._fetched.delete(member)
      yield
    }
    for (const [track, leases] of this._tracks) {
      for (const [instance, held] of leases) {
        if (dropped(instance, held.telling)) {
          leases.delete(instance)
          this._queueLease(track, instance, held, undefined)
        }
      }
      if (leases.size === 0) this._tracks.delete(track)
      yield
    }
  }

  /** The owned member's tracks told wanted. */
  wanted(member: string): string[] {
    const tracks: string[] = []
    for (const [track, members] of this._told) if (members.has(member)) tracks.push(track)
    return tracks
  }

  forgetMember(member: string): void {
    this._pairs.delete(member)
    this._members.delete(member)
    this._fetched.delete(member)
    this._published.delete(member)
    for (const [track, members] of this._told) {
      if (members.delete(member) && members.size === 0) this._told.delete(track)
    }
  }

  /** The room closed: it holds no demand from now on, so its members' leaves forget nothing one at a time. */
  close(): void {
    this._reporters.clear()
    this._pairs.clear()
    this._members.clear()
    this._fetched.clear()
    this._tracks.clear()
    this._told.clear()
    this._published.clear()
    this._due.clear()
  }

  private _takeTrack(
    instance: string,
    entry: Extract<DemandEntry, { except: unknown }>,
    on: boolean,
    telling: number,
  ): void {
    const leases = this._tracks.get(entry.track)
    const held = leases?.get(instance)
    // A member's change follows its track's entry; a lease this process lost comes whole with the telling it asks.
    if ('member' in entry) {
      if (held === undefined) return
      toggle(held.except, entry.member, on)
      return this._queue(entry.track, entry.member)
    }
    const { track, except } = entry
    const byMember = entry.byMember === true
    if (!on) {
      if (held === undefined) return
      leases!.delete(instance)
      if (leases!.size === 0) this._tracks.delete(track)
      return this._queueLease(track, instance, held, undefined)
    }
    if (held !== undefined && held.byMember === byMember && sameMembers(held.except, except)) {
      held.telling = telling
      return
    }
    const now = { instance, telling, except: new Set(except), byMember }
    getOrCreate(this._tracks, track, () => new Map()).set(instance, now)
    this._queueLease(track, instance, held, now)
  }

  /** Queues the members whose `track` a change of `instance`'s lease of it from `was` to `now` may have turned: those
   *  another reporter's lease of it of every member leaves out, else those one lease excepts and the other not when
   *  both are fetched alike, else every member. */
  private _queueLease(track: string, instance: string, was: TrackLease | undefined, now: TrackLease | undefined): void {
    for (const [other, lease] of this._tracks.get(track) ?? []) {
      if (other !== instance && !lease.byMember) return this._queueEach(track, lease.except)
    }
    if (was !== undefined && now !== undefined && was.byMember === now.byMember) {
      for (const member of was.except) if (!now.except.has(member)) this._queue(track, member)
      for (const member of now.except) if (!was.except.has(member)) this._queue(track, member)
    } else {
      this._queue(track, ALL)
    }
  }

  /** The member, on each track `instance` fetches by member. */
  private _queueFetched(instance: string, member: string): void {
    for (const [track, leases] of this._tracks) if (leases.get(instance)?.byMember) this._queue(track, member)
  }

  private _queueEach(track: string, members: Iterable<string>): void {
    for (const member of members) this._queue(track, member)
  }

  private _queue(track: string, member: string | All): void {
    const due = this._due.get(track)
    if (due === ALL) return
    if (member === ALL) this._due.set(track, ALL)
    else if (due) due.add(member)
    else this._due.set(track, new Set([member]))
  }

  /** Tells what is due a slice at a time; what falls due meanwhile is told in the same run. */
  private _settle(): void {
    if (this._settling) return
    this._settling = true
    const slice = new Slice()
    const steps = this._evaluations()
    const run = (): void => {
      try {
        while (!steps.next().done) {
          if (slice.over) {
            void slice.next().then(run)
            return
          }
        }
      } catch (error) {
        this._settling = false
        throw error
      }
      this._settling = false
    }
    run()
  }

  private *_evaluations(): Generator<void> {
    for (const [track, members] of this._due) {
      this._due.delete(track)
      for (const member of members === ALL ? this._host.ownedMembers() : members) {
        this._evaluate(member, track)
        yield
      }
    }
  }

  private _evaluate(member: string, track: string): void {
    if (!this._host.ownsMember(member)) return
    const wanted = this._wanted(member, track)
    if (wanted === hasEntry(this._told, track, member)) return
    toggleEntry(this._told, track, member, wanted)
    this._host.deliver(member, track, wanted)
  }

  /** Whether some reporter receives the owned member's track. */
  private _wanted(member: string, track: string): boolean {
    if (this._pairs.get(member)?.has(track)) return true
    const published = track === DEFAULT_TRACK || this._published.get(member)?.has(track) === true
    if (published && this._members.has(member)) return true
    for (const { instance, except, byMember } of this._tracks.get(track)?.values() ?? []) {
      if (!except.has(member) && (!byMember || this._fetched.get(member)?.has(instance) === true)) return true
    }
    return false
  }

  private _everyTrackOf(member: string): string[] {
    return [DEFAULT_TRACK, ...(this._published.get(member) ?? [])]
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
