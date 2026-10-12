export { RoomDemand, DemandReport }

import { DEFAULT_TRACK } from './binary.js'
import { ROOM_DEMAND_TTL_MS } from './constants.js'
import type { DemandEntry, RoomDemandPage } from './protocol.js'
import { Slice } from './slice.js'

/** Each reporter's lease. A remote reporter tells its entries again every heartbeat, so a crashed one lapses; this
 *  process's never do. */
type Leases = Map<string, number>
/** Each reporter's lease on one track of every member, the members it doesn't receive that track from, and whether it
 *  receives it only of the members it fetches by member. */
type TrackLeases = Map<string, { expiresAt: number; except: ReadonlySet<string>; byMember: boolean }>

/** What some holder of a replica receives. */
class Receives {
  readonly pairs = new Map<string, Set<string>>()
  /** The members whose every track. */
  readonly members = new Set<string>()
  /** The tracks of every member. */
  readonly tracks = new Set<string>()
  /** The members whose tracks fetched by member reach this process. */
  readonly fetched = new Set<string>()
}

/** What a replica's planner sets, told as it changes: one track of a member, every track of a member, and one track of
 *  every member less the members no holder here receives it from. */
class DemandReport {
  /** The reports of this process carry it, so its own echo is told apart. */
  readonly instance = crypto.randomUUID()
  private readonly _planned = new Receives()
  /** Per track of every member, the members it isn't received from, set whether or not the track is planned. */
  private readonly _except = new Map<string, Set<string>>()
  /** The planned tracks of every member that are fetched by member. */
  private readonly _byMember = new Set<string>()
  private _changed = new Receives()
  /** A telling a deadline stopped, resumed by the next one. */
  private _telling: Iterator<void> | null = null
  private readonly _told = {
    pairs: new Map<string, Set<string>>(),
    members: new Set<string>(),
    /** Each track told, with the members it was told without and whether it was told fetched by member. */
    tracks: new Map<string, { except: readonly string[]; byMember: boolean }>(),
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
  setTrack(track: string, on: boolean, byMember = false): void {
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
    if (on === hasEntry(this._except, track, member)) return
    toggleEntry(this._except, track, member, on)
    if (this._planned.tracks.has(track)) this._changed.tracks.add(track)
  }

  /** No holder here names `track` of every member any more. */
  forgetTrack(track: string): void {
    this.setTrack(track, false)
    this._except.delete(track)
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
      changed.fetched.size > 0
    )
  }

  /** Tells the planned changes until `deadline`, a `performance.now()` time, and whether all are told; a call after
   *  one the deadline stopped resumes it, so their order holds. */
  tellUntil(deadline: number): boolean {
    const telling = (this._telling ??= this._tellChanges())
    for (let step = telling.next(); !step.done; step = telling.next()) if (performance.now() >= deadline) return false
    this._telling = null
    return true
  }

  /** Tells every entry told again, which renews its leases elsewhere: a step per entry. */
  *heartbeat(): Generator<void> {
    const told = this._told
    for (const [member, tracks] of told.pairs)
      for (const track of tracks) {
        this._publish({ member, track }, true)
        yield
      }
    for (const member of told.members) {
      this._publish({ member }, true)
      yield
    }
    for (const member of told.fetched) {
      this._publish({ member, byMember: true }, true)
      yield
    }
    for (const [track, entry] of told.tracks) {
      this._publish(trackEntry(track, entry), true)
      yield
    }
  }

  /** @internal */
  _publish(entry: DemandEntry, on: boolean): void {
    this._tell(entry, on)
  }

  /** Ons go before offs, so no pair reads as unreceived while one state hands over to another: a track of every member
   *  is told before the pairs it replaces stop, and after the pairs replacing it start; a member it is fetched by is
   *  told before it, and stops after it. A track whose `except` or fetching changed is told again with it. Only a
   *  planning round sets what this reads, and none runs until the telling ends, so it is read as it goes. Each change it
   *  reads is a step. */
  private *_tellChanges(): Generator<void, void, void> {
    const { pairs, members, tracks, fetched } = this._changed
    this._changed = new Receives()
    const planned = this._planned
    const told = this._told
    for (const [member, byTrack] of pairs)
      for (const track of byTrack) {
        if (hasEntry(planned.pairs, member, track) && !hasEntry(told.pairs, member, track)) {
          toggleEntry(told.pairs, member, track, true)
          this._publish({ member, track }, true)
        }
        yield
      }
    for (const member of members) {
      if (planned.members.has(member) && !told.members.has(member)) {
        told.members.add(member)
        this._publish({ member }, true)
      }
      yield
    }
    for (const member of fetched) {
      if (planned.fetched.has(member) && !told.fetched.has(member)) {
        told.fetched.add(member)
        this._publish({ member, byMember: true }, true)
      }
      yield
    }
    for (const track of tracks) {
      if (planned.tracks.has(track)) {
        const entry = { except: [...(this._except.get(track) ?? [])], byMember: this._byMember.has(track) }
        const was = told.tracks.get(track)
        if (was === undefined || was.byMember !== entry.byMember || !sameMembers(was.except, entry.except)) {
          told.tracks.set(track, entry)
          this._publish(trackEntry(track, entry), true)
        }
      }
      yield
    }
    for (const track of tracks) {
      const was = told.tracks.get(track)
      if (!planned.tracks.has(track) && was !== undefined) {
        told.tracks.delete(track)
        this._publish(trackEntry(track, was), false)
      }
      yield
    }
    for (const member of fetched) {
      if (!planned.fetched.has(member) && told.fetched.delete(member)) this._publish({ member, byMember: true }, false)
      yield
    }
    for (const member of members) {
      if (!planned.members.has(member) && told.members.has(member)) {
        told.members.delete(member)
        this._publish({ member }, false)
      }
      yield
    }
    for (const [member, byTrack] of pairs)
      for (const track of byTrack) {
        if (!hasEntry(planned.pairs, member, track) && hasEntry(told.pairs, member, track)) {
          toggleEntry(told.pairs, member, track, false)
          this._publish({ member, track }, false)
        }
        yield
      }
  }
}

/** The demand for the members a process owns: every process's reports, each leased, and one `wanted` boolean per owned
 *  member and track, told once as it flips. */
class RoomDemand {
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
  /** Per track, the owned members told it is wanted. */
  private readonly _told = new Map<string, Set<string>>()
  /** Per track, the owned members whose `wanted` may have changed, or all of them (`null`). */
  private readonly _due = new Map<string, Set<string> | null>()
  private _settling = false

  constructor(
    private readonly _ownsMember: (id: string) => boolean,
    /** Live. */
    private readonly _ownedMembers: () => Iterable<string>,
    private readonly _deliver: (member: string, track: string, wanted: boolean) => void,
  ) {}

  /** Another process's entries. */
  applyPage(page: RoomDemandPage): void {
    const expiresAt = Date.now() + ROOM_DEMAND_TTL_MS
    for (const want of page.wants) this.take(page.instance, want, want.on, expiresAt)
  }

  /** Drops the leases another process renews, read from a demand lane this one no longer subscribes. It owns no member
   *  then, so only track leases are held. */
  forgetLeased(): void {
    for (const [track, leases] of this._tracks) {
      for (const [instance, { expiresAt }] of leases) if (expiresAt !== Infinity) leases.delete(instance)
      if (leases.size === 0) this._tracks.delete(track)
    }
  }

  /** A reporter's entry starts or stops, leased until `expiresAt`; telling it again renews it. Only a member's owner
   *  keeps its pairs; ownership is fixed from before the member exists anywhere. */
  take(instance: string, entry: DemandEntry, on: boolean, expiresAt: number): void {
    if ('except' in entry) this._takeTrack(instance, entry, on, expiresAt)
    else if (!this._ownsMember(entry.member)) return
    else if ('byMember' in entry) {
      const { member } = entry
      const held = this._fetched.get(member)?.has(instance) === true
      lease(this._fetched, member, instance, on, expiresAt)
      if (held !== on) this._queueFetched(instance, member)
    } else if ('track' in entry) {
      const { member, track } = entry
      let byTrack = this._pairs.get(member)
      if (!byTrack) this._pairs.set(member, (byTrack = new Map()))
      if (lease(byTrack, track, instance, on, expiresAt)) this._queue(track, member)
      if (byTrack.size === 0) this._pairs.delete(member)
    } else if (lease(this._members, entry.member, instance, on, expiresAt))
      for (const track of this._everyTrackOf(entry.member)) this._queue(track, entry.member)
    this._settle()
  }

  /** A member this process owns from now on is wanted on each track reported of every member, unless excepted. */
  owned(member: string): void {
    for (const track of this._tracks.keys()) this._queue(track, member)
    this._settle()
  }

  /** The member publishes on `track`, which a report of its every track covers from now on. */
  published(member: string, track: string): void {
    if (track === DEFAULT_TRACK) return
    let tracks = this._published.get(member)
    if (tracks?.has(track)) return
    if (!tracks) this._published.set(member, (tracks = new Set()))
    tracks.add(track)
    if (!this._members.has(member)) return
    this._queue(track, member)
    this._settle()
  }

  /** Drops the leases that lapsed: a step per entry. */
  *expire(): Generator<void> {
    const now = Date.now()
    for (const [member, byTrack] of this._pairs) {
      for (const [track, leases] of byTrack)
        if (lapse(leases, now)) {
          byTrack.delete(track)
          this._queue(track, member)
        }
      if (byTrack.size === 0) this._pairs.delete(member)
      yield
    }
    for (const [member, leases] of this._members) {
      if (lapse(leases, now)) {
        this._members.delete(member)
        for (const track of this._everyTrackOf(member)) this._queue(track, member)
      }
      yield
    }
    for (const [member, leases] of this._fetched) {
      for (const [instance, expiresAt] of leases)
        if (expiresAt <= now) {
          leases.delete(instance)
          this._queueFetched(instance, member)
        }
      if (leases.size === 0) this._fetched.delete(member)
      yield
    }
    for (const [track, leases] of this._tracks) {
      const before = leases.size
      for (const [instance, { expiresAt }] of leases) if (expiresAt <= now) leases.delete(instance)
      if (leases.size === 0) this._tracks.delete(track)
      if (leases.size !== before) this._queue(track, null)
      yield
    }
    this._settle()
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
    for (const [track, members] of this._told)
      if (members.delete(member) && members.size === 0) this._told.delete(track)
  }

  private _takeTrack(
    instance: string,
    entry: Extract<DemandEntry, { except: unknown }>,
    on: boolean,
    expiresAt: number,
  ): void {
    const { track, except } = entry
    const byMember = entry.byMember === true
    let leases = this._tracks.get(track)
    const held = leases?.get(instance)
    if (!on) {
      if (held === undefined) return
      leases!.delete(instance)
      if (leases!.size === 0) this._tracks.delete(track)
      return this._queue(track, null)
    }
    if (held !== undefined && held.byMember === byMember && sameMembers(held.except, except)) {
      held.expiresAt = expiresAt
      return
    }
    if (!leases) this._tracks.set(track, (leases = new Map()))
    leases.set(instance, { expiresAt, except: new Set(except), byMember })
    if (held === undefined || held.byMember !== byMember) return this._queue(track, null)
    for (const member of held.except) if (!except.includes(member)) this._queue(track, member)
    for (const member of except) if (!held.except.has(member)) this._queue(track, member)
  }

  /** The member, on each track `instance` fetches by member. */
  private _queueFetched(instance: string, member: string): void {
    for (const [track, leases] of this._tracks) if (leases.get(instance)?.byMember) this._queue(track, member)
  }

  private _queue(track: string, member: string | null): void {
    const due = this._due.get(track)
    if (due === null) return
    if (member === null) this._due.set(track, null)
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
        for (let step = steps.next(); !step.done; step = steps.next())
          if (slice.over) return void slice.next().then(run)
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
      for (const member of members ?? this._ownedMembers()) {
        this._evaluate(member, track)
        yield
      }
    }
  }

  private _evaluate(member: string, track: string): void {
    if (!this._ownsMember(member)) return
    const wanted = this._wanted(member, track)
    let told = this._told.get(track)
    if (wanted === (told?.has(member) ?? false)) return
    if (wanted) {
      if (!told) this._told.set(track, (told = new Set()))
      told.add(member)
    } else if (told!.delete(member) && told!.size === 0) this._told.delete(track)
    this._deliver(member, track, wanted)
  }

  /** Whether some reporter receives the owned member's track. */
  private _wanted(member: string, track: string): boolean {
    if (this._pairs.get(member)?.has(track)) return true
    const published = track === DEFAULT_TRACK || this._published.get(member)?.has(track) === true
    if (published && this._members.has(member)) return true
    const fetched = this._fetched.get(member)
    for (const [instance, { except, byMember }] of this._tracks.get(track) ?? [])
      if (!except.has(member) && (!byMember || fetched?.has(instance) === true)) return true
    return false
  }

  private _everyTrackOf(member: string): string[] {
    return [DEFAULT_TRACK, ...(this._published.get(member) ?? [])]
  }
}

function trackEntry(
  track: string,
  { except, byMember }: { except: readonly string[]; byMember: boolean },
): DemandEntry {
  return byMember ? { track, except, byMember } : { track, except }
}
/** Takes, renews or drops `instance`'s lease on `key`, and whether `key` went from held to not or back. */
function lease<K>(leases: Map<K, Leases>, key: K, instance: string, on: boolean, expiresAt: number): boolean {
  const held = leases.get(key)
  if (on) {
    if (held) held.set(instance, expiresAt)
    else leases.set(key, new Map([[instance, expiresAt]]))
    return held === undefined
  }
  if (!held?.delete(instance) || held.size > 0) return false
  leases.delete(key)
  return true
}
/** Drops the leases that lapsed by `now`, and whether none is left. */
function lapse(leases: Leases, now: number): boolean {
  for (const [instance, expiresAt] of leases) if (expiresAt <= now) leases.delete(instance)
  return leases.size === 0
}
function sameMembers(a: ReadonlySet<string> | readonly string[], b: readonly string[]): boolean {
  const set = a instanceof Set ? a : new Set(a)
  return set.size === b.length && b.every((member) => set.has(member))
}
function toggle<T>(set: Set<T>, value: T, on: boolean): void {
  if (on) set.add(value)
  else set.delete(value)
}
function hasEntry(entries: Map<string, Set<string>>, key: string, value: string): boolean {
  return entries.get(key)?.has(value) === true
}
function toggleEntry(entries: Map<string, Set<string>>, key: string, value: string, on: boolean): void {
  let values = entries.get(key)
  if (on) {
    if (!values) entries.set(key, (values = new Set()))
    values.add(value)
  } else if (values?.delete(value) && values.size === 0) entries.delete(key)
}
