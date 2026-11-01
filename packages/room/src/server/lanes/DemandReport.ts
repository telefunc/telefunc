export { DemandReport }
export type { DemandReportOptions, DemandReportOwner }

import type { DemandEntry } from '../protocol.js'
import { hasEntry, toggle, toggleEntry } from '../../utils/sets.js'

/** The replica that tells the room each change of its planner's demand. */
interface DemandReportOwner {
  tellDemand(entry: DemandEntry, on: boolean): void
}

type DemandReportOptions = { owner: DemandReportOwner }

/** What some holder of a replica receives. */
class DemandSets {
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
  private readonly planned = new DemandSets()
  /** The planned tracks of every member that are fetched by member. */
  private readonly byMember = new Set<string>()
  private untold = new DemandSets()
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
    this.untold = new DemandSets()
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

function trackEntry(track: string, { except, byMember }: { except: Set<string>; byMember: boolean }): DemandEntry {
  return byMember ? { track, except: [...except], byMember } : { track, except: [...except] }
}
