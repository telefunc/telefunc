export { PublishedTracks }

import { DEFAULT_TRACK } from '../shared/binary.js'
import { ROOM_MEMBER_TRACKS_MAX } from '../shared/constants.js'
import { hasEntry, toggleEntry } from './sets.js'

/** The named tracks each member this process owns published on, at most `ROOM_MEMBER_TRACKS_MAX` a member. */
class PublishedTracks {
  private readonly byMember = new Map<string, Set<string>>()

  /** Whether the member may publish on `track`: the default track, one it publishes on, or a new one while it publishes
   *  on fewer than `ROOM_MEMBER_TRACKS_MAX` named tracks. */
  admits(member: string, track: string): boolean {
    const tracks = this.byMember.get(member)
    return track === DEFAULT_TRACK || tracks === undefined || tracks.size < ROOM_MEMBER_TRACKS_MAX || tracks.has(track)
  }

  /** The member publishes on `track`; whether it is a named track the member didn't publish on before. */
  add(member: string, track: string): boolean {
    if (track === DEFAULT_TRACK || hasEntry(this.byMember, member, track)) return false
    toggleEntry(this.byMember, member, track, true)
    return true
  }

  /** The default track, or a named one the member published on. */
  has(member: string, track: string): boolean {
    return track === DEFAULT_TRACK || this.byMember.get(member)?.has(track) === true
  }

  namedOf(member: string): Iterable<string> {
    return this.byMember.get(member) ?? []
  }

  forget(member: string): void {
    this.byMember.delete(member)
  }

  clear(): void {
    this.byMember.clear()
  }
}
