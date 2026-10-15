export { emptyBinaryWants, wantsTrack, binaryWantsCovers, applyBinaryWantChange }
export type { TrackWants, BinaryWants }

import { ALL, type BinaryWantChange, type TrackKey } from '../binary.js'

/** Which of a publisher's tracks a holder wants: every track while `all`, and each of `tracks` either way
 *  (`DEFAULT_TRACK` selects the unnamed lane). */
type TrackWants = { all: boolean; tracks: Set<TrackKey> }
/** A member with no wants has no entry. */
type BinaryWants = { everyMember: TrackWants; members: Map<string, TrackWants> }
function emptyBinaryWants(): BinaryWants {
  return { everyMember: { all: false, tracks: new Set() }, members: new Map() }
}
function wantsTrack(wants: TrackWants, track: TrackKey): boolean {
  return wants.all || wants.tracks.has(track)
}
function binaryWantsCovers(wants: BinaryWants, memberId: string, track: TrackKey): boolean {
  if (wantsTrack(wants.everyMember, track)) return true
  const memberWants = wants.members.get(memberId)
  return memberWants !== undefined && wantsTrack(memberWants, track)
}
/** Whether `change` changed `wants`. */
function applyBinaryWantChange(wants: BinaryWants, { member, track, on }: BinaryWantChange): boolean {
  let scope = member === ALL ? wants.everyMember : wants.members.get(member)
  if (scope === undefined) {
    if (!on) return false
    wants.members.set(member as string, (scope = { all: false, tracks: new Set() }))
  }
  if (track === ALL) {
    if (scope.all === on) return false
    scope.all = on
  } else {
    if (scope.tracks.has(track) === on) return false
    if (on) scope.tracks.add(track)
    else scope.tracks.delete(track)
  }
  if (member !== ALL && !scope.all && scope.tracks.size === 0) wants.members.delete(member)
  return true
}
