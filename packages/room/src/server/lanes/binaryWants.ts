export { emptyBinaryWants, binaryWantsCovers, applyBinaryWantChange, forgetMemberWants }
export type { TrackWants, BinaryWants }

import { EVERY_MEMBER, EVERY_TRACK, type BinaryWantChange, type TrackKey } from '../../shared/binary.js'
import { toggle } from '../../utils/sets.js'

/** Which of a publisher's tracks a holder wants: every track while `all`, and each of `tracks` either way
 *  (`DEFAULT_TRACK` selects the unnamed lane). */
type TrackWants = { all: boolean; tracks: Set<TrackKey> }
/** A member with no wants has no entry. `named` counts the tracks named in all of them. */
type BinaryWants = { everyMember: TrackWants; members: Map<string, TrackWants>; named: number }

function emptyBinaryWants(): BinaryWants {
  return { everyMember: { all: false, tracks: new Set() }, members: new Map(), named: 0 }
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
  let wanted = member === EVERY_MEMBER ? wants.everyMember : wants.members.get(member)
  if (wanted === undefined) {
    if (!on) return false
    wants.members.set(member as string, (wanted = { all: false, tracks: new Set() }))
  }
  if (track === EVERY_TRACK) {
    if (wanted.all === on) return false
    wanted.all = on
  } else {
    if (wanted.tracks.has(track) === on) return false
    toggle(wanted.tracks, track, on)
    wants.named += on ? 1 : -1
  }
  if (member !== EVERY_MEMBER && !wanted.all && wanted.tracks.size === 0) wants.members.delete(member)
  return true
}

/** Drops the wants naming `member`; returns whether it held any. */
function forgetMemberWants(wants: BinaryWants, member: string): boolean {
  const wanted = wants.members.get(member)
  if (wanted === undefined) return false
  wants.named -= wanted.tracks.size
  return wants.members.delete(member)
}
