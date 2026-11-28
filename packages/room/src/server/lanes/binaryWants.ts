export { emptyBinaryWants, binaryWantsCovers, applyBinaryWantChange, binaryWantChangesTo, forgetMemberWants }
export type { TrackWants, BinaryWants }

import {
  EVERY_MEMBER,
  EVERY_TRACK,
  type BinaryWantChange,
  type EveryMember,
  type EveryTrack,
  type TrackKey,
} from '../../shared/binary.js'
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
    if (!on || member === EVERY_MEMBER) return false
    wants.members.set(member, (wanted = { all: false, tracks: new Set() }))
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

/** The changes that make `wants` want exactly `pairs`, the ones turning a want off first. */
function binaryWantChangesTo(wants: BinaryWants, pairs: readonly BinaryWantChange[]): BinaryWantChange[] {
  const wanted = new Map<string | EveryMember, Set<TrackKey | EveryTrack>>()
  for (const { member, track } of pairs) {
    const tracks = wanted.get(member) ?? new Set()
    wanted.set(member, tracks.add(track))
  }
  const offs: BinaryWantChange[] = []
  const turnOffUnwanted = (member: string | EveryMember, { all, tracks }: TrackWants) => {
    const held: Array<TrackKey | EveryTrack> = all ? [EVERY_TRACK, ...tracks] : [...tracks]
    for (const track of held) if (!wanted.get(member)?.delete(track)) offs.push({ member, track, on: false })
  }
  turnOffUnwanted(EVERY_MEMBER, wants.everyMember)
  for (const [member, memberWants] of wants.members) turnOffUnwanted(member, memberWants)
  const ons: BinaryWantChange[] = []
  for (const [member, tracks] of wanted) for (const track of tracks) ons.push({ member, track, on: true })
  return [...offs, ...ons]
}
