export { RosterIntake }
export type { RosterIntakeOptions }

import type { MemberSnapshot, RoomRosterEvent } from '../shared/protocol.js'
import type { MembershipEvent, RoomModel } from '../shared/RoomModel.js'

type RosterIntakeOptions = {
  model: RoomModel
  /** A roster applied: it holds whole each member's meta, those of the page's own members too. */
  onApplied: () => void
}

/** The rosters streaming in or applying, from the first part of the first until the last applies. */
type Rosters = {
  /** The roster streaming in: its members so far and the part due next. */
  parts: { members: MemberSnapshot[]; next: number } | null
  /** The members of the rosters streaming in or applying, by id, which an unstamped message may name. */
  pending: Map<string, MemberSnapshot>
  /** The membership events since the stream's first part. Each roster replays them all: those from before its own
   *  first part change nothing it lists. */
  events: readonly MembershipEvent[]
  /** The rosters received and not yet applied, applied in turn. */
  applying: number
  applied: Promise<void>
}

/** The rosters a page's server streams to it in parts, each applied to its model once whole, in turn. */
class RosterIntake {
  private rosters: Rosters | null = null
  private readonly model: RoomModel
  private readonly onApplied: () => void

  constructor({ model, onApplied }: RosterIntakeOptions) {
    this.model = model
    this.onApplied = onApplied
  }

  /** A member of a roster streaming in or applying, which an unstamped message may name. */
  pending(id: string): MemberSnapshot | undefined {
    return this.rosters?.pending.get(id)
  }

  part({ members, part, more }: Extract<RoomRosterEvent, { __r: 'roster' }>): void {
    if (part === 0) {
      this.rosters ??= {
        parts: null,
        pending: new Map(),
        events: this.model.recordMembership(),
        applying: 0,
        applied: Promise.resolve(),
      }
      this.rosters.parts = { members: [], next: 0 }
    }
    const rosters = this.rosters
    if (rosters === null) return
    const parts = rosters.parts
    // A part lost while offline: the next attach sends the whole roster again.
    if (parts === null || part !== parts.next) {
      rosters.parts = null
      this.stopRecording()
      return
    }
    for (const member of members) {
      parts.members.push(member)
      rosters.pending.set(member.id, member)
    }
    parts.next++
    if (more) return
    rosters.parts = null
    const apply = () => this.apply(rosters, parts.members)
    rosters.applying++
    rosters.applied = rosters.applying === 1 ? apply() : rosters.applied.then(apply)
  }

  private async apply(rosters: Rosters, members: MemberSnapshot[]): Promise<void> {
    try {
      await this.model.reconcileRoster({ members }, rosters.events)
      this.onApplied()
    } finally {
      rosters.applying--
      this.stopRecording()
    }
  }

  private stopRecording(): void {
    const rosters = this.rosters
    if (rosters === null || rosters.parts !== null || rosters.applying > 0) return
    this.rosters = null
    this.model.stopRecordingMembership()
  }
}
