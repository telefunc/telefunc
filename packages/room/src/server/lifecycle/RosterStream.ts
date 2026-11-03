export { RosterStream }
export type { RosterStreamOptions }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { ROOM_ROSTER_PART_MEMBERS } from '../constants.js'
import type { MemberSnapshot, RoomRosterEvent } from '../../shared/protocol.js'
import { Slice } from '../../shared/Slice.js'
import type { MembershipEvent, RoomModel, RosterRead } from '../../shared/RoomModel.js'
import { reportRoomError } from '../reportRoomError.js'
import { type RoomStub, unorderedEventText } from '../RoomStub.js'
assertIsNotBrowser()

/** One part of a roster stream: its index, its members' ids, and whether a part follows. */
type RosterPart = { index: number; ids: string[]; more: boolean }
/** A part's text for the clients that see no hidden member, and whether the part holds a hidden member. */
type PartText = { text: string; hidden: boolean }

type RosterStreamOptions = {
  /** The view whose roster streams. */
  model: RoomModel
  /** The view's attached stubs; one that leaves it drops out of a stream. */
  stubs: ReadonlySet<RoomStub>
}

/** A view's roster to its clients: on attach, and again to every client once a roster read corrected a drift. */
class RosterStream {
  /** Stubs whose first roster read failed: the next successful refresh sends them one. */
  private readonly owed = new Set<RoomStub>()
  /** Each roster part's text for the clients that see no hidden member, at one roster version: one roster's text at
   *  most, until the first stream after a change or the room's teardown. */
  private texts: { version: number; parts: PartText[] } | null = null
  /** A roster reconciling into the view, whose drift reaches its clients once it ends. */
  private reconciling = false
  /** The roster stream each stub is being sent; a newer one replaces it. */
  private readonly streamOf = new WeakMap<RoomStub, object>()

  private readonly model: RoomModel
  private readonly stubs: ReadonlySet<RoomStub>

  constructor({ model, stubs }: RosterStreamOptions) {
    this.model = model
    this.stubs = stubs
  }

  /** Whether a client of the view was sent every member it holds, as it holds them:
   *  no roster reconciles into it unsent. */
  get settled(): boolean {
    return !this.reconciling
  }

  /** Reconciles with a roster read in `ROOM_PLAN_SLICE_MS` slices, replaying `events`, which grow until it ends; false
   *  if the room closed first. Every client gets a roster that corrected a drift; otherwise only the clients still owed
   *  their first. */
  async reconcile(roster: RosterRead, events: readonly MembershipEvent[]): Promise<boolean> {
    this.reconciling = this.model.rosterKnown
    try {
      const drifted = await this.model.reconcileRoster(roster, events)
      if (drifted === null) return false
      const recipients = drifted ? [...this.stubs] : [...this.owed]
      this.owed.clear()
      if (recipients.length > 0) void this.stream(recipients).catch(reportRoomError)
      return true
    } finally {
      this.reconciling = false
    }
  }

  /** An attached stub's first roster, once `known`, the view's roster read, settles; a failed read owes it the next
   *  one. */
  sendTo(stub: RoomStub, known: Promise<void>): void {
    void known
      .then(() => {
        if (this.stubs.has(stub) && !this.model.closed) return this.stream([stub])
      })
      .catch((error) => {
        reportRoomError(error)
        if (!this.stubs.has(stub) || this.model.closed) return
        stub.relayEvent({ __r: 'roster-error' })
        this.owed.add(stub)
      })
  }

  forget(stub: RoomStub): void {
    this.owed.delete(stub)
  }

  /** The room closed: no stream reuses a text. */
  clear(): void {
    this.texts = null
  }

  /** The roster to `stubs`, `ROOM_ROSTER_PART_MEMBERS` live members per part and a `ROOM_PLAN_SLICE_MS` slice per turn,
   *  so each event relayed between two parts is one the parts after it reflect. A stub sent a newer roster drops out.
   *  While the roster is unchanged since the stream began, each part's text is the one another stream built for it. */
  private async stream(stubs: RoomStub[]): Promise<void> {
    const stream = this.claim(stubs)
    const model = this.model
    const version = model.version
    if (this.texts?.version !== version) this.texts = { version, parts: [] }
    const texts = this.texts.parts
    const ids = model.memberIds()
    const slice = new Slice()
    for (let index = 0; ; index++) {
      const part = { index, ...takePart(ids) }
      const receiving = stubs.filter((stub) => this.streamOf.get(stub) === stream && this.stubs.has(stub))
      if (receiving.length === 0 || model.closed) return
      // The first roster read adds its members without a change.
      const unchanged = model.rosterKnown && model.version === version
      const built = this.sendPart(receiving, part, unchanged ? texts[index] : undefined)
      if (unchanged && built !== undefined) texts[index] = built
      if (!part.more) return
      if (slice.over) await slice.next()
    }
  }

  /** A new stream for `stubs`, which drop out of the one they were being sent. */
  private claim(stubs: RoomStub[]): object {
    const stream = {}
    for (const stub of stubs) {
      this.streamOf.set(stub, stream)
      stub.rosterSent = false
    }
    return stream
  }

  /** The part to `stubs`: each stub's own listing when it differs from the visible one, else the visible one's text,
   *  `built` by another stream or built here once; returns the text built here. */
  private sendPart(stubs: RoomStub[], part: RosterPart, built: PartText | undefined): PartText | undefined {
    // Any stub's listing is the visible one in a part with no hidden member.
    const members = built?.hidden === false ? null : part.ids.map((id) => this.model.memberSnapshot(id))
    const visible = members?.filter(({ hidden }) => !hidden)
    let visibleText = built?.text
    for (const stub of stubs) {
      const listed = members && stub.rosterMembers(members, visible!)
      if (listed && listed !== visible) stub.relayEventText(rosterText(listed, part))
      else stub.relayEventText((visibleText ??= rosterText(visible!, part)))
      if (!part.more) stub.rosterSent = true
    }
    if (built !== undefined || visibleText === undefined) return undefined
    return { text: visibleText, hidden: visible!.length < part.ids.length }
  }
}

/** The next `ROOM_ROSTER_PART_MEMBERS` ids, and whether more may follow. */
function takePart(ids: Iterator<string>): { ids: string[]; more: boolean } {
  const part: string[] = []
  while (part.length < ROOM_ROSTER_PART_MEMBERS) {
    const next = ids.next()
    if (next.done === true) return { ids: part, more: false }
    part.push(next.value)
  }
  return { ids: part, more: true }
}

function rosterText(members: MemberSnapshot[], { index, more }: RosterPart): string {
  const event: Extract<RoomRosterEvent, { __r: 'roster' }> = { __r: 'roster', members, part: index }
  if (more) event.more = more
  return unorderedEventText(event)
}
