export { RosterStream }

import { assertIsNotBrowser } from '../utils/assertIsNotBrowser.js'
import { ROOM_ROSTER_PART_MEMBERS } from './constants.js'
import type { MemberSnapshot, RoomRosterEvent } from '../protocol.js'
import { Slice } from '../Slice.js'
import type { MembershipEvent, RoomModel } from '../state.js'
import { reportRoomError } from './errors.js'
import { type RoomStub, unorderedEventText } from './stub.js'
assertIsNotBrowser()

/** A view's roster to its clients: on attach, and again to every client once a roster read corrected a drift. */
class RosterStream {
  /** Stubs whose first roster read failed: the next successful refresh sends them one. */
  private readonly _owed = new Set<RoomStub>()
  /** Each roster part's text for the clients that see no hidden member, at one roster version: one roster's text at
   *  most, until the first stream after a change or the room's teardown. */
  private _texts: { version: number; parts: Array<{ text: string; hidden: boolean }> } | null = null
  /** A roster reconciling into the view, whose drift reaches its clients once it ends. */
  private _reconciling = false
  /** The roster stream each stub is being sent; a newer one replaces it. */
  private readonly _streamOf = new WeakMap<RoomStub, object>()

  private readonly _model: RoomModel
  /** The view's attached stubs; one that leaves it drops out of a stream. */
  private readonly _stubs: ReadonlySet<RoomStub>
  private readonly _ensureRoster: () => Promise<void>

  constructor({
    model,
    stubs,
    ensureRoster,
  }: {
    model: RoomModel
    stubs: ReadonlySet<RoomStub>
    ensureRoster: () => Promise<void>
  }) {
    this._model = model
    this._stubs = stubs
    this._ensureRoster = ensureRoster
  }

  /** Whether a client of the view was sent every member it holds, as it holds them:
   *  no roster reconciles into it unsent. */
  get settled(): boolean {
    return !this._reconciling
  }

  /** Reconciles with a roster read in `ROOM_PLAN_SLICE_MS` slices, replaying `events`, which grow until it ends; false
   *  if the room closed first. Every client gets a roster that corrected a drift; otherwise only the clients still owed
   *  their first. */
  async reconcile(
    roster: { members: MemberSnapshot[]; departing: ReadonlySet<string> },
    events: readonly MembershipEvent[],
  ): Promise<boolean> {
    this._reconciling = this._model.rosterKnown
    try {
      const drifted = await this._model.reconcileRoster(roster, events)
      if (drifted === null) return false
      const recipients = drifted ? [...this._stubs] : [...this._owed]
      this._owed.clear()
      if (recipients.length > 0) void this._stream(recipients).catch(reportRoomError)
      return true
    } finally {
      this._reconciling = false
    }
  }

  /** An attached stub's first roster, once the view knows it; a failed read owes it the next one. */
  sendTo(stub: RoomStub): void {
    void this._ensureRoster()
      .then(() => {
        if (this._stubs.has(stub) && !this._model.closed) return this._stream([stub])
      })
      .catch((error) => {
        reportRoomError(error)
        if (!this._stubs.has(stub) || this._model.closed) return
        stub._relayEvent({ __r: 'roster-error' })
        this._owed.add(stub)
      })
  }

  forget(stub: RoomStub): void {
    this._owed.delete(stub)
  }

  /** The room closed: no stream reuses a text. */
  clear(): void {
    this._texts = null
  }

  /** The roster to `stubs`, `ROOM_ROSTER_PART_MEMBERS` live members per part and a `ROOM_PLAN_SLICE_MS` slice per turn,
   *  so each event relayed between two parts is one the parts after it reflect. A stub sent a newer roster drops out.
   *  While the roster is unchanged since the stream began, each part's text is the one another stream built for it. */
  private async _stream(stubs: RoomStub[]): Promise<void> {
    const stream = {}
    for (const stub of stubs) {
      this._streamOf.set(stub, stream)
      stub._rosterSent = false
    }
    const model = this._model
    const version = model.version
    if (this._texts?.version !== version) this._texts = { version, parts: [] }
    const texts = this._texts.parts
    const ids = model.memberIds()
    const slice = new Slice()
    for (let part = 0; ; part++) {
      const chunk: string[] = []
      let more = true
      while (chunk.length < ROOM_ROSTER_PART_MEMBERS) {
        const next = ids.next()
        if (next.done === true) {
          more = false
          break
        }
        chunk.push(next.value)
      }
      const receiving = stubs.filter((stub) => this._streamOf.get(stub) === stream && this._stubs.has(stub))
      if (receiving.length === 0 || model.closed) return
      // The first roster read adds its members without a change.
      const unchanged = model.rosterKnown && model.version === version
      const built = unchanged ? texts[part] : undefined
      // Any stub's listing is the visible one in a part with no hidden member.
      const members = built?.hidden === false ? null : chunk.map((id) => model.memberSnapshot(id))
      const visible = members?.filter(({ hidden }) => !hidden)
      let visibleText = built?.text
      const textOf = (listed: MemberSnapshot[]) => {
        const event: Extract<RoomRosterEvent, { __r: 'roster' }> = { __r: 'roster', members: listed, part }
        if (more) event.more = more
        return unorderedEventText(event)
      }
      for (const stub of receiving) {
        const listed = members && stub._rosterMembers(members, visible!)
        if (listed && listed !== visible) stub._relayEventText(textOf(listed))
        else stub._relayEventText((visibleText ??= textOf(visible!)))
        if (!more) stub._rosterSent = true
      }
      if (unchanged && built === undefined && visibleText !== undefined) {
        texts[part] = { text: visibleText, hidden: visible!.length < chunk.length }
      }
      if (!more) return
      if (slice.over) await slice.next()
    }
  }
}
