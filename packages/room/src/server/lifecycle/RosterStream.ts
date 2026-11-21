export { RosterStream }
export type { RosterStreamOptions, RosterClient }

import type { PublishBody } from 'telefunc/__internal'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { ROOM_ROSTER_PART_MEMBERS } from '../constants.js'
import type { MemberSnapshot, RoomRosterEvent } from '../../shared/protocol.js'
import { Slice } from '../../shared/Slice.js'
import type { MembershipEvent, RoomModel, RosterRead } from '../../shared/RoomModel.js'
import { reportRoomError } from '../reportRoomError.js'
import { unorderedEventBody } from '../publishBodies.js'
assertIsNotBrowser()

/** One part of a roster stream: its index, its members' ids, and whether a part follows. */
type RosterPart = { index: number; ids: string[]; more: boolean }
/** A part's body for the clients that see no hidden member, and whether the part holds a hidden member. */
type PartBody = { body: PublishBody; hidden: boolean }

/** A client the view's roster and presence events reach. */
interface RosterClient {
  relayEventBody(body: PublishBody): void
  /** A roster part's members this client gets: `visible`, or with the hidden members of `part` it was handed. */
  rosterMembers(part: MemberSnapshot[], visible: MemberSnapshot[]): MemberSnapshot[]
}

type RosterStreamOptions = {
  /** The view whose roster streams. */
  model: RoomModel
  /** The view's clients; one that leaves it drops out of a stream. */
  clients: ReadonlySet<RosterClient>
}

/** A view's roster to its clients: to each once a roster read corrected a drift, and to a client that starts following
 *  the room without one (`sendTo()`). A client of a room's view got the members with its room. */
class RosterStream {
  /** Each roster part's body for the clients that see no hidden member, at one roster version: one roster's bodies at
   *  most, until the first stream after a change or the room's teardown. */
  private bodies: { version: number; parts: PartBody[] } | null = null
  /** A roster reconciling into the view, whose drift reaches its clients once it ends. */
  private reconciling = false
  /** The roster stream each client is being sent, until its last part; a newer one replaces it. */
  private readonly streamOf = new WeakMap<RosterClient, object>()

  private readonly model: RoomModel
  private readonly clients: ReadonlySet<RosterClient>

  constructor({ model, clients }: RosterStreamOptions) {
    this.model = model
    this.clients = clients
  }

  /** Whether a client of the view was sent every member it holds, as it holds them:
   *  no roster reconciles into it unsent. */
  get settled(): boolean {
    return !this.reconciling
  }

  /** Whether `client` was sent every member: no roster streams to it. */
  sentTo(client: RosterClient): boolean {
    return !this.streamOf.has(client)
  }

  /** The roster to a client that follows the room from now on. */
  sendTo(client: RosterClient): void {
    void this.stream([client]).catch(reportRoomError)
  }

  /** Reconciles with a roster read in `ROOM_PLAN_SLICE_MS` slices, replaying `events`, which grow until it ends; false
   *  if the room closed first. Every client gets a roster that corrected a drift. */
  async reconcile(roster: RosterRead, events: readonly MembershipEvent[]): Promise<boolean> {
    this.reconciling = this.model.rosterKnown
    try {
      const drifted = await this.model.reconcileRoster(roster, events)
      if (drifted === null) return false
      if (drifted && this.clients.size > 0) void this.stream([...this.clients]).catch(reportRoomError)
      return true
    } finally {
      this.reconciling = false
    }
  }

  /** The room closed: no stream reuses a body. */
  clear(): void {
    this.bodies = null
  }

  /** The roster to `clients`, `ROOM_ROSTER_PART_MEMBERS` live members per part and a `ROOM_PLAN_SLICE_MS` slice per
   *  turn, so each event relayed between two parts is one the parts after it reflect. A client sent a newer roster drops
   *  out. While the roster is unchanged since the stream began, each part's body is the one another stream built for it. */
  private async stream(clients: RosterClient[]): Promise<void> {
    const stream = this.claim(clients)
    const model = this.model
    const version = model.version
    if (this.bodies?.version !== version) this.bodies = { version, parts: [] }
    const bodies = this.bodies.parts
    const ids = model.memberIds()
    const slice = new Slice()
    for (let index = 0; ; index++) {
      const part = { index, ...takePart(ids) }
      const receiving = clients.filter((client) => this.streamOf.get(client) === stream && this.clients.has(client))
      if (receiving.length === 0 || model.closed) return
      // The first roster read adds its members without a change.
      const unchanged = model.rosterKnown && model.version === version
      const built = this.sendPart(receiving, part, unchanged ? bodies[index] : undefined)
      if (unchanged && built !== undefined) bodies[index] = built
      if (!part.more) return
      if (slice.over) await slice.next()
    }
  }

  /** A new stream for `clients`, which drop out of the one they were being sent. */
  private claim(clients: RosterClient[]): object {
    const stream = {}
    for (const client of clients) this.streamOf.set(client, stream)
    return stream
  }

  /** The part to `clients`: each client's own listing when it differs from the visible one, else the visible one's
   *  body, `built` by another stream or built here once; returns the body built here. */
  private sendPart(clients: RosterClient[], part: RosterPart, built: PartBody | undefined): PartBody | undefined {
    // Any client's listing is the visible one in a part with no hidden member.
    const members = built?.hidden === false ? null : part.ids.map((id) => this.model.memberSnapshot(id))
    const visible = members?.filter(({ hidden }) => !hidden)
    let visibleBody = built?.body
    for (const client of clients) {
      const listed = members && client.rosterMembers(members, visible!)
      if (listed && listed !== visible) client.relayEventBody(rosterBody(listed, part))
      else client.relayEventBody((visibleBody ??= rosterBody(visible!, part)))
      if (!part.more) this.streamOf.delete(client)
    }
    if (built !== undefined || visibleBody === undefined) return undefined
    return { body: visibleBody, hidden: visible!.length < part.ids.length }
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

function rosterBody(members: MemberSnapshot[], { index, more }: RosterPart): PublishBody {
  const event: Extract<RoomRosterEvent, { __r: 'roster' }> = { __r: 'roster', members, part: index }
  if (more) event.more = more
  return unorderedEventBody(event)
}
