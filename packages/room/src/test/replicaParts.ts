export { replicaParts }

import { RoomModel } from '../shared/RoomModel.js'
import { DemandExchange } from '../server/lanes/DemandExchange.js'
import { LanePlan } from '../server/lanes/LanePlan.js'
import { PageIndex } from '../server/lanes/PageIndex.js'
import { PublishedTracks } from '../server/lanes/PublishedTracks.js'
import { RoomDemand } from '../server/lanes/RoomDemand.js'
import { AuthoritySync, type AuthorityChange } from '../server/lifecycle/AuthoritySync.js'
import { RoomHosting } from '../server/lifecycle/RoomHosting.js'
import { RosterStream } from '../server/lifecycle/RosterStream.js'
import type { RoomIncarnation } from '../server/protocol.js'

/** A server's view of room `incarnation` with no members, its parts wired as `RoomReplica` wires them: a part's spec
 *  drives the others through their own state, or a spy on their class. `applyControl` hears what the authority sync
 *  hands its replica. */
function replicaParts(incarnation: RoomIncarnation, applyControl: (change: AuthorityChange) => void = () => {}) {
  const owner = { onMemberLeft() {}, onMemberMeta() {}, laneDelivered() {}, laneEvent() {}, applyControl }
  const model = new RoomModel({ roomId: incarnation.id, meta: [], seed: { members: [] }, metaSeq: 0, owner })
  const incomingDemand = new RoomDemand({ ownedMembers: new Map(), publishedTracks: new PublishedTracks() })
  const demandExchange = new DemandExchange({ incarnation, incomingDemand })
  const { outgoingDemand } = demandExchange
  const pages = new PageIndex({ model })
  const roles = { observed: false, listened: false, tailHeld: false, owned: new Map() }
  const lanePlan = new LanePlan({ owner, roles, incarnation, model, outgoingDemand, demandExchange, pages })
  const roster = new RosterStream({ model, clients: new Set() })
  const authority = new AuthoritySync({ owner, incarnation, roster, model, lanePlan })
  const hosting = new RoomHosting({ incarnation })
  return { model, lanePlan, outgoingDemand, demandExchange, roster, authority, hosting }
}
