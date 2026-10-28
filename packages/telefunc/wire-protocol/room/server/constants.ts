export {
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_SESSION_TTL_MS,
  ROOM_TAIL_HOLD_MAX,
  ROOM_TAIL_HOLD_CODE_UNITS_MAX,
  ROOM_REPLAY_HOLD_BYTES_MAX,
  ROOM_REPLAY_HOLD_ENTRY_BYTES,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_DEMAND_TTL_MS,
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HORIZON_MS,
  ROOM_EMPTY_TIMEOUT_MS,
  ROOM_DEPARTURE_TIMEOUT_MS,
  ROOM_MEMBER_NAMED_LANES_MAX,
  ROOM_GOSSIP_IN_FLIGHT_MAX,
  ROOM_MEMBER_WRITES_IN_FLIGHT_MAX,
  ROOM_ROSTER_PART_MEMBERS,
  ROOM_DEMAND_PAGE_ENTRIES,
  ROOM_META_REPAIR_MS,
  ROOM_TOMBSTONE_TTL_MS,
  ROOM_CLOSE_LEASE_MS,
  ROOM_LAPSE_AFTER_CLOSE_MS,
}

const ROOM_HEARTBEAT_INTERVAL_MS = 30_000
// Four heartbeats: a server's session in a room lapses only after several renewals in a row were missed.
const ROOM_SESSION_TTL_MS = ROOM_HEARTBEAT_INTERVAL_MS * 4
// A tail is a bounded recent suffix, not history: enforce both entry count and serialized code units.
const ROOM_TAIL_HOLD_MAX = 256
const ROOM_TAIL_HOLD_CODE_UNITS_MAX = 1024 * 1024
// The live frames a holder's replays hold back; past it they go out at once, ahead of the retained frames they follow.
const ROOM_REPLAY_HOLD_BYTES_MAX = 1024 * 1024
// What holding a frame retains besides its bytes (178 bytes measured), so empty frames count too.
const ROOM_REPLAY_HOLD_ENTRY_BYTES = 256
// Each half of the fetched-tail handoff has an independent bounded lease.
const ROOM_TAIL_ATTACH_TIMEOUT_MS = 60_000
// Three heartbeats tolerate two missed demand refreshes before expiring a crashed watcher.
const ROOM_DEMAND_TTL_MS = ROOM_HEARTBEAT_INTERVAL_MS * 3
const ROOM_DM_ACK_TIMEOUT_MS = 60_000
/** The horizon: how long Room waits for its lanes' readiness, a delivery handoff, or a lost lane's head read; driver
 *  attempts have none. */
const ROOM_HORIZON_MS = 60_000
// LiveKit's defaults: an unheld room nobody joined closes after 5 minutes, one a member had joined after 20 seconds.
const ROOM_EMPTY_TIMEOUT_MS = 300_000
const ROOM_DEPARTURE_TIMEOUT_MS = 20_000
// An instance serves a member from its every-track lane once its holders name more than this many of its tracks.
const ROOM_MEMBER_NAMED_LANES_MAX = 16
// Demand pages an instance keeps in flight; the rest wait as entries.
const ROOM_GOSSIP_IN_FLIGHT_MAX = 64
// Member and view writes, and member meta reads, kept in flight: a pass costs a round trip per 64 members.
const ROOM_MEMBER_WRITES_IN_FLIGHT_MAX = 64
// The members in one frame of a roster, so neither end stringifies or parses a whole room in one turn.
const ROOM_ROSTER_PART_MEMBERS = 1024
// The demand entries in one frame, likewise for a heartbeat's whole report.
const ROOM_DEMAND_PAGE_ENTRIES = 1024
// A meta change or a control frame waiting this long for the one before it: that one's writer crashed between its
// exchange and its publish, or the lane lost it, so a whole read repairs it.
const ROOM_META_REPAIR_MS = 1_000
const ROOM_TOMBSTONE_TTL_MS = 60_000
const ROOM_CLOSE_LEASE_MS = 15_000
/** How long past an unheld room's close its head lapses: the close that server's timer starts lands well before. */
const ROOM_LAPSE_AFTER_CLOSE_MS = 5_000
