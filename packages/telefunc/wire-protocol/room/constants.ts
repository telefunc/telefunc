export {
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_MEMBER_TTL_MS,
  ROOM_TAIL_HOLD_MAX,
  ROOM_TAIL_HOLD_CODE_UNITS_MAX,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_DEMAND_TTL_MS,
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HORIZON_MS,
  ROOM_EMPTY_TIMEOUT_MS,
  ROOM_DEPARTURE_TIMEOUT_MS,
  ROOM_TRACK_WANTS_MAX,
  ROOM_MEMBER_NAMED_LANES_MAX,
  ROOM_GOSSIP_IN_FLIGHT_MAX,
}
import { MAX_CHANNELS_PER_CONNECTION } from '../constants.js'

const ROOM_HEARTBEAT_INTERVAL_MS = 30_000
// Four heartbeats: a member is reaped only after several renewals in a row were missed.
const ROOM_MEMBER_TTL_MS = ROOM_HEARTBEAT_INTERVAL_MS * 4
// A tail is a bounded recent suffix, not history: enforce both entry count and serialized code units.
const ROOM_TAIL_HOLD_MAX = 256
const ROOM_TAIL_HOLD_CODE_UNITS_MAX = 1024 * 1024
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
// Each named track a room handle subscribes to is a substream, as a channel is: as many as one connection carries.
const ROOM_TRACK_WANTS_MAX = MAX_CHANNELS_PER_CONNECTION
// An instance serves a member from its every-track lane once its holders name more than this many of its tracks.
const ROOM_MEMBER_NAMED_LANES_MAX = 16
// Demand gossip commits an instance keeps in flight; the rest wait as events.
const ROOM_GOSSIP_IN_FLIGHT_MAX = 64
