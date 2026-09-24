export {
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_MEMBER_TTL_MS,
  ROOM_TAIL_HOLD_MAX,
  ROOM_TAIL_HOLD_CODE_UNITS_MAX,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_DEMAND_TTL_MS,
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HORIZON_MS,
  ROOM_WANTED_TRACKS_MAX,
}
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
/** The horizon: Room's one bound on waiting for a subscription or a delivery; driver attempts have none. */
const ROOM_HORIZON_MS = 60_000
// Each named track a subscriber wants opens a lane before its first frame, so a client can't name unbounded tracks.
const ROOM_WANTED_TRACKS_MAX = 16
