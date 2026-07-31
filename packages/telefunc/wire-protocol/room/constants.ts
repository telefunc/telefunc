export {
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_MEMBER_TTL_MS,
  ROOM_TAIL_HOLD_MAX,
  ROOM_TAIL_HOLD_CODE_UNITS_MAX,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_DEMAND_TTL_MS,
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS,
}
const ROOM_HEARTBEAT_INTERVAL_MS = 30_000
// One missed heartbeat plus the eventual-consistency window used by remote membership stores.
const ROOM_MEMBER_TTL_MS = 120_000
// A tail is a bounded recent suffix, not history: enforce both entry count and serialized code units.
const ROOM_TAIL_HOLD_MAX = 256
const ROOM_TAIL_HOLD_CODE_UNITS_MAX = 1024 * 1024
// Each half of the fetched-tail handoff has an independent bounded lease.
const ROOM_TAIL_ATTACH_TIMEOUT_MS = 60_000
// Three heartbeats tolerate two missed demand refreshes before expiring a crashed watcher.
const ROOM_DEMAND_TTL_MS = ROOM_HEARTBEAT_INTERVAL_MS * 3
const ROOM_DM_ACK_TIMEOUT_MS = 60_000
// Room owns the product-policy horizon; raw subscription attempts deliberately have no deadline.
const ROOM_SUBSCRIPTION_TERMINAL_TIMEOUT_MS = 60_000
