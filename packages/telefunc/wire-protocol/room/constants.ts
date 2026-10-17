export { ROOM_TRACK_WANTS_MAX, ROOM_PLAN_SLICE_MS }
import { MAX_CHANNELS_PER_CONNECTION } from '../constants.js'

// Each named track a room handle subscribes to is a substream, as a channel is: as many as one connection carries.
const ROOM_TRACK_WANTS_MAX = MAX_CHANNELS_PER_CONNECTION
// Lane planning, demand telling, or a heartbeat's renewals and roster read: what one event-loop turn runs before timers
// and I/O get theirs.
const ROOM_PLAN_SLICE_MS = 10
