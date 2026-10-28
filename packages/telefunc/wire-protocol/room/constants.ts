export { ROOM_TRACK_WANTS_MAX, ROOM_VIEW_MEMBERS_MAX, ROOM_MEMBER_TRACKS_MAX, ROOM_PLAN_SLICE_MS }
import { MAX_CHANNELS_PER_CONNECTION } from '../../client/__internal.js'

// Each named track a room handle subscribes to is a substream, as a channel is: as many as one connection carries.
const ROOM_TRACK_WANTS_MAX = MAX_CHANNELS_PER_CONNECTION
// The members one room view joins, and the named tracks one member publishes on: each is held on the server until it
// leaves, so one client holds no more of them than a view subscribes to.
const ROOM_VIEW_MEMBERS_MAX = ROOM_TRACK_WANTS_MAX
const ROOM_MEMBER_TRACKS_MAX = ROOM_TRACK_WANTS_MAX
// Lane planning, demand telling, or a heartbeat's renewals and roster read: what one event-loop turn runs before timers
// and I/O get theirs.
const ROOM_PLAN_SLICE_MS = 10
