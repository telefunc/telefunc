export { ROOM_TRACK_WANTS_MAX, ROOM_VIEW_MEMBERS_MAX, ROOM_MEMBER_TRACKS_MAX, ROOM_PLAN_SLICE_MS }
export {
  SERIALIZER_PREFIX_ROOM_STEM,
  SERIALIZER_PREFIX_ROOM,
  SERIALIZER_PREFIX_ROOM_PARTICIPANT,
  SERIALIZER_PREFIX_ROOM_REMOTE,
  TIMER_DELAY_MAX_MS,
}
import { MAX_CHANNELS_PER_CONNECTION } from 'telefunc/__internal'

// Each named track a room handle subscribes to is a substream, as a channel is: as many as one connection carries.
const ROOM_TRACK_WANTS_MAX = MAX_CHANNELS_PER_CONNECTION
// The members one room view joins, and the named tracks one member publishes on: each is held on the server until it
// leaves, so one client holds no more of them than a view subscribes to.
const ROOM_VIEW_MEMBERS_MAX = ROOM_TRACK_WANTS_MAX
const ROOM_MEMBER_TRACKS_MAX = ROOM_TRACK_WANTS_MAX
// Lane planning, demand telling, or a heartbeat's renewals and roster read: what one event-loop turn runs before timers
// and I/O get theirs.
const ROOM_PLAN_SLICE_MS = 10
const SERIALIZER_PREFIX_ROOM_STEM = '!TelefuncRoom'
const SERIALIZER_PREFIX_ROOM = `${SERIALIZER_PREFIX_ROOM_STEM}:`
const SERIALIZER_PREFIX_ROOM_PARTICIPANT = `${SERIALIZER_PREFIX_ROOM_STEM}Participant:`
const SERIALIZER_PREFIX_ROOM_REMOTE = `${SERIALIZER_PREFIX_ROOM_STEM}RemoteParticipant:`
/** The longest delay a timer takes: past it, setTimeout fires at once. */
const TIMER_DELAY_MAX_MS = 2 ** 31 - 1
