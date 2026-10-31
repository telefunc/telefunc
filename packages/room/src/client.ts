export type {
  ClientRoom as Room,
  RoomInfo,
  RoomMeta,
  ClientJoinOptions as JoinOptions,
  PublishOptions,
  ParticipantMeta,
  LocalParticipant,
  RemoteParticipant,
  Sender,
  RoomSendReceipt,
  RoomAckReceipt,
  LeaveCause,
  BinaryFrameInfo,
  BinaryPublishOptions,
  RoomSnapshotView,
  ParticipantSnapshotView,
} from './types.js'

import { config } from 'telefunc/client'
import { SERIALIZER_PREFIX_ROOM_STEM } from './constants.js'

// Keeps Room's client code out of the entry bundle: only a response whose text holds a Room prefix loads it.
config.extensions.push({
  name: 'room',
  lazyResponseTypes: {
    stem: SERIALIZER_PREFIX_ROOM_STEM,
    load: async () => (await import('./client/revivers.js')).roomTypes,
  },
})
