export type {
  Room,
  RoomInfo,
  RoomMeta,
  JoinOptions,
  PublishOptions,
  ParticipantMeta,
  ReadonlyMeta,
  LocalParticipant,
  RemoteParticipant,
  TrackDemand,
  Sender,
  RoomSendReceipt,
  RoomAckReceipt,
  LeaveCause,
  BinaryFrameInfo,
  BinaryPublishOptions,
  RoomMessage,
  RoomSnapshotView,
} from './types.js'

import { config } from 'telefunc/client'
import { SERIALIZER_PREFIX_ROOM_STEM } from './shared/constants.js'

// Keeps Room's client code out of the entry bundle: only a response whose text holds a Room prefix loads it.
config.extensions.push({
  name: 'room',
  lazyResponseTypes: {
    stem: SERIALIZER_PREFIX_ROOM_STEM,
    load: async () => (await import('./client/revivers.js')).roomTypes,
  },
})
