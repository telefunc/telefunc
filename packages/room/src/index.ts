import './server.js'
export { config } from './server/config.js'
export type { RoomConfig } from './server/config.js'
// `Room` is both the statics object and the instance type: one export carries both meanings.
export { Room } from './server/lazyRoom.js'
export type {
  RoomInfo,
  RoomOptions,
  RoomMeta,
  RoomGetOptions,
  JoinOptions,
  PublishOptions,
  ParticipantMeta,
  LocalParticipant,
  RemoteParticipant,
  Sender,
  SendGuard,
  PublishGuard,
  JoinGuard,
  AfterPublishHook,
  AfterSendHook,
  AfterJoinHook,
  RoomSendReceipt,
  RoomAckReceipt,
  LeaveCause,
  ParticipantRef,
  BinaryFrameInfo,
  BinaryPublishOptions,
  RoomSnapshotView,
  ParticipantSnapshotView,
} from './types.js'
