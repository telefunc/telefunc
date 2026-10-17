import { config } from './clientConfig.js'
export { config }
// TO-DO/next-major: remove this redundant export
export { config as telefuncConfig }
export { onAbort, onTelefunctionRemoteCallError } from './remoteTelefunctionCall/onAbort.js'
export { abort } from './abort.js'
export { close } from './close.js'
export { Abort } from '../shared/Abort.js'
export { ShieldValidationError, isShieldValidationError } from '../shared/ShieldValidationError.js'
export { withContext } from './withContext.js'
export { ConnectionError } from './ConnectionError.js'
export { NetworkError } from '../shared/NetworkError.js'
export { ChannelClosedError, ChannelOverflowError } from '../wire-protocol/channel-errors.js'
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
} from '../wire-protocol/room/types.js'

export type { TelefuncClientExtension } from './extensions.js'
export type {
  TypeContract,
  ReplacerType,
  ReviverType,
  ClientReplacerContext,
  ClientReviverContext,
} from '../wire-protocol/types.js'

export { remoteTelefunctionCall as __remoteTelefunctionCall } from './remoteTelefunctionCall.js'
