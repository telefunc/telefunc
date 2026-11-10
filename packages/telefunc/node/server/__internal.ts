export type { TELEFUNC_SHIELDS } from '../shared/transformer/generateShield/shieldKey.js'

// A transport a package adds: its server side.
export { offerChannelTransport } from './serverConfig.js'
export { getGlobalObject } from '../../utils/getGlobalObject.js'
export { getChannelMux, type ServerTransport } from '../../wire-protocol/channel/server/ChannelMux.js'
export type { IndexedPeer, LossyLane, PeerSender, RoomWaiter } from '../../wire-protocol/channel/server/IndexedPeer.js'
export type { PublishBody } from '../../wire-protocol/channel/server/PublishBody.js'
export type { WireQueue } from '../../wire-protocol/flowControl/server/waitingPublishes.js'
export { PathEstimate } from '../../wire-protocol/flowControl/PathEstimate.js'
export { MAX_CHANNELS_PER_CONNECTION, WIRE_MAX_RAW_FRAME_BYTES } from '../../wire-protocol/constants.js'
export { isClosingTag } from '../../wire-protocol/wire/tags.js'

// Its page side, whose types resolve through this file (see `client/__internal.ts`).
export {
  registerChannelTransport,
  type PackageTransport,
} from '../../wire-protocol/channel/client/packageTransports.js'
export type { ClientChannelTransport, OutboundFrame } from '../../wire-protocol/channel/client/ClientConnection.js'
export type { TransportOwner } from '../../wire-protocol/channel/client/WireLifecycle.js'
export type { ProbeOwner, ProbeWire, UpgradeTarget } from '../../wire-protocol/channel/client/Upgrade.js'
export type { Heartbeat } from '../../wire-protocol/channel/client/Heartbeat.js'
export { pairPublishes } from '../../wire-protocol/channel/client/pairPublishes.js'
export { Uplink } from '../../wire-protocol/flowControl/client/Uplink.js'
export { Downlink } from '../../wire-protocol/flowControl/client/Downlink.js'
export { decode, encode, type DecodedFrame } from '../../wire-protocol/wire/frame.js'
export { TAG, isWireCtrlTag } from '../../wire-protocol/wire/tags.js'
export { readU16 } from '../../wire-protocol/wire/header.js'
export { WS_PROBE_TIMEOUT_MS } from '../../wire-protocol/constants.js'
