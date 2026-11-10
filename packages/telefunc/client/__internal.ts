// The browser half of telefunc/__internal: what a `@telefunc/*` package's page side imports.

// A transport a package adds.
export { registerChannelTransport, type PackageTransport } from '../wire-protocol/channel/client/packageTransports.js'
export type { ClientChannelTransport, OutboundFrame } from '../wire-protocol/channel/client/ClientConnection.js'
export type { TransportOwner } from '../wire-protocol/channel/client/WireLifecycle.js'
export type { ProbeOwner, ProbeWire, UpgradeTarget } from '../wire-protocol/channel/client/Upgrade.js'
export type { Heartbeat } from '../wire-protocol/channel/client/Heartbeat.js'
export { pairPublishes } from '../wire-protocol/channel/client/pairPublishes.js'
export { Uplink } from '../wire-protocol/flowControl/client/Uplink.js'
export { Downlink } from '../wire-protocol/flowControl/client/Downlink.js'
export { decode, encode, type DecodedFrame } from '../wire-protocol/wire/frame.js'
export { TAG, isWireCtrlTag, isClosingTag } from '../wire-protocol/wire/tags.js'
export { readU16 } from '../wire-protocol/wire/header.js'
export { WIRE_MAX_RAW_FRAME_BYTES, WS_PROBE_TIMEOUT_MS } from '../wire-protocol/constants.js'
