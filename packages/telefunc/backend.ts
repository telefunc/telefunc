// Temporary fused exports remain until the pair cut-over; canonical plane contracts already live under
// wire-protocol/backend/{broadcast,room}.
export * from './wire-protocol/backend/spi.js'
export { HEAD_TRANSITIONS } from './wire-protocol/backend/room/head-transitions.js'
export { laneKey } from './wire-protocol/backend/room/lane-key.js'
export { ORDERING_FRAME_LAYOUT } from './wire-protocol/ordering-frame.js'
export {
  disposeBackend,
  getBackend,
  installBackend,
  type BackendFactory,
} from './wire-protocol/backend/install.js'
