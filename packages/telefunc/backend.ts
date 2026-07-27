// Telefunc's single backend boundary. The SPI itself lives in one source file so published declarations
// cannot drift from the implementations and conformance harnesses that use it.
export * from './wire-protocol/backend/spi.js'
export { HEAD_TRANSITIONS } from './wire-protocol/backend/head-transitions.js'
export { ORDERING_FRAME_LAYOUT } from './wire-protocol/ordering-frame.js'
export {
  disposeBackend,
  getBackend,
  installBackend,
  type BackendFactory,
} from './wire-protocol/backend/install.js'
