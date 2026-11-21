import type { config as ClientConfig } from '../client/clientConfig.js'

// no-op
/** The client's config, which code shared with the client writes to on the server too: here, writes do nothing. */
const config: typeof ClientConfig = { stream: {}, channel: {}, extensions: [] }
export { config, config as telefuncConfig }
export const onAbort = () => {}
export const abort = () => {}
export const close = () => {}
export { Abort } from './server/Abort.js'
export { ConnectionError } from '../client/ConnectionError.js'
export { ShieldValidationError, isShieldValidationError } from '../shared/ShieldValidationError.js'
export { NetworkError } from '../shared/NetworkError.js'
export {
  ChannelClosedError,
  ChannelOverflowError,
  ChannelCloseUnknownError,
} from '../wire-protocol/channel/channelErrors.js'
export const withContext = <F extends (...args: any[]) => any>(fn: F, _ctx?: any): F => fn
/** @deprecated */
export const onTelefunctionRemoteCallError = () => {}
