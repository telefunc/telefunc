export { serve, telefunc } from './telefunc.js'
import { config } from './serverConfig.js'
export { config }
export { config as telefuncConfig }
export { getContext, provideTelefuncContext } from './context/getContext.js'
export { getRawContext } from './context/context.js'
export { PROVIDED_CONTEXT } from './context/getContext.js'
export { REQUEST_CONTEXT } from './context/requestContext.js'
export type { Context } from './context/context.js'
export { Abort } from './Abort.js'
export { shield } from './shield.js'
export { onBug } from './runTelefunc/onBug.js'
export { channel } from '../../wire-protocol/server/channel.js'
export { pubsub } from '../../wire-protocol/server/server-pubsub.js'
export { ChannelClosedError, ChannelNetworkError, ChannelOverflowError } from '../../wire-protocol/channel-errors.js'
export type { ChannelBase, Channel, ClientChannel, PubSub } from '../../wire-protocol/channel.js'
export type { PubSubAdapter } from '../../wire-protocol/server/pubsub.js'
export type { TelefuncServerExtension } from './extensions.js'
export type {
  TypeContract,
  ReplacerType,
  ReviverType,
  StreamingReplacerType,
  ServerReplacerContext,
  ServerReviverContext,
} from '../../wire-protocol/types.js'

// In order to allow users to override `Telefunc.Context`, we need to export `Telefunc` (even if the user never imports `Telefunc`)
export type { Telefunc } from './context/TelefuncNamespace.js'

export { decorateTelefunction as __decorateTelefunction } from './runTelefunc/decorateTelefunction.js'

import { assertUsage } from '../../utils/assert.js'

assertServerSide()

function assertServerSide() {
  const isBrowser = typeof window !== 'undefined' && 'innerHTML' in (window?.document?.body || {})
  assertUsage(
    !isBrowser,
    [
      'You are loading the `telefunc` module in the browser, but',
      'the `telefunc` module can only be imported in Node.js.',
    ].join(' '),
  )
}
