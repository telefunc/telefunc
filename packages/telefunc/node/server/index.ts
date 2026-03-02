export { telefunc } from './telefunc.js'
import { config } from './serverConfig.js'
export { config }
export { config as telefuncConfig }
export { getContext, provideTelefuncContext } from './getContext.js'
export { Abort } from './Abort.js'
export { shield } from './shield.js'
export { onBug } from './runTelefunc/onBug.js'
export { createChannel } from './channel.js'
export type { Channel } from './channel.js'
export { telefuncWebSocket } from './telefuncWebSocket.js'

// In order to allow users to override `Telefunc.Context`, we need to export `Telefunc` (even if the user never imports `Telefunc`)
export type { Telefunc } from './getContext/TelefuncNamespace.js'

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
