export { serve, telefunc } from './telefunc.js'
import { config } from './serverConfig.js'
export { config }
export { config as telefuncConfig }
export { getContext, provideTelefuncContext } from './getContext.js'
export { Abort } from './Abort.js'
export { shield } from './shield.js'
export { onBug } from './runTelefunc/onBug.js'

// TO-DO/next-major-release: remove
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
