export { currentCloudflareSession, withCloudflareSession }
export type { CloudflareSession }

import { getRawContext, isAsyncMode, restoreContext } from '../../../node/server/context/context.js'
import { assertUsage } from '../../../utils/assert.js'
import { CHANNEL_MUX, type ChannelMux } from '../../channel/server/ChannelMux.js'
import type { CloudflareBroadcastMember } from './broadcast.js'

/** What a session DO lends the code it runs: its Broadcast membership and its channels, both of which end with this
 *  incarnation of the DO. */
type CloudflareSession = {
  broadcast: CloudflareBroadcastMember
  mux: ChannelMux
}

const SESSION = Symbol.for('telefunc.cloudflare.session')

/** Without AsyncLocalStorage, no scope survives an await, so the session's code runs on the isolate's channel mux. */
function withCloudflareSession<T>(session: CloudflareSession, fn: () => T): T {
  if (!isAsyncMode()) return fn()
  return restoreContext({ [SESSION]: session, [CHANNEL_MUX]: session.mux }, fn)
}

function currentCloudflareSession(): CloudflareSession | undefined {
  assertUsage(
    isAsyncMode(),
    // spellcheck-ignore  nodejs_als is Cloudflare's AsyncLocalStorage compatibility flag
    'Broadcast and Room on Cloudflare find their session through AsyncLocalStorage: add "nodejs_als" to the ' +
      'compatibility_flags of your wrangler.jsonc',
  )
  return getRawContext()?.[SESSION] as CloudflareSession | undefined
}
