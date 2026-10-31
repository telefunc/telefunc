export { currentCloudflareSession, requireCloudflareSession, withCloudflareSession }
export type { CloudflareSession }

import { getRawContext, restoreContext } from '../../../node/server/context/context.js'
import { assertUsage } from '../../../utils/assert.js'
import { CHANNEL_MUX, type ChannelMux } from '../../channel/server/mux.js'
import type { CloudflareBroadcastMember } from './broadcast.js'

/** What a session DO lends the code it runs: its Broadcast membership and its channels, both of which end with this
 *  incarnation of the DO. */
type CloudflareSession = {
  broadcast: CloudflareBroadcastMember
  mux: ChannelMux
}

const SESSION = Symbol.for('telefunc.cloudflare.session')

function withCloudflareSession<T>(session: CloudflareSession, fn: () => T): T {
  return restoreContext({ [SESSION]: session, [CHANNEL_MUX]: session.mux }, fn)
}

function currentCloudflareSession(): CloudflareSession | undefined {
  return getRawContext()?.[SESSION] as CloudflareSession | undefined
}

function requireCloudflareSession(): CloudflareSession {
  const session = currentCloudflareSession()
  assertUsage(
    session !== undefined,
    'A Cloudflare subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.',
  )
  return session
}
