export { currentCloudflareSession, withCloudflareSession }
export type { CloudflareSession }

import { getRawContext, restoreContext } from '../../../node/server/context/context.js'
import { CHANNEL_MUX, type ChannelMux } from '../../channel/server/ChannelMux.js'
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
