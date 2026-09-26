export { currentCloudflareSession, requireCloudflareSession, withCloudflareSession }
export type { CloudflareSession }

import { getRawContext, restoreContext } from '../../../../node/server/context/context.js'
import { CHANNEL_MUX, type ChannelMux } from '../../mux.js'
import type { CloudflareBroadcastMember } from './broadcast.js'
import type { CloudflareRoomSessionManager } from './room/subscription.js'

/** What a session DO lends the code it runs: its Room manager, its Broadcast membership and its channels, all of
 *  which end with this incarnation of the DO. */
type CloudflareSession = {
  room: CloudflareRoomSessionManager
  broadcast: CloudflareBroadcastMember
  mux: ChannelMux
}

const SESSION = Symbol('telefunc.cloudflare.session')

const CLOUDFLARE_SESSION_ERROR =
  'A Cloudflare subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.'

function withCloudflareSession<T>(session: CloudflareSession, fn: () => T): T {
  return restoreContext({ [SESSION]: session, [CHANNEL_MUX]: session.mux }, fn)
}

function currentCloudflareSession(): CloudflareSession | undefined {
  return getRawContext()?.[SESSION] as CloudflareSession | undefined
}

function requireCloudflareSession(): CloudflareSession {
  const session = currentCloudflareSession()
  if (session === undefined) throw new Error(CLOUDFLARE_SESSION_ERROR)
  return session
}
