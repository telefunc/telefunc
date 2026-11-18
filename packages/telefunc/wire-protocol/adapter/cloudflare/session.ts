export { currentCloudflareSession, withCloudflareSession, servesRequest }
export type { CloudflareSession }

import { getRawContext, isAsyncMode, restoreContext } from '../../../node/server/context/context.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { CHANNEL_MUX, type ChannelMux } from '../../channel/server/ChannelMux.js'
import { setRequestScope } from '../../channel/server/requestScope.js'
import type { CloudflareBroadcastMember } from './broadcast.js'

/** What a session DO lends the code it runs: its Broadcast membership and its channels, both of which end with this
 *  incarnation of the DO. */
type CloudflareSession = {
  broadcast: CloudflareBroadcastMember
  mux: ChannelMux
}

const SESSION = Symbol.for('telefunc.cloudflare.session')

const globalObject = getGlobalObject('wire-protocol/adapter/cloudflare/session.ts', {
  /** Without AsyncLocalStorage, the session whose page's subscription a call brings, up to its first `await`. */
  running: undefined as CloudflareSession | undefined,
  /** The session each request reached, so that a page's subscription its call brings runs in it (`inRequestScope`). */
  requests: new WeakMap<Request, CloudflareSession>(),
})

setRequestScope((request, fn) => {
  const session = globalObject.requests.get(request)
  if (session === undefined) return fn()
  return isAsyncMode() ? withCloudflareSession(session, fn) : runningIn(session, fn)
})

/** Without AsyncLocalStorage, no scope survives an await, so the session's code runs on the isolate's channel mux, and
 *  server code that subscribes finds no session. */
function withCloudflareSession<T>(session: CloudflareSession, fn: () => T): T {
  if (isAsyncMode()) return restoreContext({ [SESSION]: session, [CHANNEL_MUX]: session.mux }, fn)
  return fn()
}

/** Without AsyncLocalStorage, a page's subscription that a call brings subscribes in the session the call reached,
 *  synchronously: the page needs no flag. */
function runningIn<T>(session: CloudflareSession, fn: () => T): T {
  const outer = globalObject.running
  globalObject.running = session
  try {
    return fn()
  } finally {
    globalObject.running = outer
  }
}

/** `request` reached `session`. */
function servesRequest(session: CloudflareSession, request: Request): void {
  globalObject.requests.set(request, session)
}

function currentCloudflareSession(): CloudflareSession | undefined {
  if (!isAsyncMode()) return globalObject.running
  return getRawContext()?.[SESSION] as CloudflareSession | undefined
}
