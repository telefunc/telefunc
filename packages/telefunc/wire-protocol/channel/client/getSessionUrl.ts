export { getSessionUrl, getWireUrl, setSessionHome }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { randomUuid } from '../../../utils/randomUuid.js'
import { WIRE_VERSION } from '../../constants.js'

type Session = { token: string; home: string | null }

const globalObject = getGlobalObject<{ sessions: Map<string, Session> }>('channel/client/getSessionUrl.ts', {
  sessions: new Map<string, Session>(),
})

/** `telefuncUrl` with the page's session token, named before its first request so that its concurrent calls and their
 *  channels reach one session, and with its home once the server named it. A query parameter adds no header for a
 *  cross-origin server to allow. */
function getSessionUrl(telefuncUrl: string): string {
  return withParams(telefuncUrl, sessionParams(telefuncUrl))
}

/** The session URL of a channel's wire, which names the wire's version. */
function getWireUrl(telefuncUrl: string): string {
  return withParams(telefuncUrl, `${sessionParams(telefuncUrl)}&wire=${WIRE_VERSION}`)
}

function sessionParams(telefuncUrl: string): string {
  const { token, home } = getSession(telefuncUrl)
  return home === null ? `session=${token}` : `session=${token}&home=${home}`
}

/** `url` with `params` in its query, ahead of a fragment, which a request never sends. */
function withParams(url: string, params: string): string {
  const hashAt = url.indexOf('#')
  const beforeHash = hashAt === -1 ? url : url.slice(0, hashAt)
  const hash = hashAt === -1 ? '' : url.slice(hashAt)
  return `${beforeHash}${beforeHash.includes('?') ? '&' : '?'}${params}${hash}`
}

/** Records the home the server named for the page's session (`SESSION_HOME_HEADER`), if it named one. */
function setSessionHome(telefuncUrl: string, home: string | null): void {
  if (home) getSession(telefuncUrl).home = encodeURIComponent(home)
}

function getSession(telefuncUrl: string): Session {
  const { sessions } = globalObject
  let session = sessions.get(telefuncUrl)
  if (!session) {
    session = { token: randomUuid(), home: null }
    sessions.set(telefuncUrl, session)
  }
  return session
}
