export { setSessionToken, getSessionUrl }

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { randomUuid } from '../../utils/randomUuid.js'

/**
 * Client-side session registry.
 *
 * Keeps the client's latest session token in memory for each `telefuncUrl`,
 * so follow-up requests stay routed to the same server-side session shard.
 */

const globalObject = getGlobalObject<{ registry: Map<string, string> }>('session-registry.ts', {
  registry: new Map<string, string>(),
})

function setSessionToken(telefuncUrl: string, token: string): void {
  globalObject.registry.set(telefuncUrl, token)
}

/** `telefuncUrl` with the page's session token, named before its first request so that its concurrent calls and their
 *  channels reach one session. A query parameter, unlike a header, needs no cross-origin allowance. */
function getSessionUrl(telefuncUrl: string): string {
  const { registry } = globalObject
  const token = registry.get(telefuncUrl) ?? randomUuid()
  registry.set(telefuncUrl, token)
  return telefuncUrl.includes('?') ? `${telefuncUrl}&session=${token}` : `${telefuncUrl}?session=${token}`
}
