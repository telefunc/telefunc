export { setSessionToken, getSessionToken, getOrCreateSessionToken, appendSessionParam }

import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { randomUuid } from '../../utils/randomUuid.js'

/**
 * Client-side session registry.
 *
 * Keeps the client's latest session token in memory for each `telefuncUrl`,
 * so follow-up requests stay routed to the same server-side session shard.
 *
 * - `getSessionToken` — the page's token, if it named one yet.
 * - `getOrCreateSessionToken` — the token a call or a `ClientChannel` presents, named by the first of them.
 */

const globalObject = getGlobalObject<{ registry: Map<string, string> }>('session-registry.ts', {
  registry: new Map<string, string>(),
})

function setSessionToken(telefuncUrl: string, token: string): void {
  globalObject.registry.set(telefuncUrl, token)
}

function getSessionToken(telefuncUrl: string): string | undefined {
  return globalObject.registry.get(telefuncUrl)
}

/** A page names its token before its first request, so its concurrent calls and their channels reach one session. */
function getOrCreateSessionToken(telefuncUrl: string): string {
  let token = globalObject.registry.get(telefuncUrl)
  if (token === undefined) globalObject.registry.set(telefuncUrl, (token = randomUuid()))
  return token
}

function appendSessionParam(url: string, token: string): string {
  return url.includes('?') ? `${url}&session=${token}` : `${url}?session=${token}`
}
