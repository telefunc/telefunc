export { setSessionToken, getSessionToken, getOrCreateSessionToken, appendSessionParam }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/**
 * Client-side session registry.
 *
 * Keeps the client's latest session token in memory for each `telefuncUrl`,
 * so follow-up requests stay routed to the same server-side session shard.
 *
 * - `getSessionToken` — the token a call presents, if the page has one yet.
 * - `getOrCreateSessionToken` — the token a `ClientChannel` connects with, named by the channel if the page has none.
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

/** A channel made before the first response brings a token names one, so the call that carries it presents the same
 *  token and both reach one session. */
function getOrCreateSessionToken(telefuncUrl: string): string {
  let token = globalObject.registry.get(telefuncUrl)
  if (token === undefined) globalObject.registry.set(telefuncUrl, (token = crypto.randomUUID()))
  return token
}

function appendSessionParam(url: string, token: string): string {
  return url.includes('?') ? `${url}&session=${token}` : `${url}?session=${token}`
}
