export { setSessionToken, getSessionToken, getLastSessionToken }

import { getGlobalObject } from '../../utils/getGlobalObject.js'

/**
 * Client-side session registry.
 *
 * Keeps the client's latest session token in memory for each `telefuncUrl`,
 * so follow-up requests stay routed to the same server-side session shard.
 *
 * - `getLastSessionToken` — returns the last known token; used by `ClientChannel`
 *   to open the WS connection to the correct Durable Object.
 * - `getSessionToken` — returns the token for appending as an advisory `?session=` param to POST URLs.
 */

const globalObject = getGlobalObject<{ registry: Map<string, string> }>('session-registry.ts', {
  registry: new Map<string, string>(),
})

function setSessionToken(telefuncUrl: string, token: string): void {
  globalObject.registry.set(telefuncUrl, token)
}

function getLastSessionToken(telefuncUrl: string): string | undefined {
  return globalObject.registry.get(telefuncUrl)
}

function getSessionToken(telefuncUrl: string): string | undefined {
  return globalObject.registry.get(telefuncUrl)
}
