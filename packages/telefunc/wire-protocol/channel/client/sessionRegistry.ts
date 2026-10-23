export { getSessionUrl }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { randomUuid } from '../../../utils/randomUuid.js'

const globalObject = getGlobalObject<{ registry: Map<string, string> }>('sessionRegistry.ts', {
  registry: new Map<string, string>(),
})

/** `telefuncUrl` with the page's session token, named before its first request so that its concurrent calls and their
 *  channels reach one session. A query parameter adds no header for a cross-origin server to allow. */
function getSessionUrl(telefuncUrl: string): string {
  const { registry } = globalObject
  const token = registry.get(telefuncUrl) ?? randomUuid()
  registry.set(telefuncUrl, token)
  return telefuncUrl.includes('?') ? `${telefuncUrl}&session=${token}` : `${telefuncUrl}?session=${token}`
}
