// Workaround: Telefunc 0.3.1 adds its `upgrade` listener to a Node server unless the server is in its own registry, a
// process-wide `WeakSet` it alone knew. Without this version reading and writing that registry, an app that loads both
// copies and installs both on one server gets two `upgrade` listeners: crossws throws as both handle one socket, and the
// process ends (serve/node.offer.spec.ts). Remove once Telefunc no longer supports a process that also loads 0.3.1.
export { inLegacyRegistry, recordInLegacyRegistry, releaseLegacyRegistry }

import { getGlobalObject } from '../utils/getGlobalObject.js'

// 0.3.1's key and shape.
const { registeredServers } = getGlobalObject('serve/node.ts', { registeredServers: new WeakSet<object>() })

/** Whether a copy of Telefunc, 0.3.1 or this one, took the server's upgrades. */
function inLegacyRegistry(server: object): boolean {
  return registeredServers.has(server)
}

/** This version took the server's upgrades: 0.3.1 adds no listener to it. */
function recordInLegacyRegistry(server: object): void {
  registeredServers.add(server)
}

/** This version's listener is gone: 0.3.1's next install may take the server. */
function releaseLegacyRegistry(server: object): void {
  registeredServers.delete(server)
}
