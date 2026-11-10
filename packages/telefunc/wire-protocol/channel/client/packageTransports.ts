export { registerChannelTransport, getPackageTransport, getPackageTransportNames }
export type { PackageTransport }

import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import type { TransportOwner } from './WireLifecycle.js'
import type { UpgradeTarget } from './Upgrade.js'

/** Makes a transport a `@telefunc/*` package adds, which a page reaches by an upgrade: `offer` is the server's RECONCILED
 *  entry for it, undefined where the server has none. */
type PackageTransport = (options: { telefuncUrl: string; owner: TransportOwner; offer: unknown }) => UpgradeTarget

// One per page, however many copies of Telefunc it loaded.
const globalObject = getGlobalObject('channel/client/packageTransports.ts', {
  transports: new Map<string, PackageTransport>(),
})

/** @internal A page's channels use transport `name` after the ones they use by default, unless the app lists its own. */
function registerChannelTransport(name: string, create: PackageTransport): void {
  globalObject.transports.set(name, create)
}

function getPackageTransport(name: string): PackageTransport | undefined {
  return globalObject.transports.get(name)
}

function getPackageTransportNames(): string[] {
  return [...globalObject.transports.keys()]
}
