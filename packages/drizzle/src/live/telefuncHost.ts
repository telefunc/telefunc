export { getTelefuncHost }

import { config } from 'telefunc'
import type { TelefuncExtensionHost } from 'telefunc'

// @telefunc/drizzle integrates through telefunc's EXTENSION INTERFACE, not its internals. It registers a
// server extension whose `setup` receives the host toolkit — a `Live` producer (`createLive`) — and uses
// it in its read-capture engine. Registration is
// lazy + idempotent (done on first `reactiveDrizzle` use), so the public `reactiveDrizzle(db)` API needs
// no config step: the extension is an internal detail of this package.

let host: TelefuncExtensionHost | null = null
let registered = false

/** The telefunc host toolkit, registering the drizzle extension on first use. Throws if telefunc never
 *  called `setup` (an incompatible/old telefunc that lacks the extension-host seam). */
function getTelefuncHost(): TelefuncExtensionHost {
  if (!registered) {
    registered = true
    config.extensions.push({
      name: '@telefunc/drizzle',
      setup(providedHost) {
        host = providedHost
      },
    })
  }
  if (!host) {
    throw new Error(
      'reactiveDrizzle: telefunc did not provide an extension host — the installed `telefunc` version is too old for @telefunc/drizzle.',
    )
  }
  return host
}
