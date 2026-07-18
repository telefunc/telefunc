export { telefuncExtensionHost }

import { LiveCell } from './live/live.js'
import type { TelefuncExtensionHost } from './extensions.js'

// The concrete host handed to each extension at `setup`. It wraps the same in-house primitive telefunc
// uses itself — `LiveCell`, the producer cell — so an integration (e.g. @telefunc/drizzle) reaches it
// through this ONE public object instead of importing telefunc internals. Stateless, and deliberately not
// request-aware: everything an integration needs at request time it gets at SERIALIZE time, where the wire
// replacer already holds the context.

const telefuncExtensionHost: TelefuncExtensionHost = {
  createLive(initialData) {
    return new LiveCell(initialData)
  },
}
