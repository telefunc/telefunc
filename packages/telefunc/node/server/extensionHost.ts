export { telefuncExtensionHost }

import { getRawContext } from './context/context.js'
import { onPostSerialize } from './context/postSerialize.js'
import { LiveCell } from './live/live.js'
import type { TelefuncExtensionHost } from './extensions.js'

// The concrete host handed to each extension at `setup`. It wraps the same in-house primitives telefunc
// uses itself — `LiveCell` (the producer cell) and the post-serialize disposer seam — so an integration
// (e.g. @telefunc/drizzle) reaches them through this ONE public object instead of importing telefunc
// internals. Static: it holds no per-request state; `onRequestCleanup` reads the live request context each
// call.

const telefuncExtensionHost: TelefuncExtensionHost = {
  createLive(initialData) {
    return new LiveCell(initialData)
  },
  onRequestCleanup(cleanup) {
    // Bind to the SPECIFIC context object while it is live (before the body's first await); the
    // post-serialize drain holds the same object, so the cleanup fires even after the ambient context has
    // nulled in sync mode.
    const context = getRawContext()
    if (!context) {
      throw new Error(
        'TelefuncExtensionHost.onRequestCleanup() must be called inside a telefunction, before the body’s first await.',
      )
    }
    onPostSerialize(context, cleanup)
  },
}
