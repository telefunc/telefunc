export { createStreamingReviver }

import type { Reviver } from '@brillout/json-serializer/parse'
import { clientDialect } from '../dialect.js'
import type { ClientReviverContext, InternalClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import type { AbortError } from '../../../shared/Abort.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'

/** Creates a JSON-serializer reviver that delegates to type-specific plugins.
 *
 *  Duplicated references are deduplicated, mirroring createStreamingReplacer: the server emits
 *  one replacement string per value identity, so equal wire strings denote the same server-side
 *  value — the first occurrence revives (side effects run once), duplicates resolve to the
 *  already-revived object, and server-side `===` holds on the client too. */
function createStreamingReviver(
  context: InternalClientReviverContext,
  onRevived: (revived: {
    value: unknown
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }) => void,
  extensionTypes: ReviverType<TypeContract, ClientReviverContext>[],
) {
  const allTypes = [...clientDialect, ...extensionTypes]
  const revivedByWireString = new Map<string, unknown>()
  const reviver: Reviver = (_path, value, parser) => {
    if (revivedByWireString.has(value)) return { replacement: revivedByWireString.get(value) }
    for (const type of allTypes) {
      if (value.startsWith(type.prefix)) {
        const metadata = parser(value.slice(type.prefix.length))
        assert(isObject(metadata))
        const revived = type.revive(metadata as never, context)
        onRevived(revived)
        // After onRevived — it wraps `revived.value` (GC proxy), and duplicates must
        // resolve to the exact object the first occurrence handed out.
        revivedByWireString.set(value, revived.value)
        return { replacement: revived.value }
      }
    }
    return undefined
  }
  return reviver
}
