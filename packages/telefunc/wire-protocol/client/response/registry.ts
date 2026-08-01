export { createStreamingReviver, clientTypes }

import type { Reviver } from '@brillout/json-serializer/parse'
import { asyncGeneratorReviver } from './async-generator.js'
import { readableStreamReviver } from './readable-stream.js'
import { fileReviver } from './file.js'
import { blobReviver } from './blob.js'
import { fileDownloadReviver } from './fileDownload.js'
import { blobDownloadReviver } from './blobDownload.js'
import { promiseReviver } from './promise.js'
import { broadcastReviver } from './broadcast.js'
import { channelReviver } from './channel.js'
import { functionReviver } from './function.js'
import type { ClientReviverContext, ReviverType, TypeContract } from '../../types.js'
import type { AbortError } from '../../../shared/Abort.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'

const clientTypes = [
  asyncGeneratorReviver,
  readableStreamReviver,
  fileReviver,
  blobReviver,
  fileDownloadReviver,
  blobDownloadReviver,
  promiseReviver,
  broadcastReviver,
  channelReviver,
  functionReviver,
]

/** Creates a JSON-serializer reviver that delegates to type-specific plugins.
 *
 *  Duplicated references are deduplicated, mirroring createStreamingReplacer: the server emits
 *  one replacement string per value identity, so equal wire strings denote the same server-side
 *  value — the first occurrence revives (side effects run once), duplicates resolve to the
 *  already-revived object, and server-side `===` holds on the client too. */
function createStreamingReviver(
  context: ClientReviverContext,
  onRevived: (revived: {
    value: unknown
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }) => void,
  extensionTypes: ReviverType<TypeContract, ClientReviverContext>[],
  builtInTypes: readonly ReviverType<TypeContract, ClientReviverContext>[] = clientTypes,
) {
  const allTypes = [...builtInTypes, ...extensionTypes]
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
