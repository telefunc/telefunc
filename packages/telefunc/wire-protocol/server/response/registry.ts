export { createStreamingReplacer }

import { asyncGeneratorReplacer } from './async-generator.js'
import { readableStreamReplacer } from './readable-stream.js'
import { fileReplacer } from './file.js'
import { blobReplacer } from './blob.js'
import { fileDownloadReplacer } from './fileDownload.js'
import { blobDownloadReplacer } from './blobDownload.js'
import { promiseReplacer } from './promise.js'
import { roomReplacer, roomParticipantReplacer, roomRemoteReplacer } from '../../room/response-server.js'
import { broadcastReplacer } from './broadcast.js'
import { channelReplacer } from './channel.js'
import { functionReplacer } from './function.js'
import type { ServerReplacerContext, ReplacerType, TypeContract } from '../../types.js'
import type { AbortError } from '../../../shared/Abort.js'
import { assertUsage } from '../../../utils/assert.js'
import { isObjectOrFunction } from '../../../utils/isObjectOrFunction.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

// Order: fileDownload/blobDownload before file/blob (brand-checked vs instanceof);
// file before blob (File extends Blob); broadcast before channel (Broadcast extends Channel).
const serverTypes = [
  asyncGeneratorReplacer,
  readableStreamReplacer,
  fileDownloadReplacer,
  blobDownloadReplacer,
  fileReplacer,
  blobReplacer,
  promiseReplacer,
  roomReplacer,
  roomParticipantReplacer,
  roomRemoteReplacer,
  broadcastReplacer,
  channelReplacer,
  functionReplacer,
]

/** Creates a JSON-serializer replacer that delegates to type-specific plugins.
 *  The caller provides `getContext(value)` which returns a per-value ServerReplacerContext —
 *  the registry has no shield logic, it just iterates types and forwards the context.
 *
 *  Duplicated references are deduplicated: `JSON.stringify` visits every occurrence of a shared
 *  reference, so without the cache each occurrence would run `replace()` again — minting another
 *  stub channel, another lifecycle registration, another wire subscription. Instead, duplicates
 *  re-emit the first occurrence's replacement verbatim (wire format unchanged), and the client's
 *  mirror cache (createStreamingReviver) revives equal wire strings to one object. */
function createStreamingReplacer(
  getContext: (value: unknown) => ServerReplacerContext,
  onReplaced: (replaced: { close: () => Promise<void> | void; abort: (abortError: AbortError) => void }) => void,
  extensionTypes: ReplacerType<TypeContract, ServerReplacerContext>[],
) {
  const allTypes = [...serverTypes, ...extensionTypes]
  const replaced = new WeakMap<object, string>()
  const replacements = new Set<string>()
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of allTypes) {
      if (type.detect(value)) {
        // Primitives have no reference identity — only objects and functions are deduplicated.
        const hasIdentity = isObjectOrFunction(value)
        if (hasIdentity) {
          const cached = replaced.get(value)
          if (cached !== undefined) return { replacement: cached, resolved: true }
        }
        const { metadata, close, abort } = type.replace(value as never, getContext(value))
        onReplaced({ close, abort })
        const replacement = type.prefix + serializer(metadata)
        if (hasIdentity) {
          // The client treats equal wire strings as the same identity, so two distinct values
          // must never serialize identically — see the metadata contract on ReplacerType.
          assertUsage(
            !replacements.has(replacement),
            `Two distinct values serialize to the same wire representation (${type.prefix}…). replace() must return metadata that identifies the value uniquely within the payload — mint a fresh id per replace() call, the way every built-in type does.`,
          )
          replacements.add(replacement)
          replaced.set(value, replacement)
        }
        return { replacement, resolved: true }
      }
    }
    return undefined
  }
  return replacer
}
