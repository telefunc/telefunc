export { createStreamingReplacer }

import { asyncGeneratorReplacer } from './async-generator.js'
import { readableStreamReplacer } from './readable-stream.js'
import { fileReplacer } from './file.js'
import { blobReplacer } from './blob.js'
import { fileDownloadReplacer } from './fileDownload.js'
import { blobDownloadReplacer } from './blobDownload.js'
import { promiseReplacer } from './promise.js'
import { broadcastReplacer } from './broadcast.js'
import { channelReplacer } from './channel.js'
import { functionReplacer } from './function.js'
import type { ServerReplacerContext, ReplacerType, TypeContract } from '../../types.js'
import type { AbortError } from '../../../shared/Abort.js'
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
  broadcastReplacer,
  channelReplacer,
  functionReplacer,
]

/** Creates a JSON-serializer replacer that delegates to type-specific plugins.
 *  The caller provides `getContext(value)` which returns a per-value ServerReplacerContext —
 *  the registry has no shield logic, it just iterates types and forwards the context. */
function createStreamingReplacer(
  getContext: (value: unknown) => ServerReplacerContext,
  onReplaced: (replaced: { close: () => Promise<void> | void; abort: (abortError: AbortError) => void }) => void,
  extensionTypes: ReplacerType<TypeContract, ServerReplacerContext>[],
) {
  const allTypes = [...serverTypes, ...extensionTypes]
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of allTypes) {
      if (type.detect(value)) {
        const { metadata, close, abort } = type.replace(value as never, getContext(value))
        onReplaced({ close, abort })
        return {
          replacement: type.prefix + serializer(metadata),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return replacer
}
