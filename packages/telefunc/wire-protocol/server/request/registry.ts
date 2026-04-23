export { createRequestReviver, resolveDeferredRevivals, serverRequestTypes }
export type { DeferredRevival }

import type { Reviver } from '@brillout/json-serializer/parse'
import { fileReviver } from './file.js'
import { blobReviver } from './blob.js'
import { functionReviver } from './function.js'
import { readableStreamReviver } from './readable-stream.js'
import type { ServerReviverContext, ReviverType, TypeContract } from '../../types.js'
import type { AbortError } from '../../../shared/Abort.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { assert } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { set } from '../../../utils/set.js'
assertIsNotBrowser()

/** File before Blob — File extends Blob, so must be checked first. */
const serverRequestTypes = [fileReviver, blobReviver, readableStreamReviver, functionReviver]

/** Captures a pending revival: prefix matched and metadata parsed, but `createValue` is deferred
 *  until the telefunction's argument shields are known. `path` is the raw key sequence from
 *  brillout (generic `string[]`), e.g. `['args', '0', 'callback']`. */
type DeferredRevival = {
  type: ReviverType<TypeContract, ServerReviverContext>
  metadata: object
  path: string[]
}

/** Brillout-shape reviver that defers `createValue`. The same instance is returned to the parser
 *  as the placeholder AND collected into `deferreds` for later resolution. */
function createRequestReviver(
  extensionTypes: ReviverType<TypeContract, ServerReviverContext>[],
): { reviver: Reviver; deferreds: DeferredRevival[] } {
  const deferreds: DeferredRevival[] = []
  const allTypes = [...serverRequestTypes, ...extensionTypes]
  const reviver: Reviver = (path, value, parser) => {
    for (const type of allTypes) {
      if (!value.startsWith(type.prefix)) continue
      const metadata = parser(value.slice(type.prefix.length))
      assert(isObject(metadata))
      const entry: DeferredRevival = { type, metadata, path: path ?? [] }
      deferreds.push(entry)
      return { replacement: entry, resolved: true }
    }
    return undefined
  }
  return { reviver, deferreds }
}

/** Runs each deferred `createValue` with a per-path ServerReviverContext, then writes the result
 *  back to its slot in `args`. `getContext` receives the raw key segments; it's the caller's job
 *  to translate them into whatever lookup key its shield metadata uses. */
function resolveDeferredRevivals(
  args: unknown[],
  deferreds: DeferredRevival[],
  getContext: (segments: string[]) => ServerReviverContext,
  onRevived: (revived: {
    value: unknown
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }) => void,
): void {
  for (const entry of deferreds) {
    assert(entry.path[0] === 'args', `deferred revival outside args tree: ${entry.path.join('.')}`)
    const subPath = entry.path.slice(1)
    const revived = entry.type.createValue(entry.metadata as never, getContext(subPath))
    onRevived(revived)
    set(args, subPath, revived.value)
  }
}
