export { createRequestReplacer }

import type { ClientReplacerContext, ReplacerType, TypeContract } from '../../types.js'
import type { AbortError } from '../../../../shared/Abort.js'
import { fileReplacer } from './file.js'
import { blobReplacer } from './blob.js'
import { functionReplacer } from './function.js'
import { readableStreamReplacer } from './readableStream.js'

type ClientReplacerType = ReplacerType<TypeContract, ClientReplacerContext>

/** Carried in the request's own body. File before Blob — File extends Blob, so must be checked first. */
const bodyTypes: ClientReplacerType[] = [fileReplacer, blobReplacer]
/** Carried over a channel of their own. ReadableStream before Function — ReadableStream is an object with methods, not
 *  a function. */
const channelTypes: ClientReplacerType[] = [readableStreamReplacer, functionReplacer]

/** Before the page loaded channels, a built-in value carried over a channel is held: `hold(value)` names it in the
 *  request until channels are loaded and it is replaced. */
function createRequestReplacer(
  context: ClientReplacerContext,
  onReplaced: (replaced: {
    value: unknown
    close: () => Promise<void> | void
    abort: (abortError: AbortError) => void
  }) => void,
  extensionTypes: ClientReplacerType[],
  hold: ((value: unknown) => string) | null,
) {
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of bodyTypes) if (type.detect(value)) return replace(type, value, serializer)
    for (const type of channelTypes) {
      if (!type.detect(value)) continue
      if (hold) return { replacement: hold(value), resolved: true }
      return replace(type, value, serializer)
    }
    for (const type of extensionTypes) if (type.detect(value)) return replace(type, value, serializer)
    return undefined
  }
  function replace(type: ClientReplacerType, value: unknown, serializer: (v: unknown) => string) {
    const { metadata, close, abort } = type.replace(value, context)
    onReplaced({ value, close, abort })
    return { replacement: type.prefix + serializer(metadata), resolved: true }
  }
  return replacer
}
