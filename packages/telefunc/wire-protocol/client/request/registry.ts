export { createRequestReplacer }

import type { ClientReplacerContext, ReplacerType } from '../../types.js'
import { fileReplacer } from './file.js'
import { blobReplacer } from './blob.js'
import { functionReplacer } from './function.js'
import { readableStreamReplacer } from './readable-stream.js'

/** File before Blob — File extends Blob, so must be checked first.
 *  ReadableStream before Function — ReadableStream is an object with methods, not a function. */
const clientRequestTypes: ReplacerType<any, ClientReplacerContext>[] = [
  fileReplacer,
  blobReplacer,
  readableStreamReplacer,
  functionReplacer,
]

function createRequestReplacer(context: ClientReplacerContext) {
  const replacer = (_key: string, value: unknown, serializer: (v: unknown) => string) => {
    for (const type of clientRequestTypes) {
      if (type.detect(value)) {
        return {
          replacement: type.prefix + serializer(type.getMetadata(value, context)),
          resolved: true,
        }
      }
    }
    return undefined
  }
  return replacer
}
