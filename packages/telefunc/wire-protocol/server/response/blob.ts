export { blobReplacer }

import type { StreamingReplacerType, BlobResponseContract, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_BLOB } from '../../constants.js'
import { createStreamProducer } from './createStreamProducer.js'

const blobReplacer: StreamingReplacerType<BlobResponseContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_BLOB,
  detect: (value): value is Blob => value instanceof Blob,
  replace: (value, context) => {
    const { metadata: streamMetadata, close, abort } = context.sendStream(() => blobReplacer.createProducer(value))
    const metadata = {
      ...streamMetadata,
      type: value.type,
      ...(value.size >= 0 ? { size: value.size } : {}),
    }
    return { metadata, close, abort }
  },
  createProducer: (value) => createStreamProducer(value.stream() as ReadableStream<Uint8Array<ArrayBuffer>>),
}
