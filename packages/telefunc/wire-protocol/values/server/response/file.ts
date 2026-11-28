export { fileReplacer }

import type { StreamingReplacerType, FileResponseContract, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_FILE } from '../../../constants.js'
import { producerOfStream } from '../../producerOfStream.js'

const fileReplacer: StreamingReplacerType<FileResponseContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_FILE,
  detect: (value): value is File => value instanceof File,
  replace: (value, context) => {
    const {
      metadata: streamMetadata,
      close,
      abort,
    } = context.sendStream(() => fileReplacer.createProducer(value), { eager: true })
    const metadata = {
      ...streamMetadata,
      name: value.name,
      type: value.type,
      lastModified: value.lastModified,
      ...(value.size >= 0 ? { size: value.size } : {}),
    }
    return { metadata, close, abort }
  },
  createProducer: (value) => producerOfStream(value.stream() as ReadableStream<Uint8Array<ArrayBuffer>>),
}
