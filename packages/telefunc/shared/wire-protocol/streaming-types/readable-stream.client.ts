export { readableStreamClientType }

import type { ClientStreamingType } from './interface.js'

const readableStreamClientType: ClientStreamingType = {
  prefix: '!TelefuncStream:',
  createValue: (
    _metadata: unknown,
    readNextChunk: () => Promise<Uint8Array | null>,
    cancel: () => void,
  ): ReadableStream<Uint8Array> => {
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const chunk = await readNextChunk()
          if (chunk === null) controller.close()
          else controller.enqueue(chunk)
        } catch (err) {
          cancel()
          controller.error(err)
        }
      },
      cancel() {
        cancel()
      },
    })
  },
}
