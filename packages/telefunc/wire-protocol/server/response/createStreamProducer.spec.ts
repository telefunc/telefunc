import { expect, test } from 'vitest'
import { createStreamProducer } from './createStreamProducer.js'

test("cancelling a returned stream that failed leaves no unhandled rejection, as the docs' onDownload of a missing file does", async () => {
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    pull(controller) {
      controller.error(new Error('ENOENT'))
    },
  })
  const producer = createStreamProducer(stream)
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => void unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  try {
    await expect(producer.chunks.next()).rejects.toThrow('ENOENT')
    producer.cancel(new Error('cleanup'))
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(unhandled).toEqual([])
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
})
