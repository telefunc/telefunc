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

test('cancelling a returned stream that failed after a chunk it yielded leaves no unhandled rejection', async () => {
  let source!: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) {
      source = controller
      controller.enqueue(new Uint8Array([7]))
    },
  })
  const producer = createStreamProducer(stream)
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => void unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  try {
    await producer.chunks.next()
    source.error(new Error('EIO'))
    producer.cancel(new Error('cleanup'))
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(unhandled).toEqual([])
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
})
