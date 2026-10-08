import { expect, test } from 'vitest'
import { readableStreamReplacer } from './readable-stream.js'

test('cancelling an upload whose source failed after a chunk it yielded leaves no unhandled rejection', async () => {
  let source!: ReadableStreamDefaultController<Uint8Array<ArrayBuffer>>
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>({
    start(controller) {
      source = controller
      controller.enqueue(new Uint8Array([7]))
    },
  })
  const producer = readableStreamReplacer.createProducer(stream)
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
