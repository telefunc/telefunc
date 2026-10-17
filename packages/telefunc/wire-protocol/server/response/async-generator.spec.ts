import { expect, test } from 'vitest'
import { asyncGeneratorReplacer } from './async-generator.js'

test('cancelling a returned async generator whose finally throws leaves no unhandled rejection', async () => {
  const generator = (async function* () {
    try {
      yield 1
      yield 2
    } finally {
      throw new Error('cleanup failed')
    }
  })()
  const producer = asyncGeneratorReplacer.createProducer(generator)
  const unhandled: unknown[] = []
  const onUnhandled = (reason: unknown) => void unhandled.push(reason)
  process.on('unhandledRejection', onUnhandled)
  try {
    await producer.chunks.next()
    producer.cancel()
    await new Promise((resolve) => setTimeout(resolve, 10))
    expect(unhandled).toEqual([])
  } finally {
    process.off('unhandledRejection', onUnhandled)
  }
})
