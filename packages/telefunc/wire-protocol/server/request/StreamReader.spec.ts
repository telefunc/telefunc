import { expect, test } from 'vitest'
import { StreamReader } from './StreamReader.js'

test('reads length-prefixed chunks, however the request body splits them', async () => {
  const body = new Uint8Array([0, 0, 0, 5, 1, 2, 3, 4, 5, 0, 0, 0, 3, 6, 7, 8])
  for (const size of [1, 2, 3, 6, body.length]) {
    const reader = new StreamReader(
      new ReadableStream({
        start(controller) {
          for (let offset = 0; offset < body.length; offset += size)
            controller.enqueue(body.slice(offset, offset + size))
          controller.close()
        },
      }),
    )
    const read = [
      await reader.readLengthPrefixedBytesOrNull(16),
      await reader.readLengthPrefixedBytesOrNull(16),
      await reader.readLengthPrefixedBytesOrNull(16),
    ]
    expect(read.map((bytes) => bytes && [...bytes])).toEqual([[1, 2, 3, 4, 5], [6, 7, 8], null])
  }
})
