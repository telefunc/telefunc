import { expect, test } from 'vitest'
import { StreamReader } from './StreamReader.js'

test('reads exact byte counts, however the reads split them', async () => {
  const body = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9])
  for (const size of [1, 2, 3, 5, body.length]) {
    let offset = 0
    const reader = {
      read: async () =>
        offset < body.length ? { done: false, value: body.subarray(offset, (offset += size)) } : { done: true },
      cancel: async () => {},
    } as unknown as ReadableStreamDefaultReader<Uint8Array>
    const streamReader = new StreamReader(reader, {
      telefunctionName: 'onLoad',
      telefuncFilePath: '/page.telefunc.ts',
      abortController: new AbortController(),
    })
    const read = [await streamReader.readExact(2), await streamReader.readExact(6), await streamReader.readExact(1)]
    expect(read.map((bytes) => [...bytes])).toEqual([[1, 2], [3, 4, 5, 6, 7, 8], [9]])
  }
})
