import { expect, test } from 'vitest'
import { SSEStreamReader } from './SSEStreamReader.js'
import { uint8ArrayToBase64url } from '../../base64url.js'

test('reads the bytes of SSE events, however the reads split their lines', async () => {
  const frames = [new Uint8Array([1, 2, 3, 4, 5]), new Uint8Array([6]), new Uint8Array([7, 8, 9])]
  const body = new TextEncoder().encode(
    `: ping\n\n${frames.map((frame) => `data: ${uint8ArrayToBase64url(frame)}\n\n`).join('')}`,
  )
  for (const size of [1, 2, 3, 5, 8, body.length]) {
    let offset = 0
    const reader = {
      read: async () =>
        offset < body.length ? { done: false, value: body.subarray(offset, (offset += size)) } : { done: true },
      cancel: async () => {},
    } as unknown as ReadableStreamDefaultReader<Uint8Array<ArrayBuffer>>
    const streamReader = new SSEStreamReader(reader, {
      telefunctionName: 'onLoad',
      telefuncFilePath: '/page.telefunc.ts',
      abortController: new AbortController(),
    })
    const read = [await streamReader.readExact(2), await streamReader.readExact(6), await streamReader.readExact(1)]
    expect(read.map((bytes) => [...bytes])).toEqual([[1, 2], [3, 4, 5, 6, 7, 8], [9]])
  }
})
