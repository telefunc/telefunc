import { describe, expect, test } from 'vitest'
import { Readable } from 'node:stream'
import { readResponseText } from './readResponseText.js'

const bytes = (...values: number[]) => Uint8Array.from(values)
// "é€" is c3 a9 e2 82 ac; each chunk below ends inside a character.
const SPLIT = [bytes(0x61, 0xc3), bytes(0xa9, 0xe2), bytes(0x82, 0xac, 0x62)]

function webStream(chunks: Uint8Array[], error?: Error) {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk)
      if (error) controller.error(error)
      else controller.close()
    },
  })
}

describe('a Web stream', () => {
  test('reads to the end, decoding characters split across chunks, and is unlocked after', async () => {
    const stream = webStream(SPLIT)
    expect(await readResponseText(stream)).toBe('aé€b')
    expect(stream.locked).toBe(false)
    expect(await readResponseText(stream)).toBe('')
  })

  test('reads what another reader left', async () => {
    const stream = webStream([bytes(0x61), bytes(0x62), bytes(0x63)])
    const reader = stream.getReader()
    await reader.read()
    reader.releaseLock()
    expect(await readResponseText(stream)).toBe('bc')
  })

  test("rejects with the source's error, and is unlocked after", async () => {
    const error = new Error('the source fails, as the test means it to')
    const stream = webStream([bytes(0x61)], error)
    await expect(readResponseText(stream)).rejects.toBe(error)
    expect(stream.locked).toBe(false)
  })
})

describe('a Node stream', () => {
  test('reads to the end, decoding characters split across chunks', async () => {
    const stream = Readable.from(SPLIT)
    expect(await readResponseText(stream)).toBe('aé€b')
    expect(await readResponseText(stream)).toBe('')
  })

  test('drops a leading byte order mark', async () => {
    expect(await readResponseText(Readable.from([bytes(0xef, 0xbb), bytes(0xbf, 0x61)]))).toBe('a')
  })

  test('reads an empty stream', async () => {
    expect(await readResponseText(Readable.from([]))).toBe('')
  })

  test("rejects with the source's error", async () => {
    const error = new Error('the source fails, as the test means it to')
    const stream = new Readable({
      read() {
        this.push(bytes(0x61))
        this.destroy(error)
      },
    })
    await expect(readResponseText(stream)).rejects.toBe(error)
  })
})
