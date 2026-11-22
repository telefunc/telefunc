// A Node body the reader let go of, with a read under way: its owner reads it on as before, and an error it emits later
// reaches the reader's own error listener, which stays on it, and nothing reaches the process.

import { Readable } from 'node:stream'
import { expect, test } from 'vitest'
import { BodyReader } from './BodyReader.js'

const turn = () => new Promise<void>((resolve) => setImmediate(resolve))

test("an error a Node source emits after the reader let go of it is the reader's, and nothing is thrown to the process", async () => {
  const source = new Readable({ read() {} })
  const reader = new BodyReader(source, () => true)
  const reading = reader.readLengthPrefixedBytesOrNull(1 << 20)
  await turn()
  reader.letGo()
  const read = await reading
  source.destroy(new Error('a late error of the source'))
  await turn()
  expect({ read, errored: source.errored instanceof Error, errorListeners: source.listenerCount('error') }).toEqual({
    read: null,
    errored: true,
    errorListeners: 1,
  })
})

test("a Node source the reader let go of with a read under way reads on for its owner: a `'data'` listener and `resume()` drain it", async () => {
  const source = new Readable({ read() {} })
  const reader = new BodyReader(source, () => false)
  const reading = reader.readLengthPrefixedBytesOrNull(1 << 20)
  await turn()
  reader.letGo()
  const read = await reading
  const drained: string[] = []
  source.on('data', (chunk: Buffer) => void drained.push(chunk.toString()))
  source.resume()
  source.push(Buffer.from('after'))
  source.push(null)
  await turn()
  expect({ read, drained, buffered: source.readableLength }).toEqual({ read: null, drained: ['after'], buffered: 0 })
})
