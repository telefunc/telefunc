// A Node body the reader let go of, with a read under way: what the stream emits later reaches the reader's own
// listeners, which stay on it, and nothing reaches the process.

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
