// A body the reader is done with goes back to its owner. A Node body let go of with a read under way reads on as
// before, and an error it emits later reaches the reader's own error listener, which stays on it, and nothing reaches
// the process. A Web body read to its end, or whose read failed, is unlocked.

import { Readable } from 'node:stream'
import { expect, test } from 'vitest'
import { BodyReader } from './BodyReader.js'
import { queueMemory } from '../channel/server/channelMemory.js'

const turn = () => new Promise<void>((resolve) => setImmediate(resolve))

test("an error a Node source emits after the reader let go of it is the reader's, and nothing is thrown to the process", async () => {
  const source = new Readable({ read() {} })
  const reader = new BodyReader(source, () => true, queueMemory)
  const reading = reader.readFrame(1 << 20)
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
  const reader = new BodyReader(source, () => false, queueMemory)
  const reading = reader.readFrame(1 << 20)
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

/** A Web body that gives `frame`'s bytes as it is pulled, then ends, or fails with `failure` where given. */
function webBody(frame: number[], failure?: Error) {
  let given = false
  return new ReadableStream<Uint8Array<ArrayBuffer>>({
    pull(controller) {
      if (!given) {
        given = true
        controller.enqueue(Uint8Array.from(frame))
      } else if (failure) controller.error(failure)
      else controller.close()
    },
  })
}

test('a Web body read to its end is unlocked, so that its owner can still cancel it', async () => {
  const body = webBody([0, 0, 0, 2, 7, 8])
  const reader = new BodyReader(body, () => null, queueMemory)
  expect(await reader.readFrame(1 << 20)).toEqual(Uint8Array.of(7, 8))
  expect(await reader.readFrame(1 << 20)).toBe(null)
  expect(body.locked).toBe(false)
  await expect(body.cancel('mine')).resolves.toBeUndefined()
})

test("a Web body whose read fails with the app's own error is unlocked, and the read throws that error", async () => {
  const failure = new Error('the source failed')
  const body = webBody([0, 0, 0, 2, 7, 8], failure)
  const reader = new BodyReader(body, () => false, queueMemory)
  expect(await reader.readFrame(1 << 20)).toEqual(Uint8Array.of(7, 8))
  await expect(reader.readFrame(1 << 20)).rejects.toBe(failure)
  expect(body.locked).toBe(false)
})
