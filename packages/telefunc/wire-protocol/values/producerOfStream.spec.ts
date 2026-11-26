import { expect, test } from 'vitest'
import { producerOfStream } from './producerOfStream.js'

/** A stream with one chunk and none after it yet, whose source cleans up on `cancel()` till `cleanedUp()` is called. */
function streamCleaningUp() {
  const cancelledFor: unknown[] = []
  let cleanedUp = () => {}
  const stream = new ReadableStream<Uint8Array<ArrayBuffer>>(
    {
      start(controller) {
        controller.enqueue(Uint8Array.of(1))
      },
      cancel(reason) {
        cancelledFor.push(reason)
        return new Promise<void>((resolve) => {
          cleanedUp = resolve
        })
      },
    },
    { highWaterMark: 0 },
  )
  return { stream, cancelledFor, cleanedUp: () => cleanedUp() }
}

test.each([
  ['before its first read', 0],
  ['while its read waits for a chunk', 2],
  ['while the chunk it yielded waits to be taken', 1],
])(
  "a stream whose producer is cancelled %s is unlocked at once, while its source's cancel() still cleans up",
  async (_, reads) => {
    const { stream, cancelledFor, cleanedUp } = streamCleaningUp()
    const producer = producerOfStream(stream)
    if (reads > 0) expect(await producer.chunks.next()).toEqual({ done: false, value: Uint8Array.of(1) })
    const read = reads > 1 ? producer.chunks.next() : null
    await new Promise((resolve) => setTimeout(resolve, 0))
    const reason = new Error('the call ended')
    producer.cancel(reason)
    expect({ locked: stream.locked, cancelledFor }).toEqual({ locked: false, cancelledFor: [reason] })
    expect(await read).toEqual(read === null ? null : { done: true, value: undefined })
    cleanedUp()
    await expect(stream.cancel('mine')).resolves.toBeUndefined()
    expect(cancelledFor).toEqual([reason])
  },
)
