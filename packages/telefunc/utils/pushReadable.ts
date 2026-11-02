export { createPushReadable }
export type { PushReadable }

import type { Readable } from 'node:stream'
import { getStreamNodeModule } from './loadStreamNodeModule.js'

/** A Node `Readable` fed by `push`: `readable` is the body handed on, the rest stays with the producer. */
type PushReadable = {
  readonly readable: Readable
  push(chunk: Uint8Array): boolean
  close(): void
  /** Bytes pushed that the consumer hasn't read yet. */
  readonly bufferedAmount: number
}

function createPushReadable(onCancel?: () => void, onDrain?: () => void): PushReadable {
  const { Readable } = getStreamNodeModule()
  let ended = false
  let cancelled = false
  const readable = new Readable({
    read() {
      onDrain?.()
    },
    destroy(err, callback) {
      if (!ended && !cancelled) {
        cancelled = true
        onCancel?.()
      }
      callback(err)
    },
  })
  return {
    readable,
    push: (chunk) => readable.push(chunk),
    close: () => {
      if (ended || cancelled) return
      ended = true
      readable.push(null)
    },
    get bufferedAmount() {
      return readable.readableLength
    },
  }
}
