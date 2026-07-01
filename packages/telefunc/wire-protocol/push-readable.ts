export { createPushReadable }
export type { PushReadable }

import type { Readable } from 'node:stream'
import { getStreamNodeModule } from '../utils/loadStreamNodeModule.js'

type PushReadable = Readable & { close(): void }

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
  }) as PushReadable
  readable.close = () => {
    if (ended || cancelled) return
    ended = true
    readable.push(null)
  }
  return readable
}
