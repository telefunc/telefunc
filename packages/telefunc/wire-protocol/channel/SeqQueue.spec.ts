import { describe, expect, test } from 'vitest'

import { setFlagsFromString } from 'node:v8'
import { runInNewContext } from 'node:vm'
import { SeqQueue } from './SeqQueue.js'

describe('what the queue dropped', () => {
  setFlagsFromString('--expose-gc')
  const gc: () => void = runInNewContext('gc')

  /** A queue of 1024 bodies, seqs 1 to 1024, and weak references to them. */
  function filled() {
    const queue = new SeqQueue<Uint8Array>()
    const bodies: WeakRef<Uint8Array>[] = []
    for (let seq = 1; seq <= 1024; seq++) {
      const body = new Uint8Array(1024)
      bodies.push(new WeakRef(body))
      queue.push(seq, body)
    }
    return { queue, bodies }
  }

  test.each([1, 100, 1023])(
    'it holds none of the %i oldest of 1024 it dropped, and keeps the rest',
    async (dropped) => {
      const { queue, bodies } = filled()
      queue.dropOldest(dropped)
      for (let i = 0; i < 2; i++) {
        gc()
        await new Promise((resolve) => setTimeout(resolve, 10))
      }
      expect(bodies.slice(0, dropped).filter((body) => body.deref() !== undefined)).toHaveLength(0)
      expect(bodies.slice(dropped).every((body, i) => body.deref() === queue.at(i))).toBe(true)
    },
  )
})
