import { expect, test } from 'vitest'
import { asyncGeneratorReviver } from './asyncGenerator.js'
import type { ClientReviverContext } from '../../types.js'

test('a generator returned mid-iteration ends once: its stream is cancelled and its end callback runs one time', async () => {
  let cancels = 0
  const chunk = new TextEncoder().encode('1')
  const context = {
    receiveStream: () => ({ readNextChunk: async () => chunk, cancel: () => void cancels++, abort() {} }),
  } as unknown as ClientReviverContext
  const revived = asyncGeneratorReviver.revive({ __index: 0 }, context)
  let ends = 0
  revived.onEnd?.(() => void ends++)
  await revived.value.next()
  await revived.value.return(undefined)
  expect({ cancels, ends }).toEqual({ cancels: 1, ends: 1 })
})
