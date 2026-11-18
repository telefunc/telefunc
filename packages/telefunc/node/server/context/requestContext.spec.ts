import { expect, test, vi } from 'vitest'
import { createRequestContext } from './requestContext.js'
import { onBug } from '../runTelefunc/onBug.js'

test('an onClose() callback that throws, or whose promise rejects, is logged with its stack and reaches onBug() listeners', async () => {
  const bugs: unknown[] = []
  onBug((err) => bugs.push(err))
  const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
  const thrown = new Error('onClose() throws')
  const rejected = new Error('onClose() rejects')
  const context = createRequestContext(new Request('http://localhost/_telefunc'))
  context.onClose(() => {
    throw thrown
  })
  context.onClose(() => Promise.reject(rejected))
  context.markComplete()
  await new Promise((resolve) => setTimeout(resolve, 0))
  expect(bugs).toEqual([thrown, rejected])
  expect(logged.mock.calls).toEqual([[thrown.stack], [rejected.stack]])
  logged.mockRestore()
})
