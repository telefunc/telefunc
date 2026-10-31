import { expect, test, vi } from 'vitest'
import { ProtocolViolationError, isProtocolViolationError } from './protocolViolation.js'

test("a protocol violation another telefunc copy raised is one, though it isn't this copy's class", async () => {
  vi.resetModules()
  const copy = await import('./protocolViolation.js')
  const violation = new copy.ProtocolViolationError('malformed frame')
  expect(violation).not.toBeInstanceOf(ProtocolViolationError)
  expect(isProtocolViolationError(violation)).toBe(true)
  expect(isProtocolViolationError(new Error('malformed frame'))).toBe(false)
})
