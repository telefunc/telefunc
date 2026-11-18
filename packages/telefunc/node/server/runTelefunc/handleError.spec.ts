import { afterEach, describe, expect, it, vi } from 'vitest'
import { handleError } from './handleError.js'

afterEach(() => {
  vi.restoreAllMocks()
})

describe('handleError()', () => {
  // A backend subscription that closes reports its own error, whose cause is the broker's refusal (Redis's NOPERM).
  it("logs what an error's causes say after its stack", () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    const refusal = new Error('NOPERM this user has no permissions to access one of the channels')
    handleError(new Error('Backend subscription closed: text:refused', { cause: refusal }))

    expect(logged).toHaveBeenCalledTimes(1)
    const [text] = logged.mock.calls[0] ?? []
    expect(text).toContain('Error: Backend subscription closed: text:refused')
    expect(text).toContain('[cause]: Error: NOPERM this user has no permissions')
  })

  it('logs an error that is its own cause once', () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
    const error = new Error('loop')
    error.cause = error
    handleError(error)

    expect(String(logged.mock.calls[0]?.[0]).match(/Error: loop/g)).toHaveLength(1)
  })
})
