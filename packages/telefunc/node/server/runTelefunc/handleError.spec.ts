import { afterEach, describe, expect, it, vi } from 'vitest'
import { callBugListeners, onBug } from './onBug.js'
import { MONITORING_FAILURES } from '../../../wire-protocol/test/monitoringFailures.js'
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

/** What the first `onBug()` listener does with each bug, where a test sets it; the second records each bug it gets. */
let firstBugListener: (() => unknown) | undefined
onBug(() => firstBugListener?.())
const bugs: unknown[] = []
onBug((err) => void bugs.push(err))

describe("a telefunction's bug reported to an `onBug()` listener that fails", () => {
  for (const [given, failure] of Object.entries(MONITORING_FAILURES)) {
    for (const fails of ['throws', 'rejects'] as const) {
      it(`one that ${fails} with ${given} is logged once, and the next listener gets the bug`, async () => {
        const monitoringFailed = failure()
        firstBugListener =
          fails === 'throws'
            ? () => {
                throw monitoringFailed
              }
            : () => Promise.reject(monitoringFailed)
        const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
          const before = bugs.length
          callBugListeners(new Error('the telefunction fails, as the test means it to'))
          await Promise.resolve()
          expect({
            nextListener: bugs.slice(before).map((bug) => bug instanceof Error && bug.message),
            logged: logged.mock.calls.filter(([error]) => String(error).includes(monitoringFailed.message)).length,
          }).toEqual({
            nextListener: ['the telefunction fails, as the test means it to'],
            logged: 1,
          })
        } finally {
          firstBugListener = undefined
          logged.mockRestore()
        }
      })
    }
  }
})

it('a listener that an `onBug()` listener registers while a bug is reported gets the next bug, not that one', async () => {
  const late: unknown[] = []
  let registered = false
  firstBugListener = () => {
    if (registered) return
    registered = true
    onBug((err) => void late.push(err))
  }
  const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
  try {
    callBugListeners(new Error('the telefunction fails, as the test means it to'))
    const duringItsBug = late.length
    callBugListeners(new Error('the telefunction fails, as the test means it to'))
    expect({ duringItsBug, next: late.length }).toEqual({ duringItsBug: 0, next: 1 })
  } finally {
    firstBugListener = undefined
    logged.mockRestore()
  }
})
