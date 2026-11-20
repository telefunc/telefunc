import { describe, expect, it } from 'vitest'
import { BackendCall } from './BackendCall.js'
import { BackendError } from './BackendError.js'

const notRun = () => new BackendError({ code: 'unreachable', outcome: 'not-run', message: 'refused before running' })
const mayHaveRun = () => new BackendError({ code: 'unreachable', outcome: 'may-have-run', message: 'reply lost' })
const answered = async () => 'ok'
const failingWith = (error: unknown) => async () => {
  throw error
}
const outcomeOf = (error: unknown) => (error instanceof BackendError ? error.outcome : 'not a BackendError')

describe('BackendCall', () => {
  it("tells a step's 'not-run' as is while no write of the call may have run", async () => {
    const call = new BackendCall()
    await call.read(answered)
    const refused = notRun()

    const failure = await call.write(failingWith(refused)).catch((error: unknown) => error)

    expect(failure).toBe(refused)
  })

  it("tells 'may-have-run' for a step's 'not-run' after a write of the call was answered", async () => {
    const call = new BackendCall()
    await call.write(answered)

    const failure = await call.read(failingWith(notRun())).catch((error: unknown) => error)

    expect(failure).toBeInstanceOf(BackendError)
    expect(failure).toMatchObject({ code: 'unreachable', outcome: 'may-have-run', message: 'refused before running' })
  })

  it("tells 'may-have-run' for a step's 'not-run' after a write of the call failed without a proof", async () => {
    const call = new BackendCall()
    await call.write(failingWith(mayHaveRun())).catch(() => {})

    const failure = await call.write(failingWith(notRun())).catch((error: unknown) => error)

    expect(outcomeOf(failure)).toBe('may-have-run')
  })

  it("keeps 'not-run' after a write of the call that was itself refused before running", async () => {
    const call = new BackendCall()
    await call.write(failingWith(notRun())).catch(() => {})

    const failure = await call.read(failingWith(notRun())).catch((error: unknown) => error)

    expect(outcomeOf(failure)).toBe('not-run')
  })

  it("tells 'may-have-run' for a step's 'not-run' while another write of the call is in flight", async () => {
    const call = new BackendCall()
    let answer = () => {}
    const inFlight = call.write(() => new Promise<void>((resolve) => (answer = resolve)))

    const failure = await call.write(failingWith(notRun())).catch((error: unknown) => error)
    answer()
    await inFlight

    expect(outcomeOf(failure)).toBe('may-have-run')
  })

  it('tells what an inner call of several steps tells, through an outer step', async () => {
    const outer = new BackendCall()
    const inner = async () => {
      const call = new BackendCall()
      await call.write(answered)
      return call.read(failingWith(notRun()))
    }

    const failure = await outer.write(inner).catch((error: unknown) => error)

    expect(outcomeOf(failure)).toBe('may-have-run')
  })

  it('passes a failure that is not a BackendError through', async () => {
    const call = new BackendCall()
    await call.write(answered)
    const bug = new TypeError('a bug')

    const failure = await call.read(failingWith(bug)).catch((error: unknown) => error)

    expect(failure).toBe(bug)
  })
})
