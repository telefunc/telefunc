import { afterEach, describe, expect, it, vi } from 'vitest'
import { BackendCall } from './BackendCall.js'
import { BackendError } from './BackendError.js'

const notRun = () => new BackendError({ code: 'unreachable', outcome: 'not-run', message: 'refused before running' })
const mayHaveRun = () => new BackendError({ code: 'unreachable', outcome: 'may-have-run', message: 'reply lost' })
const answered = async () => 'ok'
const failingWith = (error: unknown) => async () => {
  throw error
}
const outcomeOf = (error: unknown) => (error instanceof BackendError ? error.outcome : 'not a BackendError')
/** Settles what only waits on promises already settled. */
const microtasks = async () => {
  for (let turn = 0; turn < 20; turn++) await Promise.resolve()
}
/** A write that runs until `answer()` or `fail(error)`. */
const pendingWrite = () => {
  const handle = { answer: () => {}, fail: (_error: unknown) => {} }
  const step = () =>
    new Promise<void>((resolve, reject) => {
      handle.answer = resolve
      handle.fail = reject
    })
  return { step, handle }
}
/** The account `run()` opens for `work`, and its answer: work other calls may join. */
const opened = <T>(work: (call: BackendCall) => Promise<T>) => {
  let account: BackendCall | undefined
  const done = BackendCall.run((call) => {
    account = call
    return work(call)
  })
  if (account === undefined) throw new Error('run() hands its work the account at once')
  return { account, done }
}

describe('BackendCall', () => {
  it("tells a step's 'not-run' as is while no write of the call may have run", async () => {
    const refused = notRun()

    const failure = await BackendCall.run(async (call) => {
      await call.read(answered)
      return call.write(failingWith(refused))
    }).catch((error: unknown) => error)

    expect(failure).toBe(refused)
  })

  it("tells 'may-have-run' for a step's 'not-run' after a write of the call was answered", async () => {
    const failure = await BackendCall.run(async (call) => {
      await call.write(answered)
      return call.read(failingWith(notRun()))
    }).catch((error: unknown) => error)

    expect(failure).toBeInstanceOf(BackendError)
    expect(failure).toMatchObject({ code: 'unreachable', outcome: 'may-have-run', message: 'refused before running' })
  })

  it("tells 'may-have-run' for a step's 'not-run' after a write of the call failed without a proof", async () => {
    const failure = await BackendCall.run(async (call) => {
      await call.write(failingWith(mayHaveRun())).catch(() => {})
      return call.write(failingWith(notRun()))
    }).catch((error: unknown) => error)

    expect(outcomeOf(failure)).toBe('may-have-run')
  })

  it("keeps 'not-run' after a write of the call that was itself refused before running", async () => {
    const failure = await BackendCall.run(async (call) => {
      await call.write(failingWith(notRun())).catch(() => {})
      return call.read(failingWith(notRun()))
    }).catch((error: unknown) => error)

    expect(outcomeOf(failure)).toBe('not-run')
  })

  it("tells 'may-have-run' for a step's 'not-run' while another write of the call is in flight", async () => {
    const write = pendingWrite()

    const failure = await BackendCall.run(async (call) => {
      const inFlight = call.write(write.step)
      const failure = await call.write(failingWith(notRun())).catch((error: unknown) => error)
      write.handle.answer()
      await inFlight
      return failure
    })

    expect(outcomeOf(failure)).toBe('may-have-run')
  })

  it('tells what an inner call of several steps tells, through an outer step', async () => {
    const inner = () =>
      BackendCall.run(async (call) => {
        await call.write(answered)
        return call.read(failingWith(notRun()))
      })

    const failure = await BackendCall.run((outer) => outer.write(inner)).catch((error: unknown) => error)

    expect(outcomeOf(failure)).toBe('may-have-run')
  })

  it('passes a failure that is not a BackendError through', async () => {
    const bug = new TypeError('a bug')

    const failure = await BackendCall.run(async (call) => {
      await call.write(answered)
      return call.read(failingWith(bug))
    }).catch((error: unknown) => error)

    expect(failure).toBe(bug)
  })

  describe('run(): the call answers at its close', () => {
    afterEach(() => {
      vi.useRealTimers()
    })

    it('settles a failure when its last write in flight settles, with no timer of its own', async () => {
      // A timer in the close would never fire here, and the answer would never come.
      vi.useFakeTimers()
      const first = pendingWrite()
      const last = pendingWrite()
      let settled = false

      const closing = BackendCall.run(async (call) => {
        await Promise.all([call.write(first.step), call.write(last.step), failingWith(notRun())()])
      })
        .catch((error: unknown) => error)
        .finally(() => (settled = true))
      await microtasks()
      const whileBoth = settled
      first.handle.answer()
      await microtasks()
      const whileLast = settled
      last.handle.answer()

      expect(outcomeOf(await closing)).toBe('may-have-run')
      expect({ whileBoth, whileLast }).toEqual({ whileBoth: false, whileLast: false })
    })

    it("reads its answer after the writes in flight settled: 'not-run' once the one in flight was refused too", async () => {
      const write = pendingWrite()

      const closing = BackendCall.run(async (call) => {
        await Promise.all([call.write(write.step), failingWith(notRun())()])
      }).catch((error: unknown) => error)
      await microtasks()
      write.handle.fail(notRun())

      expect(outcomeOf(await closing)).toBe('not-run')
    })

    it('refuses a write handed over after the close, inside its sibling, with no unhandled rejection', async () => {
      const unhandled: unknown[] = []
      const onUnhandled = (reason: unknown) => {
        unhandled.push(reason)
      }
      process.on('unhandledRejection', onUnhandled)
      try {
        const refused = notRun()
        let open = () => {}
        const later = new Promise<void>((resolve) => (open = resolve))
        let ran = false
        let refusal: unknown

        const failure = await BackendCall.run(async (call) => {
          const sibling = async () => {
            await later
            await call
              .write(async () => {
                ran = true
              })
              .catch((error: unknown) => {
                refusal = error
                throw error
              })
          }
          await Promise.all([failingWith(refused)(), sibling()])
        }).catch((error: unknown) => error)
        open()
        await new Promise((resolve) => setImmediate(resolve))

        expect(failure).toBe(refused)
        expect({ ran, refusal: outcomeOf(refusal), unhandled }).toEqual({
          ran: false,
          refusal: 'not-run',
          unhandled: [],
        })
      } finally {
        process.off('unhandledRejection', onUnhandled)
      }
    })

    it('settles a failure once the work it joined settled', async () => {
      const write = pendingWrite()
      const shared = opened((account) => account.write(write.step))
      let settled = false

      const closing = BackendCall.run(async (call) => {
        await Promise.all([call.join(shared.account, shared.done), failingWith(notRun())()])
      })
        .catch((error: unknown) => error)
        .finally(() => (settled = true))
      await microtasks()
      const whileShared = settled
      write.handle.answer()

      expect(outcomeOf(await closing)).toBe('may-have-run')
      expect(whileShared).toBe(false)
    })
  })

  describe('all(): steps side by side', () => {
    it("tells 'may-have-run' for a step's 'not-run' once a sibling that was still reading hands a write over", async () => {
      let readDone = () => {}
      const read = new Promise<void>((resolve) => (readDone = resolve))

      const failure = await BackendCall.run(async (call) => {
        const sibling = async () => {
          await call.read(() => read)
          await call.write(answered)
        }
        const refused = call.read(failingWith(notRun()))
        const joined = call.all([refused, sibling()]).catch((error: unknown) => error)
        await refused.catch(() => {})
        readDone()
        return await joined
      })

      expect(outcomeOf(failure)).toBe('may-have-run')
    })

    it("keeps 'not-run' where no step of the call wrote", async () => {
      const failure = await BackendCall.run((call) =>
        call.all([call.read(failingWith(notRun())), call.read(answered)]),
      ).catch((error: unknown) => error)

      expect(outcomeOf(failure)).toBe('not-run')
    })

    it('gives the values in the order of the steps', async () => {
      let first = (_value: string) => {}

      const values = await BackendCall.run((call) => {
        const joined = call.all([
          call.read(() => new Promise<string>((resolve) => (first = resolve))),
          call.read(answered),
        ])
        first('first')
        return joined
      })

      expect(values).toEqual(['first', 'ok'])
    })

    it('waits for every step before it tells a failure', async () => {
      let settled = false
      const slow = async () => {
        await new Promise((resolve) => setTimeout(resolve, 10))
        settled = true
      }

      await BackendCall.run((call) => call.all([call.read(failingWith(notRun())), slow()])).catch(() => {})

      expect(settled).toBe(true)
    })
  })

  describe('join(): work under another account, which other calls may share', () => {
    it("tells 'may-have-run' for a later step's 'not-run' once the shared work wrote and succeeded", async () => {
      const shared = opened((account) => account.write(answered))

      const failure = await BackendCall.run(async (call) => {
        await call.join(shared.account, shared.done)
        return call.read(failingWith(notRun()))
      }).catch((error: unknown) => error)

      expect(outcomeOf(failure)).toBe('may-have-run')
    })

    it("tells 'may-have-run' for the shared work's 'not-run' after its own write", async () => {
      const shared = opened(async (account) => {
        await account.write(answered)
        throw notRun()
      })

      const failure = await BackendCall.run((call) => call.join(shared.account, shared.done)).catch(
        (error: unknown) => error,
      )

      expect(outcomeOf(failure)).toBe('may-have-run')
    })

    it("keeps 'not-run' where the shared work only read", async () => {
      const shared = opened((account) => account.read(answered))

      const failure = await BackendCall.run(async (call) => {
        await call.join(shared.account, shared.done)
        return call.read(failingWith(notRun()))
      }).catch((error: unknown) => error)

      expect(outcomeOf(failure)).toBe('not-run')
    })

    it("gives a second call that joins the same work the same account: 'may-have-run' after its write", async () => {
      const shared = opened((account) => account.write(answered))
      const joiner = () =>
        BackendCall.run(async (call) => {
          await call.join(shared.account, shared.done)
          return call.read(failingWith(notRun()))
        }).catch((error: unknown) => error)

      const failures = await Promise.all([joiner(), joiner()])

      expect(failures.map(outcomeOf)).toEqual(['may-have-run', 'may-have-run'])
    })
  })
})
