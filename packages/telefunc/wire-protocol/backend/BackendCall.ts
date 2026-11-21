export { BackendCall }

import { BackendError } from './BackendError.js'

/** One public call of several steps against the backend's store: a failure says the call may have run once a write of
 *  it was handed over without a proof that it didn't run, whatever the failing step proves of itself. The call answers
 *  once, at its close (`run()`), after nothing it started can still write. It waits on other work in two ways: `all()`,
 *  for its own steps side by side, and `join()`, for work under another account. */
class BackendCall {
  /** Writes handed over and not settled yet. */
  private writing = 0
  /** A write was answered, or failed without a proof that it didn't run. */
  private wrote = false
  /** The call answered or is about to: a write handed over now is refused. */
  private closed = false
  /** Writes handed over and work joined, not settled yet: the close waits for them. */
  private readonly unsettled = new Set<Promise<unknown>>()

  private constructor() {}

  /** The value of `work`, run under a new account. Then the call closes: a write it hands over later is refused, and
   *  the answer waits until each write already handed over and each work joined settled, so the store no longer
   *  changes for this call. Its failure is told as the call stands then. The close waits for nothing else. */
  static async run<T>(work: (call: BackendCall) => Promise<T>): Promise<T> {
    const call = new BackendCall()
    let result: { value: T } | { error: unknown }
    try {
      result = { value: await work(call) }
    } catch (error) {
      result = { error }
    }
    call.closed = true
    while (call.unsettled.size > 0) await Promise.allSettled(call.unsettled)
    if ('error' in result) throw call.failure(result.error, 0)
    return result.value
  }

  async read<T>(step: () => Promise<T>): Promise<T> {
    try {
      return await step()
    } catch (error) {
      throw this.failure(error, 0)
    }
  }

  write<T>(step: () => Promise<T>): Promise<T> {
    if (this.closed) {
      const message = 'Not run: the call it was a step of had already failed or answered'
      return Promise.reject(new BackendError({ code: 'failed', outcome: 'not-run', message }))
    }
    return this.track(this.written(step))
  }

  /** The values of `steps`, which run side by side, in order; else, once every one of them settled, the first failure
   *  as the call stands then. A step that fails before a sibling hands its write over is told after that write. */
  async all<T extends readonly unknown[]>(steps: T): Promise<{ -readonly [K in keyof T]: Awaited<T[K]> }> {
    for (const result of await Promise.allSettled(steps)) {
      if (result.status === 'rejected') throw this.failure(result.reason, 0)
    }
    return await Promise.all(steps)
  }

  /** The value of `work`, which runs under the account `shared` and may be shared with other calls, once it settled:
   *  a write `shared` counted then counts as this call's, so a failure of `work`, or of a later step of this call, is
   *  told as this call stands with it. */
  async join<T>(shared: BackendCall, work: Promise<T>): Promise<T> {
    const [settled] = await Promise.allSettled([this.track(work)])
    if (shared.wrote || shared.writing > 0) this.wrote = true
    if (settled.status === 'rejected') throw this.failure(settled.reason, 0)
    return settled.value
  }

  private async written<T>(step: () => Promise<T>): Promise<T> {
    this.writing++
    try {
      const result = await step()
      this.wrote = true
      return result
    } catch (error) {
      if (!(error instanceof BackendError) || error.outcome !== 'not-run') this.wrote = true
      throw this.failure(error, 1)
    } finally {
      this.writing--
    }
  }

  /** `work`, counted as unsettled until it settles. */
  private track<T>(work: Promise<T>): Promise<T> {
    this.unsettled.add(work)
    const settle = () => {
      this.unsettled.delete(work)
    }
    work.then(settle, settle)
    return work
  }

  /** What `error` of a step tells of the whole call; `own` is the step's own write, still counted in `writing`. */
  private failure(error: unknown, own: 0 | 1): unknown {
    if (!(error instanceof BackendError) || error.outcome !== 'not-run') return error
    if (!this.wrote && this.writing === own) return error
    return new BackendError({ code: error.code, outcome: 'may-have-run', message: error.message })
  }
}
