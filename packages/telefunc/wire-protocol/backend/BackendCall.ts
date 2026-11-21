export { BackendCall }

import { BackendError } from './BackendError.js'

/** One public call of several steps against the backend's store: a failure says the call may have run once a write of
 *  it was handed over without a proof that it didn't run, whatever the failing step proves of itself. */
class BackendCall {
  /** Writes handed over and not settled yet. */
  private writing = 0
  /** A write was answered, or failed without a proof that it didn't run. */
  private wrote = false

  async read<T>(step: () => Promise<T>): Promise<T> {
    try {
      return await step()
    } catch (error) {
      throw this.failure(error, 0)
    }
  }

  async write<T>(step: () => Promise<T>): Promise<T> {
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

  /** The values of `steps`, which run side by side, in order; else, once every one of them settled, the first failure
   *  as the call stands then. A step that fails before a sibling hands its write over is told after that write. */
  async all<T extends readonly unknown[]>(steps: T): Promise<{ -readonly [K in keyof T]: Awaited<T[K]> }> {
    for (const result of await Promise.allSettled(steps)) {
      if (result.status === 'rejected') throw this.failure(result.reason, 0)
    }
    return await Promise.all(steps)
  }

  /** What `error` of a step tells of the whole call; `own` is the step's own write, still counted in `writing`. */
  private failure(error: unknown, own: 0 | 1): unknown {
    if (!(error instanceof BackendError) || error.outcome !== 'not-run') return error
    if (!this.wrote && this.writing === own) return error
    return new BackendError({ code: error.code, outcome: 'may-have-run', message: error.message })
  }
}
