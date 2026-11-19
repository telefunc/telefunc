export { TransientRetry }

/** As Cloudflare's error-handling page shows: 3 attempts, the waits between them drawn as `100 ms × random × 2^n`. */
const TRANSIENT_ATTEMPTS = 3
const TRANSIENT_BACKOFF_BASE_MS = 100
/** The runtime starts none of a call's own retries more than 10 s after the call; a retry of ours starts none later
 *  either, from the first attempt. */
const TRANSIENT_WINDOW_MS = 10_000

/** A Durable Object call that is made again where it failed as Cloudflare says to retry, from its first attempt on. */
class TransientRetry {
  private readonly startedAt = Date.now()
  private failures = 0
  private lastError: unknown = undefined
  private expired = false

  /** Makes an attempt's call, where it is made: one that would start past the window isn't, and rejects with the last
   *  failure. */
  attempt<T>(call: () => Promise<T>): Promise<T> {
    if (this.failures === 0 || Date.now() - this.startedAt < TRANSIENT_WINDOW_MS) return call()
    this.expired = true
    return Promise.reject(this.lastError)
  }

  /** How long to wait before the next attempt after an attempt failed with `error`; `null` where none is made: a failure
   *  Cloudflare marks `.overloaded`, or doesn't mark `.retryable`, the last attempt, or past the window. */
  delayAfter(error: unknown): number | null {
    this.failures++
    this.lastError = error
    if (this.expired || !isRetryable(error) || this.failures >= TRANSIENT_ATTEMPTS) return null
    return TRANSIENT_BACKOFF_BASE_MS * Math.random() * 2 ** (this.failures - 1)
  }
}

/** A failure Cloudflare marks as transient: `.retryable`, unless `.overloaded`, which it says not to retry. */
function isRetryable(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false
  return 'retryable' in error && error.retryable === true && !('overloaded' in error && error.overloaded === true)
}
