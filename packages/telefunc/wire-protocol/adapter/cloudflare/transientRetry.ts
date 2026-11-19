export { transientRetryDelay }

/** As Cloudflare's error-handling page shows: 3 attempts, the waits between them drawn as `100 ms × random × 2^n`. Each
 *  attempt also carries the runtime's own retries, which start none more than 10 s after the call did; ours start none
 *  past the same 10 s from the first, so no attempt of either starts later. */
const TRANSIENT_ATTEMPTS = 3
const TRANSIENT_BACKOFF_BASE_MS = 100
const TRANSIENT_WINDOW_MS = 10_000

/** How long to wait before making a Durable Object call again after its `failures`-th failure, with `error`, `elapsed`
 *  ms after its first attempt started; `null` where it isn't made again: a failure Cloudflare marks `.overloaded`, or
 *  doesn't mark `.retryable`, the last attempt, or one whose next would start past the window. */
function transientRetryDelay(error: unknown, failures: number, elapsed: number): number | null {
  if (!isRetryable(error) || failures >= TRANSIENT_ATTEMPTS) return null
  const delay = TRANSIENT_BACKOFF_BASE_MS * Math.random() * 2 ** (failures - 1)
  return elapsed + delay < TRANSIENT_WINDOW_MS ? delay : null
}

/** A failure Cloudflare marks as transient: `.retryable`, unless `.overloaded`, which it says not to retry. */
function isRetryable(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false
  return 'retryable' in error && error.retryable === true && !('overloaded' in error && error.overloaded === true)
}
