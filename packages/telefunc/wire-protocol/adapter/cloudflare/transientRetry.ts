export { transientRetryDelay }

/** As Cloudflare's error-handling page shows: 3 attempts, the waits between them drawn as `100 ms × random × 2^n`. Each
 *  attempt also carries the runtime's own retries of a call that never reached the Durable Object. */
const TRANSIENT_ATTEMPTS = 3
const TRANSIENT_BACKOFF_BASE_MS = 100

/** How long to wait before making a Durable Object call again after its `failures`-th failure, with `error`; `null`
 *  where it isn't made again: a failure Cloudflare marks `.overloaded`, or doesn't mark `.retryable`, or the last. */
function transientRetryDelay(error: unknown, failures: number): number | null {
  if (!isRetryable(error) || failures >= TRANSIENT_ATTEMPTS) return null
  return TRANSIENT_BACKOFF_BASE_MS * Math.random() * 2 ** (failures - 1)
}

/** A failure Cloudflare marks as transient: `.retryable`, unless `.overloaded`, which it says not to retry. */
function isRetryable(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false
  return 'retryable' in error && error.retryable === true && !('overloaded' in error && error.overloaded === true)
}
