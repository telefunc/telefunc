export { report }

/** Every diagnostic the write-capture path emits, and the only place it calls `console.error`.
 *
 *  Non-throwing by construction, because each of these reports sits IMMEDIATELY BEFORE a recovery: the
 *  substitution retry, a savepoint rewind, a coarse degradation. A host whose console throws — a patched or
 *  instrumented console, a logger that rejects a circular argument — would take the recovery down with it
 *  and turn "the write is safe" into "the write failed", which is precisely the failure the capture path
 *  exists to prevent. Telling the operator is best-effort; the recovery is not.
 *
 *  Shared by the capture facade and every module it delegates to, so the guarantee holds for all of them
 *  rather than being re-established per file. */
function report(...args: unknown[]): void {
  try {
    console.error(...args)
  } catch {
    // A console that cannot be written to is not a reason to fail a committed write.
  }
}
