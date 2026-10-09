export { causeChain }

/** An error and everything it wraps, outermost first.
 *
 *  Drizzle re-throws driver errors wrapped ("Failed query: …"), so the fact worth reading — a SQLSTATE, a
 *  permission refusal — is on an inner link rather than on what was caught. Reading only the outer error
 *  classified every refusal as a syntax refusal, which silently disabled a capability fallback; both
 *  callers had then written this walk for themselves, with the same depth bound and no shared name for it.
 *
 *  BOUNDED at five links, because a `cause` chain can be circular and a diagnostic must never hang the
 *  path it is diagnosing. Five is past anything drizzle wraps. */
function* causeChain(error: unknown): Generator<object> {
  let current = error
  for (let depth = 0; current != null && depth < 5; depth++) {
    yield current as object
    current = (current as { cause?: unknown }).cause
  }
}
