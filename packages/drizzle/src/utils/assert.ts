export { assert, assertUsage }

/** A broken contract with the user (misuse, unsupported setup). The message is shown as-is. */
function assertUsage(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`[@telefunc/drizzle] ${message}`)
}

/** A broken internal invariant — never expected to fire; if it does, it's our bug. */
function assert(condition: unknown): asserts condition {
  if (!condition) {
    throw new Error(
      '[@telefunc/drizzle] You stumbled upon a bug. Reach out at https://github.com/brillout/telefunc/issues/new and include this error stack.',
    )
  }
}
