export { assertUsage }

/** A broken contract with the user (misuse, unsupported setup). The message is shown as-is. */
function assertUsage(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`[@telefunc/drizzle-experimental] ${message}`)
}
