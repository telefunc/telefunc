export { assertUsage }

function assertUsage(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`[@telefunc/live][Wrong Usage] ${message}`)
}
