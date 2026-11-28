export { assert, isBug }

const projectName = '@telefunc/redis'
const bugPrefix = `[${projectName}][Bug]`

/** Whether `error` is one `assert()` threw. */
function isBug(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith(bugPrefix)
}

function assert(condition: unknown, debugInfo?: unknown): asserts condition {
  if (condition) return
  const debugStr = debugInfo ? (typeof debugInfo === 'string' ? debugInfo : '`' + JSON.stringify(debugInfo) + '`') : ''
  throw new Error(
    [bugPrefix, 'You stumbled upon a bug in the source code of', `${projectName}.`, debugStr].filter(Boolean).join(' '),
  )
}
