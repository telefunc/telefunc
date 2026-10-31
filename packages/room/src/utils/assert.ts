export { assert, assertUsage }

const projectName = '@telefunc/room'
const version = '0.1.0'
const errorPrefix = `[${projectName}@${version}]`

function assert(condition: unknown, debugInfo?: unknown): asserts condition {
  if (condition) return
  const debugStr = debugInfo ? (typeof debugInfo === 'string' ? debugInfo : '`' + JSON.stringify(debugInfo) + '`') : ''
  throw new Error(
    [`${errorPrefix}[Bug]`, 'You stumbled upon a bug in the source code of', `${projectName}.`, debugStr]
      .filter(Boolean)
      .join(' '),
  )
}

function assertUsage(condition: unknown, errorMessage: string): asserts condition {
  if (condition) return
  throw new Error(`${errorPrefix}[Wrong Usage] ${errorMessage}`)
}
