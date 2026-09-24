export { assert }

const projectName = '@telefunc/rxjs'
const version = '0.1.0'
const errorPrefix = `[${projectName}@${version}]`
const bugPrefix = `${errorPrefix}[Bug]`

function assert(condition: unknown, debugInfo?: unknown): asserts condition {
  if (condition) return
  const debugStr = debugInfo ? (typeof debugInfo === 'string' ? debugInfo : '`' + JSON.stringify(debugInfo) + '`') : ''
  throw new Error(
    [bugPrefix, 'You stumbled upon a bug in the source code of', `${projectName}.`, debugStr].filter(Boolean).join(' '),
  )
}
