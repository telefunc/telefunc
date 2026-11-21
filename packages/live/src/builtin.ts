export { builtin }

import { assertUsage } from './assert.js'

/** A built-in module of Node.js or Bun, `node:fs` say, loaded at once: SQLite plans a statement before it runs,
 *  without waiting, and an import would follow the package to runtimes that have none. */
function builtin(name: string): any {
  const getBuiltinModule = (globalThis as { process?: { getBuiltinModule?(name: string): unknown } }).process
    ?.getBuiltinModule
  assertUsage(
    getBuiltinModule,
    `SQLite support needs Node.js 22.3+ or Bun: it loads ${name} through process.getBuiltinModule().`,
  )
  return getBuiltinModule(name)
}
