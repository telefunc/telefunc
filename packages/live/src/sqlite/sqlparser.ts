export { loadParser }

import { builtin } from '../builtin.js'

/** Loads the parser's WASM at once: better-sqlite3 and bun:sqlite plan a statement before it runs, without waiting.
 *  The package's `init()` reads the same file, asynchronously; its glue loads it synchronously. */
function loadParser(): { parse_sql(dialect: string, sql: string): any } {
  const { createRequire } = builtin('node:module')
  const { readFileSync } = builtin('node:fs')
  const { fileURLToPath, pathToFileURL } = builtin('node:url')
  const require = createRequire(import.meta.url)
  const main = pathToFileURL(require.resolve('@guanmingchiu/sqlparser-ts'))
  const glue = require(fileURLToPath(new URL('../wasm/sqlparser_rs_wasm.js', main)))
  glue.initSync({ module: readFileSync(new URL('../wasm/sqlparser_rs_wasm_bg.wasm', main)) })
  return glue
}
