// Copies sqlparser-ts' WASM bindings and their license into dist, where `sqlite/sqlparser.workerd.js` imports them.
// TODO(upstream-sqlparser-ts): import them from the package once its `exports` reaches them, and delete this.
import { cpSync } from 'node:fs'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'

const from = new URL('../wasm/', pathToFileURL(createRequire(import.meta.url).resolve('@guanmingchiu/sqlparser-ts')))
for (const file of ['sqlparser_rs_wasm.js', 'sqlparser_rs_wasm_bg.wasm', 'LICENSE'])
  cpSync(new URL(file, from), new URL(`./dist/sqlite/sqlparser/${file}`, import.meta.url))
