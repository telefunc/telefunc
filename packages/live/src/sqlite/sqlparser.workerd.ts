export { loadParser }

// TODO(upstream-sqlparser-ts): import both from the package once its `exports` reaches them; the build copies them.
import { initSync, parse_sql } from './sqlparser/sqlparser_rs_wasm.js'
import module from './sqlparser/sqlparser_rs_wasm_bg.wasm'

/** A Worker can't compile WASM from bytes: the bundler uploads the file as a module of the Worker. */
function loadParser(): { parse_sql(dialect: string, sql: string): any } {
  initSync({ module })
  return { parse_sql }
}
