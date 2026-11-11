export { loadParser }

// The build copies both; import them from the package once https://github.com/guan404ming/sqlparser-ts/pull/29 exports them.
import { initSync, parse_sql } from './sqlparser/sqlparser_rs_wasm.js'
import module from './sqlparser/sqlparser_rs_wasm_bg.wasm'

/** A Worker can't compile WASM from bytes: the bundler uploads the file as a module of the Worker. */
function loadParser(): { parse_sql(dialect: string, sql: string): any } {
  initSync({ module })
  return { parse_sql }
}
