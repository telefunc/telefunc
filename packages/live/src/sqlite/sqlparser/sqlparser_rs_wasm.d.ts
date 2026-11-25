// sqlparser-ts' WASM bindings, which the build copies here (`copySqlparser.js`).
export function initSync(module: { module: WebAssembly.Module }): unknown
export function parse_sql(dialect: string, sql: string): any
