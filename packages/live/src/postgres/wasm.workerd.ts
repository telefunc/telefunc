export { wasm }

// A Worker can't compile WASM from bytes: the bundler uploads the file as a module of the Worker.
import wasm from 'libpg-query/wasm/libpg-query.wasm'

// libpg-query's glue takes workerd for Node and reads Node's module paths: https://github.com/constructive-io/libpg-query-node/issues/194
const paths = globalThis as { __filename?: string; __dirname?: string }
paths.__filename ??= '/libpg-query.js'
paths.__dirname ??= '/'
