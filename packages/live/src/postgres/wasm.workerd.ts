export { wasm }

// A Worker can't compile WASM from bytes: the bundler uploads the file as a module of the Worker.
import wasm from 'libpg-query/wasm/libpg-query.wasm'

// TODO (upstream issue to be filed): libpg-query's glue takes workerd for Node and reads Node's module paths.
const paths = globalThis as { __filename?: string; __dirname?: string }
paths.__filename ??= '/libpg-query.js'
paths.__dirname ??= '/'
