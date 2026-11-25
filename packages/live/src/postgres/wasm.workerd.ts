export { wasm }

// A Worker can't compile WASM from bytes: the bundler uploads the file as a module of the Worker.
import wasm from 'libpg-query/wasm/libpg-query.wasm'
