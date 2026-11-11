export { wasm }

/** libpg-query's WASM, where the runtime can't read it from its file: Node, Bun and Deno can. */
const wasm: WebAssembly.Module | undefined = undefined
