// The `./tanstack-query` subpath: the client-side adapter that turns a telefunction returning a `Live`
// into a TanStack Query `queryFn` that keeps itself current.
//
// A separate entry from the package root on purpose — the root is server-side and must not drag
// `@tanstack/query-core` into a project that does not use TanStack.

export { live } from './live.js'
