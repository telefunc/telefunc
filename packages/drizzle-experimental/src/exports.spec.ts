import { describe, expect, it } from 'vitest'
import * as root from './index.js'

// THE PUBLIC SURFACE of @telefunc/drizzle-experimental, pinned. Owner-specified, complete:
//
//   import { derived, reactiveDrizzle } from '@telefunc/drizzle-experimental'
//   import { live } from '@telefunc/drizzle-experimental/tanstack-query'
//
// plus `Live<T>` as a TYPE from the root. A new runtime key here is a public-API change and must be
// deliberate, which is what this asserts — the engine internals (IR, extraction, capture, the cell) are
// implementation detail and must not leak out.
//
// The `./tanstack-query` subpath is deliberately NOT imported here: importing it registers the client
// reviver with telefunc as a side effect, which is the right behaviour there and the wrong thing to do
// from a spec about the root's surface. Its own export is covered by the adapter's spec.
describe('@telefunc/drizzle-experimental — public surface', () => {
  it('the root exports EXACTLY the two documented values', () => {
    expect(Object.keys(root).sort()).toEqual(['derived', 'reactiveDrizzle'])
    expect(typeof root.reactiveDrizzle).toBe('function')
    expect(typeof root.derived).toBe('function')
  })

  it('the producer verbs and the cell are NOT reachable from the root', () => {
    // `derived` returns the public `Live<R>`; the cell behind it is internal. A consumer reads `.data` —
    // it must not be handed `invalidate`/`activate`/`release` through any public name.
    for (const internal of ['LiveCell', 'Live', 'liveReplacer', 'liveReviver', 'installLiveReplacer']) {
      expect((root as Record<string, unknown>)[internal]).toBeUndefined()
    }
  })

  it('`derived` produces a value whose only public surface is `.data`', () => {
    const live = root.derived(() => 41 + 1)
    expect(live.data).toBe(42)
    // The runtime object is the cell (that is what the wire replacer serializes), but nothing in the
    // TYPE says so — `Live<T>` is `{ readonly data: T }`, which the typing spec pins at compile time.
    expect(Object.keys(live)).not.toContain('invalidate')
  })
})
