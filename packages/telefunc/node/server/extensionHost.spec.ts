import { afterEach, describe, expect, it, vi } from 'vitest'
import { config } from './serverConfig.js'
import { provideTelefuncContext } from './context/getContext.js'
import { getRawContext } from './context/context.js'
import { drainPostSerializeDisposers } from './context/postSerialize.js'
import type { TelefuncExtensionHost } from './extensions.js'

// The extension-host seam: `config.extensions.push(ext)` calls `ext.setup(host)` synchronously at
// registration, handing an integration the reactive toolkit (a `Live` producer + per-request cleanup)
// WITHOUT it importing telefunc internals — this is how @telefunc/drizzle reaches those primitives.

const tick = () => new Promise<void>((resolve) => setTimeout(resolve, 0))

let host: TelefuncExtensionHost | undefined
config.extensions.push({
  name: 'test:extension-host',
  setup: (h) => {
    host = h
  },
})

// Flush the sync-mode null timer scheduled by provideTelefuncContext so it never leaks into the next test.
afterEach(async () => {
  await tick()
})

describe('extension host — setup delivers the reactive toolkit', () => {
  it('setup runs synchronously at registration and receives a host with the two capabilities', () => {
    expect(host).toBeDefined()
    expect(typeof host!.createLive).toBe('function')
    expect(typeof host!.onRequestCleanup).toBe('function')
  })

  it('createLive returns a Live producer: `.data` is the seed, and the producer verbs work', () => {
    const live = host!.createLive({ n: 1 })
    expect(live.data).toEqual({ n: 1 }) // to the caller it is a Live<T>
    expect(() => live.invalidate()).not.toThrow() // producer verb
    expect(() => live.attachSource({ subscribe: () => () => {} })).not.toThrow() // activation-source seam
  })

  it('onRequestCleanup registers a cleanup that the post-serialize drain runs exactly once', () => {
    provideTelefuncContext({})
    const context = getRawContext()!
    const cleanup = vi.fn()
    host!.onRequestCleanup(cleanup)
    expect(cleanup).not.toHaveBeenCalled() // inert until the execute→serialize pipeline settles
    drainPostSerializeDisposers(context)
    expect(cleanup).toHaveBeenCalledTimes(1)
    drainPostSerializeDisposers(context) // idempotent — a second drain does not re-run it
    expect(cleanup).toHaveBeenCalledTimes(1)
  })

  it('onRequestCleanup throws when called outside a telefunction (no live context to bind)', () => {
    // afterEach flushed any prior context's null timer, so getRawContext() is null here.
    expect(() => host!.onRequestCleanup(() => {})).toThrow(/must be called inside a telefunction/)
  })
})
