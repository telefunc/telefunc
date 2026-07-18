import { describe, expect, it } from 'vitest'
import { config } from './serverConfig.js'
import type { TelefuncExtensionHost } from './extensions.js'

// The extension-host seam: `config.extensions.push(ext)` calls `ext.setup(host)` synchronously at
// registration, handing an integration the reactive toolkit (a `Live` producer) WITHOUT it importing
// telefunc internals — this is how @telefunc/drizzle reaches that primitive. Nothing here is
// request-scoped: the host is stateless, so there is no context to provide or drain.

let host: TelefuncExtensionHost | undefined
config.extensions.push({
  name: 'test:extension-host',
  setup: (h) => {
    host = h
  },
})

describe('extension host — setup delivers the reactive toolkit', () => {
  it('setup runs synchronously at registration and receives the host', () => {
    expect(host).toBeDefined()
    expect(typeof host!.createLive).toBe('function')
  })

  it('createLive returns a Live producer: `.data` is the seed, and the producer verbs work', () => {
    const live = host!.createLive({ n: 1 })
    expect(live.data).toEqual({ n: 1 }) // to the caller it is a Live<T>
    expect(() => live.invalidate()).not.toThrow() // producer verb
    expect(() => live.attachSource({ subscribe: () => () => {} })).not.toThrow() // activation-source seam
  })
})
