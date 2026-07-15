import { describe, expect, it, vi } from 'vitest'
import { addLiveSource, takeLiveSources, disposeUntakenLiveSources } from './source.js'
import type { LiveSource } from './source.js'
import { restoreContext } from '../context/context.js'

/** Run `fn` with a fresh restored request context so `getRawContext()` is non-null. */
function inRequest(fn: () => void): void {
  restoreContext({}, fn)
}

function fakeSource(identity: string, opts: { dispose?: () => void; teardown?: () => void } = {}): LiveSource {
  const source: LiveSource = {
    identity,
    subscribe() {
      return opts.teardown ?? (() => {})
    },
  }
  if (opts.dispose) source.dispose = opts.dispose
  return source
}

describe('LiveSource registry (§3.C)', () => {
  it('T1.C1 identity dedup — first wins', () => {
    inRequest(() => {
      addLiveSource(fakeSource('a'))
      addLiveSource(fakeSource('a'))
      expect(takeLiveSources()).toHaveLength(1)
    })
  })

  it('T1.C2 distinct identities kept in insertion order', () => {
    inRequest(() => {
      addLiveSource(fakeSource('a'))
      addLiveSource(fakeSource('b'))
      expect(takeLiveSources().map((s) => s.identity)).toEqual(['a', 'b'])
    })
  })

  it('T1.C3 take is one-shot', () => {
    inRequest(() => {
      addLiveSource(fakeSource('a'))
      expect(takeLiveSources()).toHaveLength(1)
      expect(takeLiveSources()).toHaveLength(0)
    })
  })

  it('T1.C4 cap boundary — 64 accepted', () => {
    inRequest(() => {
      for (let i = 0; i < 64; i++) addLiveSource(fakeSource(`id-${i}`))
      expect(takeLiveSources()).toHaveLength(64)
    })
  })

  it('T1.C5 cap boundary — 65th asserts AND disposes the rejected source', () => {
    inRequest(() => {
      for (let i = 0; i < 64; i++) addLiveSource(fakeSource(`id-${i}`))
      const dispose = vi.fn()
      expect(() => addLiveSource(fakeSource('id-64', { dispose }))).toThrow()
      expect(dispose).toHaveBeenCalledTimes(1) // rejected source not leaked
    })
  })

  it('T1.C6 subscribed source → teardown on channel close; settle does neither', () => {
    inRequest(() => {
      const dispose = vi.fn()
      const teardown = vi.fn()
      const source = fakeSource('a', { dispose, teardown })
      addLiveSource(source)
      const [taken] = takeLiveSources() // result hook takes it
      const channelClose = taken!.subscribe(() => {}) // ...and subscribes
      disposeUntakenLiveSources() // settle
      expect(dispose).not.toHaveBeenCalled()
      expect(teardown).not.toHaveBeenCalled()
      channelClose() // permanent channel close tears it down
      expect(teardown).toHaveBeenCalledTimes(1)
    })
  })

  it('T1.C7 never-subscribed leftover → disposed at settle, never torn down', () => {
    inRequest(() => {
      const dispose = vi.fn()
      addLiveSource(fakeSource('a', { dispose }))
      disposeUntakenLiveSources()
      expect(dispose).toHaveBeenCalledTimes(1)
      expect(takeLiveSources()).toHaveLength(0)
    })
  })

  it('T1.C8 dispose is optional', () => {
    inRequest(() => {
      addLiveSource(fakeSource('a')) // no dispose
      expect(() => disposeUntakenLiveSources()).not.toThrow()
    })
  })

  it('T1.C9 exactly one terminal action per resource-owning source; zero for a no-resource source', () => {
    // Subscribed (taken) source ⇒ (teardown 1, dispose 0), via channel close — settle does neither.
    inRequest(() => {
      const dispose = vi.fn()
      const teardown = vi.fn()
      addLiveSource(fakeSource('sub', { dispose, teardown }))
      const [taken] = takeLiveSources()
      const channelClose = taken!.subscribe(() => {})
      disposeUntakenLiveSources()
      channelClose()
      expect([teardown.mock.calls.length, dispose.mock.calls.length]).toEqual([1, 0])
    })
    // Never-subscribed leftovers ⇒ resource-owning (0,1), no-resource (0,0). No take called.
    inRequest(() => {
      const leftDispose = vi.fn()
      const noResourceDispose = vi.fn()
      addLiveSource(fakeSource('left', { dispose: leftDispose }))
      addLiveSource(fakeSource('none')) // omits dispose → no resource
      disposeUntakenLiveSources()
      expect(leftDispose).toHaveBeenCalledTimes(1)
      expect(noResourceDispose).not.toHaveBeenCalled()
    })
  })

  it('T1.C10 no context → assert', async () => {
    await new Promise((resolve) => setTimeout(resolve, 0)) // let any prior sync context clear
    expect(() => addLiveSource(fakeSource('a'))).toThrow()
  })

  it('T1.C11 duplicate first-wins — core disposes the rejected duplicate, first untouched', () => {
    inRequest(() => {
      const keptDispose = vi.fn()
      const dupDispose = vi.fn()
      addLiveSource(fakeSource('a', { dispose: keptDispose }))
      addLiveSource(fakeSource('a', { dispose: dupDispose }))
      expect(dupDispose).toHaveBeenCalledTimes(1)
      expect(keptDispose).not.toHaveBeenCalled()
      expect(takeLiveSources()).toHaveLength(1)
    })
  })

  it('T1.C9 attempt-all — a throwing disposer does not block later disposers', () => {
    inRequest(() => {
      const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
      const secondDispose = vi.fn()
      addLiveSource({
        identity: 'first',
        subscribe: () => () => {},
        dispose: () => {
          throw new Error('disposer failed')
        },
      })
      addLiveSource(fakeSource('second', { dispose: secondDispose }))
      expect(() => disposeUntakenLiveSources()).not.toThrow()
      expect(secondDispose).toHaveBeenCalledTimes(1) // not blocked by the first throw
      errorSpy.mockRestore()
    })
  })

  it('T1.C12 a source added after take is not orphaned — settle disposes it', () => {
    inRequest(() => {
      addLiveSource(fakeSource('taken'))
      takeLiveSources() // the result hook takes the set (clears it)
      const lateDispose = vi.fn()
      addLiveSource(fakeSource('late', { dispose: lateDispose })) // a later hook adds one
      disposeUntakenLiveSources()
      expect(lateDispose).toHaveBeenCalledTimes(1) // disposed, not leaked
    })
  })
})
