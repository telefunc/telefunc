import { describe, expect, it } from 'vitest'
import * as shared from './shared.js'
import { __TQ__CHANNEL_KEY, __TQ__DATA_KEY, isWrapper } from './shared.js'

describe('shared (§3.D)', () => {
  it('T2.D1 isWrapper requires OWN wrapper properties (prototype-forged / ordinary negatives)', () => {
    const genuine = { [__TQ__DATA_KEY]: 1, [__TQ__CHANNEL_KEY]: { listen() {}, close() {} } }
    expect(isWrapper(genuine)).toBe(true)

    // prototype-forged: the keys live on the prototype, not as own properties (rejected by hasOwn
    // even though the inherited channel looks valid)
    const proto = { [__TQ__DATA_KEY]: 1, [__TQ__CHANNEL_KEY]: { listen() {}, close() {} } }
    const forged = Object.create(proto)
    expect(isWrapper(forged)).toBe(false)

    // ordinary / partial / primitive negatives
    expect(isWrapper({ a: 1 })).toBe(false)
    expect(isWrapper({ [__TQ__DATA_KEY]: 1 })).toBe(false) // only one key
    expect(isWrapper(null)).toBe(false)
    expect(isWrapper(42)).toBe(false)
    expect(isWrapper(undefined)).toBe(false)
    expect(isWrapper('str')).toBe(false)

    // accidental business data that OWNS both reserved keys but whose `__tq__channel` is not a
    // channel (no callable listen/close) must NOT be misread as live.
    expect(isWrapper({ [__TQ__DATA_KEY]: 'business', [__TQ__CHANNEL_KEY]: { business: true } })).toBe(false)
    expect(isWrapper({ [__TQ__DATA_KEY]: 'business', [__TQ__CHANNEL_KEY]: 'not-a-channel' })).toBe(false)
    // a genuine wrapper needs a channel with BOTH listen and close callable
    expect(isWrapper({ [__TQ__DATA_KEY]: 1, [__TQ__CHANNEL_KEY]: { listen() {} } })).toBe(false) // no close
    expect(isWrapper({ [__TQ__DATA_KEY]: 1, [__TQ__CHANNEL_KEY]: { listen() {}, close() {} } })).toBe(true)
  })

  it('T2.D2 shared exposes only the wrapper keys + isWrapper (old global/broadcast surface deleted)', () => {
    // Exhaustive: the runtime surface is exactly these three names, so every previously-exported
    // symbol (the global-prefix / broadcast-key machinery) is provably gone.
    expect(Object.keys(shared).sort()).toEqual(['__TQ__CHANNEL_KEY', '__TQ__DATA_KEY', 'isWrapper'])
  })
})
