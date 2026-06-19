import { serve, telefunc } from './telefunc.js'
import { expect, describe, it } from 'vitest'

describe('serve()', () => {
  it('`serve()` is exported', () => {
    expect(typeof serve).toBe('function')
  })

  it('`telefunc()` is a (deprecated) alias of `serve()`', () => {
    expect(typeof telefunc).toBe('function')
    expect(telefunc).toBe(serve)
  })

  it('`serve()` validates its argument', async () => {
    // @ts-expect-error all arguments should be passed as a single argument object
    await expect(serve()).rejects.toThrow('`serve()`: all arguments should be passed as a single argument object.')
    await expect(serve(undefined as any)).rejects.toThrow('`serve(httpRequest)`: argument `httpRequest` is missing.')
    await expect(serve('foo' as any)).rejects.toThrow(
      '`serve(httpRequest)`: argument `httpRequest` should be an object.',
    )
    await expect(serve({} as any)).rejects.toThrow('`serve({ url })`: argument `url` is missing.')
  })
})
