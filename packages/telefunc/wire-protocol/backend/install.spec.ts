import { afterEach, describe, expect, test, vi } from 'vitest'
import { disposeBackend, getBackend, installBackend, setDefaultBackend } from './install.js'
import { MemoryBackend } from './memory/backend.js'
import type { BackendSpi } from './spi.js'

afterEach(async () => {
  await disposeBackend()
})

describe('Room backend installation', () => {
  test('lazily installs one implicit memory backend', () => {
    const backend = getBackend()
    expect(backend).toBeInstanceOf(MemoryBackend)
    expect(getBackend()).toBe(backend)
  })

  test('replaces implicit memory with a default and disposes the fallback once', () => {
    const implicit = getBackend()
    const dispose = vi.spyOn(implicit, 'dispose')
    const backend = memoryBackend()

    expect(setDefaultBackend(() => backend)).toBe(backend)
    expect(getBackend()).toBe(backend)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  test('lets an explicit install replace and dispose a default', () => {
    const backend = memoryBackend()
    const dispose = vi.spyOn(backend, 'dispose')
    const explicit = memoryBackend()
    setDefaultBackend(() => backend)

    expect(installBackend(() => explicit)).toBe(explicit)
    expect(getBackend()).toBe(explicit)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  test('never lets a later default overwrite an explicit install', () => {
    const explicit = memoryBackend()
    const factory = vi.fn(() => memoryBackend())
    installBackend(() => explicit)

    expect(setDefaultBackend(factory)).toBe(explicit)
    expect(getBackend()).toBe(explicit)
    expect(factory).not.toHaveBeenCalled()
  })

  test('keeps an equivalent default connection-idempotent', () => {
    const identity = {}
    const backend = memoryBackend()
    const dispose = vi.spyOn(backend, 'dispose')
    const replacement = vi.fn(() => memoryBackend())

    expect(setDefaultBackend(() => backend, identity)).toBe(backend)
    expect(setDefaultBackend(replacement, identity)).toBe(backend)
    expect(replacement).not.toHaveBeenCalled()
    expect(dispose).not.toHaveBeenCalled()
  })

  test('uses last-default-wins for different identities and disposes each retired default once', async () => {
    const first = memoryBackend()
    const firstDispose = vi.spyOn(first, 'dispose')
    const second = memoryBackend()
    const secondDispose = vi.spyOn(second, 'dispose')

    expect(setDefaultBackend(() => first, {})).toBe(first)
    expect(setDefaultBackend(() => second, {})).toBe(second)
    expect(getBackend()).toBe(second)
    expect(firstDispose).toHaveBeenCalledTimes(1)
    expect(secondDispose).not.toHaveBeenCalled()

    await Promise.all([disposeBackend(), disposeBackend()])
    expect(firstDispose).toHaveBeenCalledTimes(1)
    expect(secondDispose).toHaveBeenCalledTimes(1)
  })

  test('does not self-dispose when a default factory returns the implicit memory instance', async () => {
    const backend = getBackend()
    const dispose = vi.spyOn(backend, 'dispose')

    expect(setDefaultBackend(() => backend)).toBe(backend)
    expect(dispose).not.toHaveBeenCalled()
    await expect(backend.readHead('same-implicit')).resolves.toBe(null)
  })

  test('does not self-dispose when an explicit factory returns the installed default instance', async () => {
    const backend = memoryBackend()
    const dispose = vi.spyOn(backend, 'dispose')
    setDefaultBackend(() => backend)

    expect(installBackend(() => backend)).toBe(backend)
    expect(dispose).not.toHaveBeenCalled()
    await expect(backend.readHead('same-default')).resolves.toBe(null)
  })

  test('keeps an explicit instance live when installation is repeated', async () => {
    const backend = memoryBackend()
    const dispose = vi.spyOn(backend, 'dispose')
    const repeatedFactory = vi.fn(() => backend)
    installBackend(() => backend)

    expect(installBackend(repeatedFactory)).toBe(backend)
    expect(repeatedFactory).not.toHaveBeenCalled()
    expect(dispose).not.toHaveBeenCalled()
    await expect(backend.readHead('same-explicit')).resolves.toBe(null)
  })

  test('makes installing explicit before a factory can reenter', () => {
    const nestedFactory = vi.fn(() => memoryBackend())
    const backend = memoryBackend()

    expect(
      installBackend(() => {
        expect(() => getBackend()).toThrow('still installing')
        expect(() => installBackend(nestedFactory)).toThrow('still installing')
        return backend
      }),
    ).toBe(backend)
    expect(nestedFactory).not.toHaveBeenCalled()
    expect(getBackend()).toBe(backend)
  })

  test('clears a throwing factory before a retry', () => {
    const failure = new Error('factory failed')
    expect(() =>
      installBackend(() => {
        throw failure
      }),
    ).toThrow(failure)
    expect(installBackend(() => memoryBackend())).toBeInstanceOf(MemoryBackend)
  })

  test('rejects a mismatched SPI version before committing the installation', () => {
    const backend = memoryBackend()
    ;(backend as unknown as { spiVersion: number }).spiVersion = 2

    expect(() => installBackend(() => backend)).toThrow('spiVersion 2; expected 1')
    expect(getBackend()).toBeInstanceOf(MemoryBackend)
  })

  test('rejects a backend with a missing core method', () => {
    const backend = memoryBackend()
    ;(backend as unknown as Record<string, unknown>).commitLane = undefined

    expect(() => installBackend(() => backend)).toThrow('missing required method "commitLane"')
  })

  test('rejects unusable receiver and directory capability declarations', () => {
    const noReceivers = memoryBackend()
    ;(noReceivers.capabilities as { receivers: string }).receivers = 'none'
    expect(() => installBackend(() => noReceivers)).toThrow('capabilities.receivers')

    const noDirectoryMethod = memoryBackend()
    ;(noDirectoryMethod as unknown as Record<string, unknown>).directoryList = undefined
    expect(() => installBackend(() => noDirectoryMethod)).toThrow('missing required method "directoryList"')
  })

  test('latches the first validated factory and disposes it once before a fresh installation', async () => {
    const first = memoryBackend()
    const dispose = vi.spyOn(first, 'dispose')
    const secondFactory = vi.fn(() => memoryBackend())

    expect(installBackend(() => first)).toBe(first)
    expect(installBackend(secondFactory)).toBe(first)
    expect(secondFactory).not.toHaveBeenCalled()
    expect(getBackend()).toBe(first)

    await Promise.all([disposeBackend(), disposeBackend()])
    expect(dispose).toHaveBeenCalledTimes(1)
    const implicit = getBackend()
    expect(implicit).toBeInstanceOf(MemoryBackend)

    const second = installBackend(() => memoryBackend())
    expect(second).not.toBe(first)
    expect(second).not.toBe(implicit)
  })

  test('blocks acquisition and installation for the full disposal barrier', async () => {
    const deferred = createDeferred<void>()
    const backend = memoryBackendWithDispose(() => deferred.promise)
    installBackend(() => backend)

    const disposing = disposeBackend()
    try {
      expect(getBackend).toThrow('still disposing')
      expect(() => installBackend(() => memoryBackend())).toThrow('still disposing')
      expect(disposeBackend()).toBe(disposing)
    } finally {
      deferred.resolve()
      await disposing
    }
    expect(getBackend()).toBeInstanceOf(MemoryBackend)
    expect(installBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('normalizes synchronous disposal throws and permits a different backend afterward', async () => {
    const failure = new Error('dispose synchronously failed')
    const backend = memoryBackendWithDispose(() => {
      throw failure
    })
    installBackend(() => backend)

    const first = disposeBackend()
    const second = disposeBackend()
    expect(second).toBe(first)
    await expect(first).rejects.toBe(failure)
    await expect(second).rejects.toBe(failure)
    expect(getBackend()).toBeInstanceOf(MemoryBackend)
    expect(installBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('clears a rejected asynchronous disposal before retrying', async () => {
    const failure = new Error('dispose asynchronously failed')
    const backend = memoryBackendWithDispose(() => Promise.reject(failure))
    installBackend(() => backend)

    const first = disposeBackend()
    const second = disposeBackend()
    expect(second).toBe(first)
    await expect(first).rejects.toBe(failure)
    await expect(second).rejects.toBe(failure)
    expect(getBackend()).toBeInstanceOf(MemoryBackend)
    expect(installBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('rejects an uncaught direct synchronous reentrant disposal through the outer outcome', async () => {
    let calls = 0
    const backend = memoryBackendWithDispose(() => {
      calls += 1
      return disposeBackend()
    })
    installBackend(() => backend)

    await expect(disposeBackend()).rejects.toThrow('backend.dispose() must not call disposeBackend()')
    expect(calls).toBe(1)
  })

  test('lets caught self-reentry defer to the backend real disposal and shares ordinary callers afterward', async () => {
    const deferred = createDeferred<void>()
    let reentrant: Promise<void> | undefined
    const backend = memoryBackendWithDispose(() => {
      reentrant = disposeBackend()
      return reentrant.catch(() => deferred.promise)
    })
    installBackend(() => backend)

    const outer = disposeBackend()
    await expect(reentrant).rejects.toThrow('backend.dispose() must not call disposeBackend()')
    expect(disposeBackend()).toBe(outer)
    deferred.resolve()
    await outer
  })

  test('rejects an unrelated synchronous shutdown callback while real disposal remains pending', async () => {
    const deferred = createDeferred<void>()
    let callbackPromise: Promise<void> | undefined
    const backend = memoryBackendWithDispose(() => {
      callbackPromise = disposeBackend()
      return deferred.promise
    })
    installBackend(() => backend)

    const outer = disposeBackend()
    try {
      expect(callbackPromise).not.toBe(outer)
      await expect(callbackPromise).rejects.toThrow('backend.dispose() must not call disposeBackend()')
      expect(getBackend).toThrow('still disposing')
      expect(disposeBackend()).toBe(outer)
    } finally {
      deferred.resolve()
      await outer
    }
  })

  test('never reinstalls an instance after disposal has begun', async () => {
    const deferred = createDeferred<void>()
    const backend = memoryBackendWithDispose(() => deferred.promise)
    installBackend(() => backend)

    const disposing = disposeBackend()
    deferred.resolve()
    await disposing
    expect(() => installBackend(() => backend)).toThrow('cannot be reinstalled')
    expect(installBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('defines disposal while empty and disposal during installation', async () => {
    await expect(disposeBackend()).resolves.toBeUndefined()

    const backend = memoryBackend()
    let disposing: Promise<void> | undefined
    installBackend(() => {
      disposing = disposeBackend()
      return backend
    })
    await expect(disposing).rejects.toThrow('still installing')
  })
})

function memoryBackend(): BackendSpi {
  const backend = new MemoryBackend()
  return new Proxy(backend, {
    get(target, property) {
      const value = Reflect.get(target, property, target)
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
}

function memoryBackendWithDispose(dispose: () => void | Promise<void>): BackendSpi {
  const backend = memoryBackend()
  ;(backend as unknown as Record<string, unknown>).dispose = dispose
  return backend
}

function createDeferred<T>(): { promise: Promise<T>; resolve(value: T): void } {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}
