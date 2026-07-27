import { afterEach, describe, expect, test, vi } from 'vitest'
import {
  disposeRoomBackend,
  getRoomBackend,
  installRoomBackend,
  setDefaultRoomBackend,
  type RoomBackendSpi,
} from './install.js'
import { MemoryRoomBackend } from './memory/backend.js'

afterEach(async () => {
  await disposeRoomBackend()
})

describe('Room backend installation', () => {
  test('lazily installs one implicit memory backend', () => {
    const backend = getRoomBackend()
    expect(backend).toBeInstanceOf(MemoryRoomBackend)
    expect(getRoomBackend()).toBe(backend)
  })

  test('replaces implicit memory with a default and disposes the fallback once', () => {
    const implicit = getRoomBackend()
    const dispose = vi.spyOn(implicit, 'dispose')
    const backend = memoryBackend()

    expect(setDefaultRoomBackend(() => backend)).toBe(backend)
    expect(getRoomBackend()).toBe(backend)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  test('lets an explicit install replace and dispose a default', () => {
    const backend = memoryBackend()
    const dispose = vi.spyOn(backend, 'dispose')
    const explicit = memoryBackend()
    setDefaultRoomBackend(() => backend)

    expect(installRoomBackend(() => explicit)).toBe(explicit)
    expect(getRoomBackend()).toBe(explicit)
    expect(dispose).toHaveBeenCalledTimes(1)
  })

  test('never lets a later default overwrite an explicit install', () => {
    const explicit = memoryBackend()
    const factory = vi.fn(() => memoryBackend())
    installRoomBackend(() => explicit)

    expect(setDefaultRoomBackend(factory)).toBe(explicit)
    expect(getRoomBackend()).toBe(explicit)
    expect(factory).not.toHaveBeenCalled()
  })

  test('keeps an equivalent default connection-idempotent', () => {
    const identity = {}
    const backend = memoryBackend()
    const dispose = vi.spyOn(backend, 'dispose')
    const replacement = vi.fn(() => memoryBackend())

    expect(setDefaultRoomBackend(() => backend, identity)).toBe(backend)
    expect(setDefaultRoomBackend(replacement, identity)).toBe(backend)
    expect(replacement).not.toHaveBeenCalled()
    expect(dispose).not.toHaveBeenCalled()
  })

  test('uses last-default-wins for different identities and disposes each retired default once', async () => {
    const first = memoryBackend()
    const firstDispose = vi.spyOn(first, 'dispose')
    const second = memoryBackend()
    const secondDispose = vi.spyOn(second, 'dispose')

    expect(setDefaultRoomBackend(() => first, {})).toBe(first)
    expect(setDefaultRoomBackend(() => second, {})).toBe(second)
    expect(getRoomBackend()).toBe(second)
    expect(firstDispose).toHaveBeenCalledTimes(1)
    expect(secondDispose).not.toHaveBeenCalled()

    await Promise.all([disposeRoomBackend(), disposeRoomBackend()])
    expect(firstDispose).toHaveBeenCalledTimes(1)
    expect(secondDispose).toHaveBeenCalledTimes(1)
  })

  test('does not self-dispose when a default factory returns the implicit memory instance', async () => {
    const backend = getRoomBackend()
    const dispose = vi.spyOn(backend, 'dispose')

    expect(setDefaultRoomBackend(() => backend)).toBe(backend)
    expect(dispose).not.toHaveBeenCalled()
    await expect(backend.readHead('same-implicit')).resolves.toBe(null)
  })

  test('does not self-dispose when an explicit factory returns the installed default instance', async () => {
    const backend = memoryBackend()
    const dispose = vi.spyOn(backend, 'dispose')
    setDefaultRoomBackend(() => backend)

    expect(installRoomBackend(() => backend)).toBe(backend)
    expect(dispose).not.toHaveBeenCalled()
    await expect(backend.readHead('same-default')).resolves.toBe(null)
  })

  test('keeps an explicit instance live when installation is repeated', async () => {
    const backend = memoryBackend()
    const dispose = vi.spyOn(backend, 'dispose')
    const repeatedFactory = vi.fn(() => backend)
    installRoomBackend(() => backend)

    expect(installRoomBackend(repeatedFactory)).toBe(backend)
    expect(repeatedFactory).not.toHaveBeenCalled()
    expect(dispose).not.toHaveBeenCalled()
    await expect(backend.readHead('same-explicit')).resolves.toBe(null)
  })

  test('makes installing explicit before a factory can reenter', () => {
    const nestedFactory = vi.fn(() => memoryBackend())
    const backend = memoryBackend()

    expect(
      installRoomBackend(() => {
        expect(() => getRoomBackend()).toThrow('still installing')
        expect(() => installRoomBackend(nestedFactory)).toThrow('still installing')
        return backend
      }),
    ).toBe(backend)
    expect(nestedFactory).not.toHaveBeenCalled()
    expect(getRoomBackend()).toBe(backend)
  })

  test('clears a throwing factory before a retry', () => {
    const failure = new Error('factory failed')
    expect(() =>
      installRoomBackend(() => {
        throw failure
      }),
    ).toThrow(failure)
    expect(installRoomBackend(() => memoryBackend())).toBeInstanceOf(MemoryRoomBackend)
  })

  test('rejects a mismatched SPI version before committing the installation', () => {
    const backend = memoryBackend()
    ;(backend as unknown as { spiVersion: number }).spiVersion = 2

    expect(() => installRoomBackend(() => backend)).toThrow('spiVersion 2; expected 1')
    expect(getRoomBackend()).toBeInstanceOf(MemoryRoomBackend)
  })

  test('rejects a backend with a missing core method', () => {
    const backend = memoryBackend()
    ;(backend as unknown as Record<string, unknown>).commitLane = undefined

    expect(() => installRoomBackend(() => backend)).toThrow('missing required method "commitLane"')
  })

  test('rejects unusable receiver and directory capability declarations', () => {
    const noReceivers = memoryBackend()
    ;(noReceivers.capabilities as { receivers: string }).receivers = 'none'
    expect(() => installRoomBackend(() => noReceivers)).toThrow('capabilities.receivers')

    const noDirectoryMethod = memoryBackend()
    ;(noDirectoryMethod as unknown as Record<string, unknown>).directoryList = undefined
    expect(() => installRoomBackend(() => noDirectoryMethod)).toThrow('missing required method "directoryList"')
  })

  test('latches the first validated factory and disposes it once before a fresh installation', async () => {
    const first = memoryBackend()
    const dispose = vi.spyOn(first, 'dispose')
    const secondFactory = vi.fn(() => memoryBackend())

    expect(installRoomBackend(() => first)).toBe(first)
    expect(installRoomBackend(secondFactory)).toBe(first)
    expect(secondFactory).not.toHaveBeenCalled()
    expect(getRoomBackend()).toBe(first)

    await Promise.all([disposeRoomBackend(), disposeRoomBackend()])
    expect(dispose).toHaveBeenCalledTimes(1)
    const implicit = getRoomBackend()
    expect(implicit).toBeInstanceOf(MemoryRoomBackend)

    const second = installRoomBackend(() => memoryBackend())
    expect(second).not.toBe(first)
    expect(second).not.toBe(implicit)
  })

  test('blocks acquisition and installation for the full disposal barrier', async () => {
    const deferred = createDeferred<void>()
    const backend = memoryBackendWithDispose(() => deferred.promise)
    installRoomBackend(() => backend)

    const disposing = disposeRoomBackend()
    try {
      expect(getRoomBackend).toThrow('still disposing')
      expect(() => installRoomBackend(() => memoryBackend())).toThrow('still disposing')
      expect(disposeRoomBackend()).toBe(disposing)
    } finally {
      deferred.resolve()
      await disposing
    }
    expect(getRoomBackend()).toBeInstanceOf(MemoryRoomBackend)
    expect(installRoomBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('normalizes synchronous disposal throws and permits a different backend afterward', async () => {
    const failure = new Error('dispose synchronously failed')
    const backend = memoryBackendWithDispose(() => {
      throw failure
    })
    installRoomBackend(() => backend)

    const first = disposeRoomBackend()
    const second = disposeRoomBackend()
    expect(second).toBe(first)
    await expect(first).rejects.toBe(failure)
    await expect(second).rejects.toBe(failure)
    expect(getRoomBackend()).toBeInstanceOf(MemoryRoomBackend)
    expect(installRoomBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('clears a rejected asynchronous disposal before retrying', async () => {
    const failure = new Error('dispose asynchronously failed')
    const backend = memoryBackendWithDispose(() => Promise.reject(failure))
    installRoomBackend(() => backend)

    const first = disposeRoomBackend()
    const second = disposeRoomBackend()
    expect(second).toBe(first)
    await expect(first).rejects.toBe(failure)
    await expect(second).rejects.toBe(failure)
    expect(getRoomBackend()).toBeInstanceOf(MemoryRoomBackend)
    expect(installRoomBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('rejects an uncaught direct synchronous reentrant disposal through the outer outcome', async () => {
    let calls = 0
    const backend = memoryBackendWithDispose(() => {
      calls += 1
      return disposeRoomBackend()
    })
    installRoomBackend(() => backend)

    await expect(disposeRoomBackend()).rejects.toThrow('backend.dispose() must not call disposeRoomBackend()')
    expect(calls).toBe(1)
  })

  test('lets caught self-reentry defer to the backend real disposal and shares ordinary callers afterward', async () => {
    const deferred = createDeferred<void>()
    let reentrant: Promise<void> | undefined
    const backend = memoryBackendWithDispose(() => {
      reentrant = disposeRoomBackend()
      return reentrant.catch(() => deferred.promise)
    })
    installRoomBackend(() => backend)

    const outer = disposeRoomBackend()
    await expect(reentrant).rejects.toThrow('backend.dispose() must not call disposeRoomBackend()')
    expect(disposeRoomBackend()).toBe(outer)
    deferred.resolve()
    await outer
  })

  test('rejects an unrelated synchronous shutdown callback while real disposal remains pending', async () => {
    const deferred = createDeferred<void>()
    let callbackPromise: Promise<void> | undefined
    const backend = memoryBackendWithDispose(() => {
      callbackPromise = disposeRoomBackend()
      return deferred.promise
    })
    installRoomBackend(() => backend)

    const outer = disposeRoomBackend()
    try {
      expect(callbackPromise).not.toBe(outer)
      await expect(callbackPromise).rejects.toThrow('backend.dispose() must not call disposeRoomBackend()')
      expect(getRoomBackend).toThrow('still disposing')
      expect(disposeRoomBackend()).toBe(outer)
    } finally {
      deferred.resolve()
      await outer
    }
  })

  test('never reinstalls an instance after disposal has begun', async () => {
    const deferred = createDeferred<void>()
    const backend = memoryBackendWithDispose(() => deferred.promise)
    installRoomBackend(() => backend)

    const disposing = disposeRoomBackend()
    deferred.resolve()
    await disposing
    expect(() => installRoomBackend(() => backend)).toThrow('cannot be reinstalled')
    expect(installRoomBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('defines disposal while empty and disposal during installation', async () => {
    await expect(disposeRoomBackend()).resolves.toBeUndefined()

    const backend = memoryBackend()
    let disposing: Promise<void> | undefined
    installRoomBackend(() => {
      disposing = disposeRoomBackend()
      return backend
    })
    await expect(disposing).rejects.toThrow('still installing')
  })
})

function memoryBackend(): RoomBackendSpi {
  const backend = new MemoryRoomBackend()
  return new Proxy(backend, {
    get(target, property) {
      const value = Reflect.get(target, property, target)
      return typeof value === 'function' ? value.bind(target) : value
    },
  })
}

function memoryBackendWithDispose(dispose: () => void | Promise<void>): RoomBackendSpi {
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
