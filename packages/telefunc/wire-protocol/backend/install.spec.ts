import { afterEach, describe, expect, test, vi } from 'vitest'
import { disposeRoomBackend, getRoomBackend, installRoomBackend, type RoomBackendSpi } from './install.js'
import { MemoryRoomBackend } from './memory/backend.js'

afterEach(async () => {
  await disposeRoomBackend()
})

describe('Room backend installation', () => {
  test('fails clearly before an installation exists', () => {
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')
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
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')
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
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')

    const second = installRoomBackend(() => memoryBackend())
    expect(second).not.toBe(first)
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
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')
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
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')
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
    expect(() => getRoomBackend()).toThrow('no Room backend is installed')
    expect(installRoomBackend(() => memoryBackend())).not.toBe(backend)
  })

  test('does not recursively dispose when backend.dispose owns a synchronous shutdown callback', async () => {
    let calls = 0
    let reentrantSettled = false
    const backend = memoryBackendWithDispose(async () => {
      calls += 1
      void disposeRoomBackend().then(() => {
        reentrantSettled = true
      })
      await Promise.resolve()
      expect(reentrantSettled).toBe(true)
    })
    installRoomBackend(() => backend)

    await disposeRoomBackend()
    expect(calls).toBe(1)
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
